# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/lightning_impl.py — Jupiter Metis Swap execution layer (V3+++ LOCK-IN)
===============================================================================

Execution engine behind utils_pkg/router.py (and used by scripts/smoke_lightning.py).

LOCK-IN invariants
------------------
- No network at import time.
- Async-only HTTP via a single lazy httpx.AsyncClient.
- Correct Metis endpoints by default:
      Quote: /swap/v1/quote
      Swap : /swap/v1/swap
  Override via env:
      JUP_QUOTE_PATH, JUP_SWAP_PATH
- Multi-base failover (JUP_ALT_BASE_URLS) + per-base cooldown + simple scoring.
- AUTH failures (401/403) detected; can hard-stop via STRICT_AUTH.
- DRYRUN safety:
      DRYRUN=1 and LIGHTNING_DRYRUN_BUILD_TX=0 => never builds tx, never sends
      DRYRUN=1 and LIGHTNING_DRYRUN_BUILD_TX=1 => builds tx, never sends
- Prequote pass-through: if router passes quoteResponse, we POST it unmodified.
- Backwards-compatible return shapes:
      - buy_via_lightning(...) -> Optional[str]
      - sell_via_lightning(...) -> Optional[str]
      - return_meta=True -> (sig|None, meta_dict)
      - return_receipt=True OR LIGHTNING_RETURN_RECEIPTS=1 -> receipt dict
      - LIGHTNING_ALWAYS_RETURN_META=1 -> receipt dict even when not requested
- Mint sanity:
      - validates base58-ish mint strings
      - warns and auto-fixes a known USDC typo that causes TOKEN_NOT_TRADABLE
- “Recognizable quote functions” for smoke tests:
      get_quote(...), get_buy_quote(...), get_sell_quote(...), quote = get_quote

CRITICAL FIX (your current failure)
-----------------------------------
Your logs show:
  "Using http2=True, but the 'h2' package is not installed..."
That caused Lightning quotes to error and fall back to direct HTTP.

This rewrite makes HTTP/2 *optional*:
- If JUP_HTTP2=1 but h2 is not installed -> auto-disable HTTP/2 and continue.
- Default is safe and stable (HTTP/1.1 works fine with Jupiter/Metis).

Public API expected by router
-----------------------------
- _jup_headers()
- _variant_flags()
- get_quote() / quote()
- get_buy_quote()
- get_sell_quote()
- buy_via_lightning()
- sell_via_lightning()
- close_http()
- stats()
"""

import asyncio
import contextlib
import importlib.util
import inspect
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx

# ──────────────────────────────────────────────────────────────────────────────
# Logging shim (repo-compatible: log.event + log.every)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger  # type: ignore
    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging
    log = logging.getLogger(__name__)  # type: ignore[assignment]


def _short(x: Any, n: int = 360) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


if not hasattr(log, "event"):
    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)
    setattr(log, "event", _event)  # type: ignore[attr-defined]


if not hasattr(log, "every"):
    def _every(key: str, n: int = 30, msg: Optional[str] = None, **kw: Any) -> None:  # pragma: no cover
        try:
            now = time.monotonic()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(max(1, int(n))):
                store[key] = now
                setattr(_every, "_store", store)  # type: ignore[attr-defined]
                getattr(log, "event")(msg or key, **kw)
        except Exception:
            return None
    setattr(log, "every", _every)  # type: ignore[attr-defined]


def _warn(name: str, **kw: Any) -> None:
    with contextlib.suppress(Exception):
        getattr(log, "event")(name, **kw)


# ──────────────────────────────────────────────────────────────────────────────
# Optional budget hooks (sync OR async supported)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover
    class _BudgetNoop:
        def before_quote(self, units: int = 1) -> bool: return True
        def after_quote(self, units: int = 1) -> None: return None
        def before_swap(self, units: int = 1) -> bool: return True
        def after_swap(self, units: int = 1) -> None: return None
    budget = _BudgetNoop()  # type: ignore


async def _budget_call(fn: Any, *a: Any, **kw: Any) -> Any:
    if not callable(fn):
        return None
    try:
        out = fn(*a, **kw)
        if inspect.isawaitable(out):
            return await out
        return out
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Canonical mints (mainnet)
# ──────────────────────────────────────────────────────────────────────────────
KNOWN_SOL_MINT = "So11111111111111111111111111111111111111112"
KNOWN_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
KNOWN_BAD_USDC_TYPO = "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v"

# ──────────────────────────────────────────────────────────────────────────────
# Imports (no network at import time)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg.mints import normalize_mint, SOL_MINT as CANON_SOL_MINT, USDC_MINT as CANON_USDC_MINT  # type: ignore
except Exception:  # pragma: no cover
    def normalize_mint(x: str) -> str:  # type: ignore
        return (x or "").strip()
    CANON_SOL_MINT = KNOWN_SOL_MINT
    CANON_USDC_MINT = KNOWN_USDC_MINT

if str(CANON_USDC_MINT) != KNOWN_USDC_MINT:
    _warn(
        "MINT_CONST_MISMATCH",
        what="USDC_MINT",
        repo_value=str(CANON_USDC_MINT),
        expected=KNOWN_USDC_MINT,
        hint="Fix utils_pkg/mints.py USDC_MINT; wrong USDC causes TOKEN_NOT_TRADABLE.",
    )
    CANON_USDC_MINT = KNOWN_USDC_MINT

from utils_pkg.solana_wallet import get_pubkey_str, load_keypair, sign_and_send_b64_tx  # type: ignore

try:  # pragma: no cover
    from utils_pkg.solana_wallet import _send_b64_with_diag  # type: ignore
except Exception:  # pragma: no cover
    _send_b64_with_diag = None  # type: ignore


# =============================================================================
# Errors
# =============================================================================
class JupHTTPError(RuntimeError):
    def __init__(self, status: int, url: str, body: Any):
        self.status = int(status)
        self.url = str(url)
        self.body = body
        super().__init__(f"HTTP {self.status} {self.url}")


# =============================================================================
# Env helpers
# =============================================================================
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(_env_str(name, str(default))))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env_str(name, str(default)))
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_tristate(name: str) -> Optional[bool]:
    v = os.getenv(name)
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return None


def _clamp_i(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x


def _intish(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        with contextlib.suppress(Exception):
            return int(str(x))
        return int(default)


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _to_thread(fn, *a, **kw):
    try:
        return await asyncio.to_thread(fn, *a, **kw)  # py3.9+
    except AttributeError:  # pragma: no cover
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(*a, **kw))


def _mono() -> float:
    return time.monotonic()


# =============================================================================
# Mint validation / normalization
# =============================================================================
_BASE58_ALPH = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _looks_like_mint(s: str) -> bool:
    s = (s or "").strip()
    if not (32 <= len(s) <= 44):
        return False
    return all(ch in _BASE58_ALPH for ch in s)


def _fix_common_mint_typos(m: str) -> str:
    mm = (m or "").strip()
    if mm == KNOWN_BAD_USDC_TYPO:
        _warn("MINT_AUTO_FIX", bad=mm, fixed=KNOWN_USDC_MINT, what="USDC typo")
        return KNOWN_USDC_MINT
    return mm


def _normalize_and_validate_mint(m: str, *, label: str) -> str:
    m2 = normalize_mint(m)
    m2 = _fix_common_mint_typos(m2)
    if not _looks_like_mint(m2):
        raise ValueError(f"invalid_mint:{label}")
    return m2


# =============================================================================
# Config (Metis Swap API + execution knobs)
# =============================================================================
LIGHTNING_VERSION = "v3+++_lockin_lightning_2025-12-23"
LAMPORTS_PER_SOL = 1_000_000_000

SOL_MINT = _env_str("SOL_MINT", str(CANON_SOL_MINT or KNOWN_SOL_MINT))
USDC_MINT = _env_str("USDC_MINT", str(CANON_USDC_MINT or KNOWN_USDC_MINT))

DEFAULT_BASE = "https://api.jup.ag"
PUBLIC_BASE = "https://public.jupiterapi.com"

BASE = (_env_str("JUP_BASE_URL") or _env_str("JUP_UPSTREAM") or DEFAULT_BASE).rstrip("/")

ALT_BASES_RAW = _env_str("JUP_ALT_BASE_URLS", "")
ALT_BASES: List[str] = []
for part in (ALT_BASES_RAW.split(",") if ALT_BASES_RAW else []):
    u = (part or "").strip().rstrip("/")
    if u and u != BASE and u not in ALT_BASES:
        ALT_BASES.append(u)

ALLOW_PUBLIC_FALLBACK = _env_bool("JUP_ALLOW_PUBLIC_FALLBACK", False)
BASE_URLS: List[str] = [BASE] + ALT_BASES
if ALLOW_PUBLIC_FALLBACK and PUBLIC_BASE not in BASE_URLS:
    BASE_URLS.append(PUBLIC_BASE)

QUOTE_PATH = (_env_str("JUP_QUOTE_PATH", "/swap/v1/quote") or "/swap/v1/quote").strip()
SWAP_PATH = (_env_str("JUP_SWAP_PATH", "/swap/v1/swap") or "/swap/v1/swap").strip()

ONLY_DIRECT = _env_bool("JUP_ONLY_DIRECT", _env_bool("JUP_ONLY_DIRECT_ROUTES", False))
RESTRICT_INTERMEDIATE = _env_bool("JUP_RESTRICT_INTERMEDIATE_TOKENS", True)
DEFAULT_SLIPPAGE_BPS = _clamp_i(_env_int("PF_SLIPPAGE_BPS", _env_int("SLIPPAGE_BPS", 30)), 1, 5000)
MAX_ACCOUNTS = max(0, _env_int("JUP_MAX_ACCOUNTS", 0))  # 0 => omit
PLATFORM_FEE_BPS = _clamp_i(_env_int("JUP_PLATFORM_FEE_BPS", 0), 0, 10_000)
INSTRUCTION_VERSION = _env_str("JUP_INSTRUCTION_VERSION", "").strip()

AS_LEGACY_TX = _env_bool("JUP_AS_LEGACY_TX", _env_bool("JUP_AS_LEGACY", False))
WRAP_UNWRAP_SOL = _env_bool("PF_WRAP_UNWRAP_SOL", True)

COMPUTE_UNIT_LIMIT = max(0, _env_int("PF_COMPUTE_UNIT_LIMIT", 0))
COMPUTE_UNIT_PRICE_MICRO = max(0, _env_int("PF_COMPUTE_UNIT_PRICE_MICRO", _env_int("JUP_COMPUTE_UNIT_PRICE_MICRO", 0)))

FORCE_SHARED = _env_bool("JUP_FORCE_SHARED_ACCOUNTS", False)
_SHARED_TRISTATE = _env_tristate("JUP_USE_SHARED_ACCOUNTS")
USE_SHARED_ACCOUNTS = bool(_SHARED_TRISTATE) if _SHARED_TRISTATE is not None else False
if FORCE_SHARED:
    USE_SHARED_ACCOUNTS = True

USE_TOKEN_LEDGER = _env_bool("JUP_USE_TOKEN_LEDGER", False)
FEE_ACCOUNT = _env_str("JUP_FEE_ACCOUNT", "")
TRACKING_ACCOUNT = _env_str("JUP_TRACKING_ACCOUNT", "")
PAYER = _env_str("JUP_PAYER", "")

DYNAMIC_SLIPPAGE = _env_bool("JUP_DYNAMIC_SLIPPAGE", False)
SKIP_USER_ACCOUNTS_RPC_CALLS = _env_bool("JUP_SKIP_USER_ACCOUNTS_RPC_CALLS", False)
BLOCKHASH_SLOTS_TO_EXPIRY = max(0, _env_int("JUP_BLOCKHASH_SLOTS_TO_EXPIRY", 0))

def _micro_to_lamports(micro: int) -> int:
    try:
        return int(round(max(0, int(micro)) / 1_000_000))
    except Exception:
        return 0

_fallback_micro = _env_int("JITO_FEE_MICROLAMPORTS", 0)
PF_PRIORITY_LAMPORTS = max(0, _env_int("PF_PRIORITY_LAMPORTS", _micro_to_lamports(_fallback_micro)))
PF_PRIORITY_SOL = max(0.0, _env_float("PF_PRIORITY_SOL", 0.0))

HTTP_TIMEOUT_SEC = max(5.0, _env_float("JUP_HTTP_TIMEOUT_SEC", 20.0))
HTTP_RETRY_MAX = max(0, _env_int("JUP_HTTP_RETRY_MAX", 2))
HTTP_RETRY_BACKOFF_SEC = max(0.1, _env_float("JUP_HTTP_RETRY_BACKOFF_SEC", 0.8))
HTTP_RETRY_JITTER_SEC = max(0.0, _env_float("JUP_HTTP_RETRY_JITTER_SEC", 0.25))

# ===== HTTP/2 FIX: only enable if requested AND h2 is installed =====
_HTTP2_REQUESTED = _env_bool("JUP_HTTP2", False)  # default SAFE (HTTP/1.1)
_H2_AVAILABLE = importlib.util.find_spec("h2") is not None
HTTP2 = bool(_HTTP2_REQUESTED and _H2_AVAILABLE)
if _HTTP2_REQUESTED and not _H2_AVAILABLE:
    log.every(
        "JUP_HTTP2_AUTO_DISABLED",
        n=120,
        msg="JUP_HTTP2_AUTO_DISABLED",
        hint="You set JUP_HTTP2=1 but 'h2' isn't installed; continuing with HTTP/1.1.",
    )

HTTP_MAX_KEEPALIVE = max(5, _env_int("JUP_HTTP_MAX_KEEPALIVE", 20))
HTTP_MAX_CONN = max(10, _env_int("JUP_HTTP_MAX_CONN", 60))
HTTP_CONNECT_TIMEOUT = max(2.0, _env_float("JUP_HTTP_CONNECT_TIMEOUT", 5.0))
HTTP_POOL_TIMEOUT = max(2.0, _env_float("JUP_HTTP_POOL_TIMEOUT", 5.0))

JUP_MAX_QPS = max(1, _env_int("JUP_MAX_QPS", 2))
_JUP_MIN_INTERVAL = 1.0 / float(JUP_MAX_QPS)

JUP_MAX_CONCURRENCY = max(1, _env_int("JUP_MAX_CONCURRENCY", 8))
_CONCURRENCY = asyncio.Semaphore(JUP_MAX_CONCURRENCY)

_RATE_LOCK = asyncio.Lock()
_LAST_TS: float = 0.0

BREAKER_ENABLED = _env_bool("JUP_BREAKER_ENABLED", True)
_FAIL_COUNT = 0
_FAIL_LIMIT = max(1, _env_int("JUP_FAIL_LIMIT", 8))
_BREAKER_COOLDOWN = max(5.0, _env_float("JUP_BREAKER_COOLDOWN_SEC", 30.0))
_BREAKER_UNTIL: float = 0.0

BASE_FAIL_LIMIT = max(1, _env_int("JUP_BASE_FAIL_LIMIT", 4))
BASE_COOLDOWN_SEC = max(5.0, _env_float("JUP_BASE_COOLDOWN_SEC", 20.0))
_BASE_FAILS: Dict[str, int] = {}
_BASE_UNTIL: Dict[str, float] = {}
_BASE_SCORE: Dict[str, int] = {}

UNROUTABLE_TTL_SEC = max(30, _env_int("JUP_UNROUTABLE_TTL_SEC", 1800))
REQUOTE_BUMP_PCT = max(1, _env_int("JUP_REQUOTE_BUMP_PCT", 5))
_UNROUTABLE_UNTIL: Dict[Tuple[str, str], float] = {}
_UNROUTABLE_LOCK = asyncio.Lock()

DRY_RUN = _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)
DRYRUN_BUILD_TX = _env_bool("LIGHTNING_DRYRUN_BUILD_TX", False)

LIGHTNING_RETURN_RECEIPTS = _env_bool("LIGHTNING_RETURN_RECEIPTS", False)
LIGHTNING_ALWAYS_RETURN_META = _env_bool("LIGHTNING_ALWAYS_RETURN_META", False)
LIGHTNING_INCLUDE_FULL_QUOTE = _env_bool("LIGHTNING_INCLUDE_FULL_QUOTE", True)

LIGHTNING_USE_ROUTEGUARD = _env_bool("LIGHTNING_USE_ROUTEGUARD", False)

DISABLE_BUYS = _env_bool("DISABLE_BUYS", False)
DISABLE_SELLS = _env_bool("DISABLE_SELLS", False)
KILL_SWITCH = _env_bool("KILL_SWITCH", False)

SEND_SKIP_PREFLIGHT = _env_bool("SOLANA_SKIP_PREFLIGHT", True)
STRICT_AUTH = _env_bool("ROUTER_STRICT_AUTH", _env_bool("JUP_STRICT_AUTH", False))

QUOTE_TTL_SEC = max(0.0, float(_env_float("LIGHTNING_QUOTE_TTL_SEC", 0.0)))  # default OFF
_QUOTE_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_QUOTE_FLIGHT: Dict[str, asyncio.Future[Any]] = {}
_QUOTE_LOCK = asyncio.Lock()

_LAST_GOOD_BASE: Optional[str] = None

_STATS: Dict[str, int] = {
    "quote_ok": 0,
    "quote_fail": 0,
    "swap_build_ok": 0,
    "swap_build_fail": 0,
    "send_ok": 0,
    "send_fail": 0,
    "no_route": 0,
    "breaker_trip": 0,
    "base_cooldown": 0,
    "auth_fail": 0,
    "invalid_mint": 0,
    "token_not_tradable": 0,
    "prequote_build_fail": 0,
    "prequote_requote": 0,
    "noop_same_mint": 0,
    "all_bases_cooldown": 0,
    "http2_disabled_runtime": 0,
}


def _qkey(inp: str, out: str, amount: int, sbps: int, only_direct: Optional[bool]) -> str:
    od = ONLY_DIRECT if only_direct is None else bool(only_direct)
    return f"{inp}:{out}:{int(amount)}:{int(sbps)}:{'D' if od else 'A'}"


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    if QUOTE_TTL_SEC <= 0:
        return None
    rec = _QUOTE_CACHE.get(key)
    if not rec:
        return None
    exp, payload = rec
    if exp >= _mono():
        return payload
    _QUOTE_CACHE.pop(key, None)
    return None


def _cache_set(key: str, payload: Dict[str, Any]) -> None:
    if QUOTE_TTL_SEC <= 0:
        return
    _QUOTE_CACHE[key] = (_mono() + float(QUOTE_TTL_SEC), payload)
    if len(_QUOTE_CACHE) > 5000:
        items = sorted(_QUOTE_CACHE.items(), key=lambda kv: kv[1][0])
        cut = max(1, int(len(items) * 0.2))
        for k, _ in items[:cut]:
            _QUOTE_CACHE.pop(k, None)


async def _singleflight(key: str, maker) -> Any:
    async with _QUOTE_LOCK:
        fut = _QUOTE_FLIGHT.get(key)
        if fut is None or fut.done():
            fut = asyncio.create_task(maker())
            _QUOTE_FLIGHT[key] = fut
    try:
        return await fut
    finally:
        async with _QUOTE_LOCK:
            cur = _QUOTE_FLIGHT.get(key)
            if cur is fut:
                _QUOTE_FLIGHT.pop(key, None)


# =============================================================================
# Response classifiers
# =============================================================================
def _route_not_found(body: Any) -> bool:
    try:
        if isinstance(body, dict):
            if isinstance(body.get("error"), dict):
                return _route_not_found(body["error"])
            code = str(body.get("errorCode") or body.get("code") or "")
            msg = str(body.get("message") or body.get("error") or "")
            if code in {"COULD_NOT_FIND_ANY_ROUTE", "CIRCULAR_ARBITRAGE_IS_DISABLED", "NO_ROUTES"}:
                return True
            if "COULD_NOT_FIND_ANY_ROUTE" in msg:
                return True
            ml = msg.lower()
            return ("could not find any route" in ml) or ("no routes found" in ml)
        if isinstance(body, str):
            if "COULD_NOT_FIND_ANY_ROUTE" in body:
                return True
            bl = body.lower()
            return ("could not find any route" in bl) or ("no routes found" in bl)
    except Exception:
        return False
    return False


def _is_auth_fail(status: int, body: Any) -> bool:
    s = int(status)
    if s in (401, 403):
        return True
    try:
        if isinstance(body, dict):
            msg = str(body.get("message") or body.get("error") or "")
            return ("unauthorized" in msg.lower()) or ("forbidden" in msg.lower())
        if isinstance(body, str):
            bl = body.lower()
            return ("unauthorized" in bl) or ("forbidden" in bl)
    except Exception:
        pass
    return False


def _is_token_not_tradable(body: Any) -> bool:
    try:
        if isinstance(body, dict):
            if isinstance(body.get("error"), dict):
                return _is_token_not_tradable(body["error"])
            code = str(body.get("errorCode") or "")
            msg = str(body.get("error") or body.get("message") or "")
            if code == "TOKEN_NOT_TRADABLE":
                return True
            return ("not tradable" in msg.lower())
        if isinstance(body, str):
            return ("not tradable" in body.lower())
    except Exception:
        pass
    return False


# =============================================================================
# HTTP client (singleton, lazy)
# =============================================================================
_client: Optional[httpx.AsyncClient] = None
_headers_snapshot: Optional[Tuple[str, str, str, bool]] = None  # (api_key, bearer, ua, http2)


def _mask_key(k: str) -> str:
    k = (k or "").strip()
    if not k:
        return ""
    if len(k) <= 8:
        return "*" * len(k)
    return k[:3] + ("*" * (len(k) - 6)) + k[-3:]


def _jup_headers() -> Dict[str, str]:
    api_key = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", _env_str("JUP_KEY", "")))
    bearer = _env_str("JUP_BEARER", _env_str("JUP_ACCESS_TOKEN", ""))
    ua = _env_str("JUP_USER_AGENT", "pnlsniper")

    h: Dict[str, str] = {
        "accept": "application/json",
        "content-type": "application/json",
        "user-agent": ua,
        "origin": _env_str("JUP_ORIGIN", "https://jup.ag"),
        "referer": _env_str("JUP_REFERER", "https://jup.ag/"),
    }
    if api_key:
        h["x-api-key"] = api_key
    if bearer:
        tok = f"Bearer {bearer}"
        h["authorization"] = tok
        h["x-jup-auth"] = tok
    return h


def _effective_http2() -> bool:
    """
    Dynamic re-check:
    - respects env JUP_HTTP2
    - only enables if h2 is installed
    """
    req = _env_bool("JUP_HTTP2", False)
    if not req:
        return False
    if importlib.util.find_spec("h2") is None:
        return False
    return True


def _get_client() -> httpx.AsyncClient:
    global _client, _headers_snapshot

    api_key = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", _env_str("JUP_KEY", "")))
    bearer = _env_str("JUP_BEARER", _env_str("JUP_ACCESS_TOKEN", ""))
    ua = _env_str("JUP_USER_AGENT", "pnlsniper")
    http2_eff = _effective_http2()
    snap = (api_key, bearer, ua, bool(http2_eff))

    if _client is None or _client.is_closed or (_headers_snapshot is not None and snap != _headers_snapshot):
        old = _client
        limits = httpx.Limits(max_keepalive_connections=int(HTTP_MAX_KEEPALIVE), max_connections=int(HTTP_MAX_CONN))
        timeout = httpx.Timeout(
            connect=float(HTTP_CONNECT_TIMEOUT),
            read=float(HTTP_TIMEOUT_SEC),
            write=float(HTTP_TIMEOUT_SEC),
            pool=float(HTTP_POOL_TIMEOUT),
        )
        _client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            headers=_jup_headers(),
            http2=bool(http2_eff),
            follow_redirects=True,
        )
        _headers_snapshot = snap

        if old is not None and not old.is_closed:
            with contextlib.suppress(Exception):
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(old.aclose())
                except Exception:
                    pass

    return _client


async def close_http() -> None:
    global _client, _headers_snapshot
    if _client is not None and not _client.is_closed:
        with contextlib.suppress(Exception):
            await _client.aclose()
    _client = None
    _headers_snapshot = None


# =============================================================================
# Keypair/pubkey cache (non-blocking)
# =============================================================================
_KP_LOCK = asyncio.Lock()
_KP = None
_PUBKEY_STR: Optional[str] = None


async def _get_keypair_cached():
    global _KP, _PUBKEY_STR
    if _KP is not None and _PUBKEY_STR:
        return _KP, _PUBKEY_STR
    async with _KP_LOCK:
        if _KP is not None and _PUBKEY_STR:
            return _KP, _PUBKEY_STR
        kp = await _to_thread(load_keypair)
        pk = await _to_thread(get_pubkey_str, kp)
        _KP = kp
        _PUBKEY_STR = str(pk)
        return _KP, _PUBKEY_STR


# =============================================================================
# Rate limiting + breaker + base scoring
# =============================================================================
async def _qps_gate() -> None:
    global _LAST_TS
    async with _RATE_LOCK:
        now = time.monotonic()
        wait_for = float(_JUP_MIN_INTERVAL) - float(now - _LAST_TS)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _LAST_TS = time.monotonic()


def _breaker_check() -> None:
    if not BREAKER_ENABLED:
        return
    if time.time() < _BREAKER_UNTIL:
        raise RuntimeError("jup_breaker_active")


def _breaker_success() -> None:
    global _FAIL_COUNT, _BREAKER_UNTIL
    _FAIL_COUNT = 0
    _BREAKER_UNTIL = 0.0


def _breaker_fail(tag: str = "") -> None:
    global _FAIL_COUNT, _BREAKER_UNTIL
    if tag in {"no_route", "token_not_tradable", "invalid_mint"}:
        return
    _FAIL_COUNT += 1
    if _FAIL_COUNT >= _FAIL_LIMIT:
        _BREAKER_UNTIL = time.time() + float(_BREAKER_COOLDOWN)
        _STATS["breaker_trip"] += 1
        _warn("JUP_BREAKER_TRIPPED", until=_BREAKER_UNTIL, fails=_FAIL_COUNT)


def _is_transient_status(code: int) -> bool:
    return int(code) in (408, 425, 429, 500, 502, 503, 504)


async def _sleep_backoff(attempt: int) -> None:
    base = float(HTTP_RETRY_BACKOFF_SEC) * (2 ** max(0, int(attempt)))
    jitter = float(HTTP_RETRY_JITTER_SEC) * (0.5 + random.random()) if HTTP_RETRY_JITTER_SEC > 0 else 0.0
    await asyncio.sleep(base + jitter)


def _safe_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except Exception:
        with contextlib.suppress(Exception):
            return {"text": (r.text or "")[:2000]}
        return {"text": ""}


def _base_allowed(base: str) -> bool:
    until = float(_BASE_UNTIL.get(base, 0.0) or 0.0)
    return time.time() >= until


def _base_note_ok(base: str) -> None:
    _BASE_FAILS[base] = 0
    _BASE_UNTIL.pop(base, None)
    _BASE_SCORE[base] = int(_BASE_SCORE.get(base, 0)) + 1


def _base_note_fail(base: str) -> None:
    n = int(_BASE_FAILS.get(base, 0)) + 1
    _BASE_FAILS[base] = n
    _BASE_SCORE[base] = int(_BASE_SCORE.get(base, 0)) - 1
    if n >= int(BASE_FAIL_LIMIT):
        _BASE_UNTIL[base] = time.time() + float(BASE_COOLDOWN_SEC)
        _STATS["base_cooldown"] += 1
        _warn("JUP_BASE_COOLDOWN", base=base, fails=n, cooldown_sec=BASE_COOLDOWN_SEC)


def _bases_in_preferred_order() -> List[str]:
    allowed = [b for b in BASE_URLS if _base_allowed(b)]
    if not allowed:
        _STATS["all_bases_cooldown"] += 1
        soonest = min((float(_BASE_UNTIL.get(b, 0.0) or 0.0) for b in BASE_URLS), default=0.0)
        wait_s = max(0.0, soonest - time.time()) if soonest else 0.0
        log.every("JUP_ALL_BASES_COOLDOWN", n=10, msg="JUP_ALL_BASES_COOLDOWN", wait_s=round(wait_s, 3))
        return []
    allowed.sort(key=lambda b: int(_BASE_SCORE.get(b, 0)), reverse=True)
    if _LAST_GOOD_BASE and _LAST_GOOD_BASE in allowed:
        allowed = [_LAST_GOOD_BASE] + [b for b in allowed if b != _LAST_GOOD_BASE]
    return allowed


# =============================================================================
# Budget + HTTP request wrapper
# =============================================================================
async def _budget_before(is_get: bool, units: int) -> bool:
    if is_get:
        ok = await _budget_call(getattr(budget, "before_quote", None), units=int(units))
        return bool(True if ok is None else ok)
    ok2 = await _budget_call(getattr(budget, "before_swap", None), units=int(units))
    if ok2 is None:
        ok2 = await _budget_call(getattr(budget, "before_quote", None), units=int(units))
    return bool(True if ok2 is None else ok2)


async def _budget_after(is_get: bool, units: int) -> None:
    if is_get:
        await _budget_call(getattr(budget, "after_quote", None), units=int(units))
        return
    x = getattr(budget, "after_swap", None)
    if callable(x):
        await _budget_call(x, units=int(units))
    else:
        await _budget_call(getattr(budget, "after_quote", None), units=int(units))


async def _http_json(
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    budget_units: int = 1,
) -> Any:
    """
    Hardened HTTP wrapper:
    - breaker + qps + concurrency
    - retries on transient
    - auto-recovers if HTTP/2 requested but h2 missing (no Lightning failure)
    """
    _breaker_check()
    await _qps_gate()

    is_get = method.upper() == "GET"
    budget_granted = False
    t0 = time.monotonic()

    try:
        if not await _budget_before(is_get=is_get, units=int(budget_units)):
            raise RuntimeError("quote_blocked_by_budget" if is_get else "swap_blocked_by_budget")
        budget_granted = True

        for attempt in range(0, int(HTTP_RETRY_MAX) + 1):
            c = _get_client()
            try:
                async with _CONCURRENCY:
                    if is_get:
                        r = await c.get(url, params=params)
                    else:
                        r = await c.post(url, json=json_body)

                body = _safe_json(r)

                if r.status_code in (200, 201):
                    if isinstance(body, dict) and body.get("error"):
                        _breaker_fail(tag="")
                        raise JupHTTPError(status=r.status_code, url=url, body=body)
                    _breaker_success()
                    return body

                if _is_auth_fail(r.status_code, body):
                    _STATS["auth_fail"] += 1
                    _breaker_fail(tag="auth")
                    raise JupHTTPError(status=r.status_code, url=url, body=body)

                if _route_not_found(body):
                    _breaker_fail(tag="no_route")
                    raise JupHTTPError(status=r.status_code, url=url, body=body)

                if _is_token_not_tradable(body):
                    _breaker_fail(tag="token_not_tradable")
                    raise JupHTTPError(status=r.status_code, url=url, body=body)

                if _is_transient_status(r.status_code) and attempt < int(HTTP_RETRY_MAX):
                    _warn("JUP_HTTP_RETRY", url=url[:140], status=r.status_code, attempt=attempt, body=_short(body, 260))
                    await _sleep_backoff(attempt)
                    continue

                _breaker_fail(tag="")
                raise JupHTTPError(status=r.status_code, url=url, body=body)

            except (httpx.TimeoutException, httpx.NetworkError) as e:
                if attempt < int(HTTP_RETRY_MAX):
                    _warn("JUP_NET_RETRY", url=url[:140], attempt=attempt, err=_short(e, 220))
                    await _sleep_backoff(attempt)
                    continue
                _breaker_fail(tag="")
                raise

            except Exception as e:
                # If httpx throws the classic "h2 not installed" error, auto-disable http2 and retry once.
                msg = str(e)
                if ("'h2' package is not installed" in msg or "http2=True" in msg) and attempt < int(HTTP_RETRY_MAX):
                    _STATS["http2_disabled_runtime"] += 1
                    log.every(
                        "JUP_HTTP2_DISABLED_RUNTIME",
                        n=120,
                        msg="JUP_HTTP2_DISABLED_RUNTIME",
                        hint="HTTP/2 failed due to missing h2; continuing with HTTP/1.1.",
                    )
                    # force rebuild client without http2 by closing snapshot
                    await close_http()
                    await _sleep_backoff(attempt)
                    continue
                raise

    finally:
        dt_ms = int((time.monotonic() - t0) * 1000)
        if dt_ms > 2500:
            log.every("JUP_SLOW_HTTP", n=20, msg="JUP_SLOW_HTTP", method=method, url=url[:120], ms=dt_ms)
        if budget_granted:
            with contextlib.suppress(Exception):
                await _budget_after(is_get=is_get, units=int(budget_units))


# =============================================================================
# Endpoint builders
# =============================================================================
def _join(base: str, path: str) -> str:
    b = (base or "").rstrip("/")
    p = (path or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    return b + p


def _quote_url(base: str) -> str:
    return _join(base, QUOTE_PATH)


def _swap_url(base: str) -> str:
    return _join(base, SWAP_PATH)


def _quote_params(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: Optional[bool] = None,
) -> Dict[str, Any]:
    od = ONLY_DIRECT if only_direct is None else bool(only_direct)
    p: Dict[str, Any] = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount)),
        "slippageBps": int(slippage_bps),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": od,
        "asLegacyTransaction": bool(AS_LEGACY_TX),
        "restrictIntermediateTokens": bool(RESTRICT_INTERMEDIATE),
    }
    if INSTRUCTION_VERSION:
        p["instructionVersion"] = INSTRUCTION_VERSION
    if MAX_ACCOUNTS > 0:
        p["maxAccounts"] = int(MAX_ACCOUNTS)
    if PLATFORM_FEE_BPS > 0:
        p["platformFeeBps"] = int(PLATFORM_FEE_BPS)
    return p


def _choose_priority_lamports() -> Optional[int]:
    if PF_PRIORITY_LAMPORTS > 0:
        return int(PF_PRIORITY_LAMPORTS)
    if PF_PRIORITY_SOL > 0:
        with contextlib.suppress(Exception):
            return int(float(PF_PRIORITY_SOL) * float(LAMPORTS_PER_SOL))
    return None


async def _gc_unroutable_unlocked() -> None:
    now = time.time()
    for k, exp in list(_UNROUTABLE_UNTIL.items()):
        if exp <= now:
            _UNROUTABLE_UNTIL.pop(k, None)


async def _is_unroutable_pair(inp: str, out: str) -> bool:
    async with _UNROUTABLE_LOCK:
        exp = _UNROUTABLE_UNTIL.get((inp, out))
        return bool(exp and exp > time.time())


async def _mark_unroutable(inp: str, out: str, ttl: Optional[int] = None) -> None:
    async with _UNROUTABLE_LOCK:
        _UNROUTABLE_UNTIL[(inp, out)] = time.time() + float(ttl or UNROUTABLE_TTL_SEC)
        if len(_UNROUTABLE_UNTIL) > 5000:
            await _gc_unroutable_unlocked()


def _compact_route_summary(q: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "inAmount": q.get("inAmount"),
        "outAmount": q.get("outAmount"),
        "priceImpactPct": q.get("priceImpactPct"),
    }
    rp = q.get("routePlan")
    if isinstance(rp, list) and rp:
        out["hops"] = len(rp)
        with contextlib.suppress(Exception):
            first = rp[0] if isinstance(rp[0], dict) else None
            if first and isinstance(first.get("swapInfo"), dict):
                si = first["swapInfo"]
                out["first_label"] = si.get("label") or si.get("ammKey") or si.get("programId")
    return out


def _remember_last_good(base: str) -> None:
    global _LAST_GOOD_BASE
    _LAST_GOOD_BASE = str(base)


# =============================================================================
# Quote core (multi-base + bump + unroutable cache)
# =============================================================================
async def _get_quote_once(*, base: str, params: Dict[str, Any]) -> Dict[str, Any]:
    js = await _http_json("GET", _quote_url(base), params=params, budget_units=1)
    if not isinstance(js, dict):
        raise RuntimeError("jup_quote_non_dict")
    return js


async def _get_quote_kwonly(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: Optional[bool] = None,
) -> Tuple[str, Dict[str, Any]]:
    if int(amount) <= 0:
        raise RuntimeError("amount<=0")

    try:
        inp = _normalize_and_validate_mint(input_mint, label="inputMint")
        out = _normalize_and_validate_mint(output_mint, label="outputMint")
    except ValueError as e:
        _STATS["invalid_mint"] += 1
        raise RuntimeError(str(e)) from e

    if await _is_unroutable_pair(inp, out):
        _STATS["no_route"] += 1
        raise RuntimeError("jup_no_route_cached")

    params = _quote_params(
        input_mint=inp,
        output_mint=out,
        amount=int(amount),
        slippage_bps=int(slippage_bps),
        only_direct=only_direct,
    )

    last_err: Optional[BaseException] = None
    saw_no_route = False
    saw_token_not_tradable = False
    bumped_done = False

    bases = _bases_in_preferred_order()
    if not bases:
        raise RuntimeError("jup_all_bases_cooldown")

    for base in bases:
        try:
            js = await _get_quote_once(base=base, params=params)
            _STATS["quote_ok"] += 1
            _base_note_ok(base)
            _remember_last_good(base)
            log.every("JUP_QUOTE_OK", n=40, msg="JUP_QUOTE_OK", base=base, inp=inp, out=out)
            return base, js

        except JupHTTPError as e:
            last_err = e
            body = e.body

            if _is_auth_fail(e.status, body):
                _base_note_fail(base)
                _warn(
                    "JUP_AUTH_FAIL",
                    base=base,
                    status=e.status,
                    has_api_key=bool(_env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", _env_str("JUP_KEY", "")))),
                    api_key_mask=_mask_key(_env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", _env_str("JUP_KEY", "")))),
                    hint="Set JUP_API_KEY (or JUP_PRO_KEY) to a valid Metis key.",
                    body=_short(body, 260),
                )
                if STRICT_AUTH:
                    raise RuntimeError("jup_unauthorized") from e
                continue

            if _is_token_not_tradable(body):
                saw_token_not_tradable = True
                _STATS["token_not_tradable"] += 1
                _base_note_fail(base)
                _warn("JUP_TOKEN_NOT_TRADABLE", base=base, status=e.status, body=_short(body, 260))
                break

            if _route_not_found(body):
                saw_no_route = True

                if not bumped_done:
                    bumped_done = True
                    bumped = max(int(int(amount) * (100 + int(REQUOTE_BUMP_PCT)) / 100), int(amount) + 1)
                    log.every("JUP_REQUOTE_BUMP", n=25, msg="JUP_REQUOTE_BUMP", base=base, amt=int(amount), bumped=bumped)
                    try:
                        params2 = _quote_params(
                            input_mint=inp,
                            output_mint=out,
                            amount=bumped,
                            slippage_bps=int(slippage_bps),
                            only_direct=only_direct,
                        )
                        js2 = await _get_quote_once(base=base, params=params2)
                        _STATS["quote_ok"] += 1
                        _base_note_ok(base)
                        _remember_last_good(base)
                        log.every("JUP_QUOTE_OK_BUMPED", n=25, msg="JUP_QUOTE_OK_BUMPED", base=base)
                        return base, js2
                    except Exception:
                        pass

                _STATS["quote_fail"] += 1
                _base_note_fail(base)
                continue

            _STATS["quote_fail"] += 1
            _base_note_fail(base)
            _warn("JUP_QUOTE_FAIL", base=base, status=e.status, body=_short(body, 260))
            continue

        except RuntimeError as e:
            last_err = e
            _STATS["quote_fail"] += 1
            _base_note_fail(base)
            _warn("JUP_QUOTE_RT_FAIL", base=base, err=_short(e, 220))
            continue

        except BaseException as e:
            last_err = e
            _STATS["quote_fail"] += 1
            _base_note_fail(base)
            _warn("JUP_QUOTE_UNKNOWN_FAIL", base=base, err=_short(e, 220))
            continue

    if saw_token_not_tradable:
        raise RuntimeError("jup_token_not_tradable")

    if saw_no_route:
        await _mark_unroutable(inp, out)
        _STATS["no_route"] += 1
        raise RuntimeError("jup_no_route")

    if last_err:
        raise last_err
    raise RuntimeError("jup_quote_failed")


async def _get_quote(*args: Any, **kwargs: Any) -> Tuple[str, Dict[str, Any]]:
    if args and not kwargs and len(args) == 4:
        return await _get_quote_kwonly(
            input_mint=str(args[0]),
            output_mint=str(args[1]),
            amount=int(args[2]),
            slippage_bps=int(args[3]),
        )
    return await _get_quote_kwonly(**kwargs)


# =============================================================================
# Swap build + send
# =============================================================================
async def _build_swap_tx(*, base_url: str, user_pubkey: str, quote: Dict[str, Any]) -> str:
    dynamic_cu = not (COMPUTE_UNIT_LIMIT and COMPUTE_UNIT_LIMIT > 0)

    body: Dict[str, Any] = {
        "quoteResponse": quote,  # DO NOT MODIFY
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": bool(WRAP_UNWRAP_SOL),
        "dynamicComputeUnitLimit": bool(dynamic_cu),
        "asLegacyTransaction": bool(AS_LEGACY_TX),
        "useSharedAccounts": bool(USE_SHARED_ACCOUNTS),
        "skipUserAccountsRpcCalls": bool(SKIP_USER_ACCOUNTS_RPC_CALLS),
        "dynamicSlippage": bool(DYNAMIC_SLIPPAGE),
    }

    if USE_TOKEN_LEDGER:
        body["useTokenLedger"] = True
    if PAYER:
        body["payer"] = PAYER
    if PLATFORM_FEE_BPS > 0 and FEE_ACCOUNT:
        body["feeAccount"] = FEE_ACCOUNT
    if TRACKING_ACCOUNT:
        body["trackingAccount"] = TRACKING_ACCOUNT

    prio = _choose_priority_lamports()
    if prio is not None and prio > 0:
        body["prioritizationFeeLamports"] = int(prio)
    if COMPUTE_UNIT_PRICE_MICRO > 0:
        body["computeUnitPriceMicroLamports"] = int(COMPUTE_UNIT_PRICE_MICRO)
    if not dynamic_cu:
        body["computeUnitLimit"] = int(COMPUTE_UNIT_LIMIT)
    if BLOCKHASH_SLOTS_TO_EXPIRY > 0:
        body["blockhashSlotsToExpiry"] = int(BLOCKHASH_SLOTS_TO_EXPIRY)

    js_any = await _http_json("POST", _swap_url(base_url), json_body=body, budget_units=2)
    if not isinstance(js_any, dict):
        _STATS["swap_build_fail"] += 1
        raise RuntimeError("jup_swap_non_dict")
    tx_b64 = js_any.get("swapTransaction")
    if not tx_b64:
        _STATS["swap_build_fail"] += 1
        raise RuntimeError(f"jup_swap_missing_tx: {_short(js_any, 520)}")

    _STATS["swap_build_ok"] += 1
    _base_note_ok(base_url)
    _remember_last_good(base_url)
    log.every("JUP_SWAP_BUILT", n=30, msg="JUP_SWAP_BUILT", base=base_url, shared=USE_SHARED_ACCOUNTS, legacy=int(AS_LEGACY_TX))
    return str(tx_b64)


async def _send_tx_b64(tx_b64: str) -> str:
    try:
        if _send_b64_with_diag is not None:
            with contextlib.suppress(Exception):
                sig = await _maybe_await(_send_b64_with_diag(tx_b64, skip_preflight=SEND_SKIP_PREFLIGHT))  # type: ignore[misc]
                if sig:
                    _STATS["send_ok"] += 1
                    return str(sig)
    except Exception:
        pass

    kp, _ = await _get_keypair_cached()
    try:
        sig2 = await _maybe_await(sign_and_send_b64_tx(tx_b64, kp, skip_preflight=SEND_SKIP_PREFLIGHT))
    except TypeError:
        sig2 = await _maybe_await(sign_and_send_b64_tx(tx_b64, kp))  # type: ignore[misc]
    _STATS["send_ok"] += 1
    return str(sig2)


async def _try_build_from_prequote_anywhere(*, user_pk: str, quote: Dict[str, Any]) -> Tuple[str, str]:
    bases = _bases_in_preferred_order()
    if not bases:
        raise RuntimeError("jup_all_bases_cooldown")
    last_err: Optional[BaseException] = None
    for base in bases:
        try:
            tx_b64 = await _build_swap_tx(base_url=base, user_pubkey=user_pk, quote=quote)
            return base, tx_b64
        except BaseException as e:
            last_err = e
            _base_note_fail(base)
            continue
    raise last_err or RuntimeError("swap_build_failed_all")


# =============================================================================
# Receipts / meta
# =============================================================================
def _expected_out_sol(quote: Optional[Dict[str, Any]]) -> Tuple[Optional[int], Optional[float]]:
    if not isinstance(quote, dict):
        return None, None
    out_raw = _intish(quote.get("outAmount"), 0)
    if out_raw <= 0:
        return None, None
    return int(out_raw), float(out_raw) / float(LAMPORTS_PER_SOL)


def _mk_receipt(
    *,
    ok: bool,
    side: str,
    base_url: str,
    input_mint: str,
    output_mint: str,
    slippage_bps: int,
    quote: Optional[Dict[str, Any]] = None,
    sig: Optional[str] = None,
    err: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    q = quote or {}
    in_raw = _intish(q.get("inAmount"), 0) if isinstance(q, dict) else 0
    out_raw = _intish(q.get("outAmount"), 0) if isinstance(q, dict) else 0

    out_lamports, out_sol = (None, None)
    if str(side).upper() == "SELL" and normalize_mint(output_mint) == normalize_mint(SOL_MINT):
        out_lamports, out_sol = _expected_out_sol(q)

    receipt: Dict[str, Any] = {
        "ok": bool(ok),
        "side": str(side).upper(),
        "sig": sig,
        "signature": sig,
        "tx_sig": sig,
        "base_url": str(base_url),
        "inputMint": str(input_mint),
        "outputMint": str(output_mint),
        "in_amount_raw": int(in_raw),
        "out_amount_raw": int(out_raw),
        "out_lamports": out_lamports,
        "out_sol": out_sol,
        "priceImpactPct": q.get("priceImpactPct") if isinstance(q, dict) else None,
        "slippage_bps": int(slippage_bps),
        "routeSummary": _compact_route_summary(q) if isinstance(q, dict) else {},
        "err": err,
        "ts": float(time.time()),
        "lightning_version": LIGHTNING_VERSION,
    }
    if meta is not None:
        receipt["meta"] = meta
    if LIGHTNING_INCLUDE_FULL_QUOTE:
        receipt["quote"] = q
    return receipt


def _mk_meta_buy(*, mint: str, base: str, quote: Dict[str, Any], lamports_in: int) -> Dict[str, Any]:
    in_raw = _intish(quote.get("inAmount"), lamports_in)
    out_raw = _intish(quote.get("outAmount"), 0)
    return {
        "mint": mint,
        "in_amount_raw": int(in_raw),
        "amount_raw": int(out_raw),  # legacy key
        "inAmount": int(in_raw),
        "outAmount": int(out_raw),
        "priceImpactPct": quote.get("priceImpactPct"),
        "base_url": base,
    }


def _extract_prequote(*qs: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for q in qs:
        if isinstance(q, dict) and q:
            return q
    return None


async def _maybe_routeguard_preflight(*, mint: str, lamports_in: int) -> Tuple[bool, Optional[Dict[str, Any]]]:
    if not LIGHTNING_USE_ROUTEGUARD:
        return True, None
    try:
        from utils_pkg import routeguard  # local import
        fn = getattr(routeguard, "preflight", None)
        if callable(fn):
            ok, q = await fn(mint=str(mint), lamports_in=int(lamports_in))  # type: ignore[misc]
            return bool(ok), (q if isinstance(q, dict) else None)
    except Exception as e:
        log.every("LIGHTNING_RG_ERR", n=10, msg="LIGHTNING_RG_ERR", err=_short(e, 220))
    return True, None


# =============================================================================
# “Recognizable” quote functions (router + smoke_lightning friendly)
# =============================================================================
async def get_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    only_direct: Optional[bool] = None,
) -> Dict[str, Any]:
    sbps = _clamp_i(int(slippage_bps), 1, 5000)
    base, q = await _get_quote(
        input_mint=str(input_mint),
        output_mint=str(output_mint),
        amount=int(amount),
        slippage_bps=sbps,
        only_direct=only_direct,
    )
    _remember_last_good(base)
    return q


quote = get_quote


async def get_buy_quote(
    *,
    mint: str,
    lamports_in: Optional[int] = None,
    amount_sol: Optional[float] = None,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    only_direct: Optional[bool] = None,
    **_kw: Any,
) -> Dict[str, Any]:
    sbps = _clamp_i(int(slippage_bps), 1, 5000)
    if lamports_in is None:
        lamports_in = max(1, int(float(amount_sol or 0.0) * float(LAMPORTS_PER_SOL)))
    lamports = max(1, int(lamports_in))
    m = _normalize_and_validate_mint(mint, label="mint")

    key = _qkey(SOL_MINT, m, lamports, int(sbps), only_direct)
    cached = _cache_get(key)
    if cached is not None:
        return dict(cached)

    async def _mk():
        _, q = await _get_quote(
            input_mint=SOL_MINT,
            output_mint=m,
            amount=lamports,
            slippage_bps=sbps,
            only_direct=only_direct,
        )
        if isinstance(q, dict):
            _cache_set(key, q)
        return q

    return await _singleflight(key, _mk)


async def get_sell_quote(
    *,
    mint: str,
    amount_raw: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    only_direct: Optional[bool] = None,
    **_kw: Any,
) -> Dict[str, Any]:
    sbps = _clamp_i(int(slippage_bps), 1, 5000)
    m = _normalize_and_validate_mint(mint, label="mint")
    amt = max(1, int(amount_raw))

    key = _qkey(m, SOL_MINT, amt, int(sbps), only_direct)
    cached = _cache_get(key)
    if cached is not None:
        return dict(cached)

    async def _mk():
        _, q = await _get_quote(
            input_mint=m,
            output_mint=SOL_MINT,
            amount=amt,
            slippage_bps=sbps,
            only_direct=only_direct,
        )
        if isinstance(q, dict):
            _cache_set(key, q)
        return q

    return await _singleflight(key, _mk)


# =============================================================================
# Public swap API (router depends on these)
# =============================================================================
async def buy_via_lightning(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    return_meta: bool = False,
    return_receipt: bool = False,
    prequote: Optional[Dict[str, Any]] = None,
    quote: Optional[Dict[str, Any]] = None,
    prefetched_quote: Optional[Dict[str, Any]] = None,
    **_ignored: Any,
) -> Union[Optional[str], Tuple[Optional[str], Dict[str, Any]], Dict[str, Any]]:
    sbps = _clamp_i(int(slippage_bps), 1, 5000)
    lamports_in = max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))
    m = _normalize_and_validate_mint(mint, label="mint")

    if normalize_mint(m) == normalize_mint(SOL_MINT):
        _STATS["noop_same_mint"] += 1
        rec = _mk_receipt(
            ok=True,
            side="BUY",
            base_url=BASE_URLS[0],
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote={"inAmount": str(lamports_in), "outAmount": str(lamports_in), "priceImpactPct": 0.0, "routePlan": []},
            sig=None,
            err="noop_same_mint",
            meta={"mint": m, "in_amount_raw": lamports_in, "amount_raw": lamports_in},
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        if return_meta:
            return None, {"mint": m, "in_amount_raw": lamports_in, "amount_raw": lamports_in}
        return rec if LIGHTNING_ALWAYS_RETURN_META else None

    if KILL_SWITCH or DISABLE_BUYS:
        rec = _mk_receipt(
            ok=False,
            side="BUY",
            base_url=BASE_URLS[0],
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote=None,
            sig=None,
            err="buys_disabled",
            meta=None,
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        if return_meta:
            return None, {}
        return rec if LIGHTNING_ALWAYS_RETURN_META else None

    base_used = BASE_URLS[0]
    q: Dict[str, Any] = {}
    meta: Dict[str, Any] = {}

    ok_rg, rg_quote = await _maybe_routeguard_preflight(mint=m, lamports_in=lamports_in)
    if not ok_rg:
        rec = _mk_receipt(
            ok=False,
            side="BUY",
            base_url=base_used,
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote=None,
            sig=None,
            err="routeguard_blocked",
            meta=None,
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        if return_meta:
            return None, {}
        return rec if LIGHTNING_ALWAYS_RETURN_META else None

    supplied = _extract_prequote(rg_quote if isinstance(rg_quote, dict) else None, prequote, quote, prefetched_quote)

    try:
        if supplied is not None:
            q = supplied
            meta = _mk_meta_buy(mint=m, base=base_used, quote=q, lamports_in=lamports_in)
        else:
            base_used, q = await _get_quote(
                input_mint=SOL_MINT,
                output_mint=m,
                amount=lamports_in,
                slippage_bps=sbps,
            )
            meta = _mk_meta_buy(mint=m, base=base_used, quote=q, lamports_in=lamports_in)

        if DRY_RUN and not DRYRUN_BUILD_TX:
            rec = _mk_receipt(
                ok=True,
                side="BUY",
                base_url=str(meta.get("base_url", base_used)),
                input_mint=SOL_MINT,
                output_mint=m,
                slippage_bps=sbps,
                quote=q,
                sig=None,
                err=None,
                meta=meta,
            )
            if return_receipt or LIGHTNING_RETURN_RECEIPTS:
                return rec
            if return_meta:
                return None, meta
            return rec if LIGHTNING_ALWAYS_RETURN_META else None

        _, user_pk = await _get_keypair_cached()

        if supplied is not None:
            try:
                base_used, tx_b64 = await _try_build_from_prequote_anywhere(user_pk=user_pk, quote=q)
                meta["base_url"] = base_used
            except Exception as e:
                _STATS["prequote_build_fail"] += 1
                _warn("PREQUOTE_BUILD_FAIL", err=_short(e, 240))
                _STATS["prequote_requote"] += 1
                base_used, q = await _get_quote(
                    input_mint=SOL_MINT,
                    output_mint=m,
                    amount=lamports_in,
                    slippage_bps=sbps,
                )
                meta = _mk_meta_buy(mint=m, base=base_used, quote=q, lamports_in=lamports_in)
                tx_b64 = await _build_swap_tx(base_url=base_used, user_pubkey=user_pk, quote=q)
        else:
            tx_b64 = await _build_swap_tx(base_url=base_used, user_pubkey=user_pk, quote=q)

        if DRY_RUN:
            rec = _mk_receipt(
                ok=True,
                side="BUY",
                base_url=base_used,
                input_mint=SOL_MINT,
                output_mint=m,
                slippage_bps=sbps,
                quote=q,
                sig=None,
                err=None,
                meta=meta,
            )
            if return_receipt or LIGHTNING_RETURN_RECEIPTS:
                return rec
            if return_meta:
                return None, meta
            return rec if LIGHTNING_ALWAYS_RETURN_META else None

        sig = await _send_tx_b64(tx_b64)
        log.event("ENTRY_TX_OK", sig=sig, mint=m, lamports_in=lamports_in, out_raw=meta.get("amount_raw"), base=base_used)

        rec = _mk_receipt(
            ok=True,
            side="BUY",
            base_url=base_used,
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote=q,
            sig=sig,
            err=None,
            meta=meta,
        )

        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        if return_meta:
            return sig, meta
        return rec if LIGHTNING_ALWAYS_RETURN_META else sig

    except RuntimeError as e:
        tag = str(e)
        if tag in {"jup_no_route", "jup_no_route_cached", "jup_breaker_active", "jup_unauthorized", "jup_token_not_tradable", "jup_all_bases_cooldown"}:
            log.every("ENTRY_SKIP", n=20, msg="ENTRY_SKIP", mint=m, tag=tag)
            if tag.startswith("jup_no_route"):
                _STATS["no_route"] += 1
        rec = _mk_receipt(
            ok=False,
            side="BUY",
            base_url=base_used,
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote=q or None,
            sig=None,
            err=tag,
            meta=meta or None,
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        if return_meta:
            return None, (meta or {})
        return rec if LIGHTNING_ALWAYS_RETURN_META else None

    except Exception as e:
        _STATS["send_fail"] += 1
        _warn("ENTRY_TX_FAIL", err=_short(e, 240), mint=m, base=base_used)
        rec = _mk_receipt(
            ok=False,
            side="BUY",
            base_url=base_used,
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote=q or None,
            sig=None,
            err=str(e),
            meta=meta or None,
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        if return_meta:
            return None, (meta or {})
        return rec if LIGHTNING_ALWAYS_RETURN_META else None


async def sell_via_lightning(
    *,
    mint: str,
    token_program: Optional[str] = None,
    amount_raw: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    return_receipt: bool = False,
    prequote: Optional[Dict[str, Any]] = None,
    quote: Optional[Dict[str, Any]] = None,
    prefetched_quote: Optional[Dict[str, Any]] = None,
    **_ignored: Any,
) -> Union[Optional[str], Dict[str, Any]]:
    del token_program

    sbps = _clamp_i(int(slippage_bps), 1, 5000)
    amt = int(amount_raw)
    m = _normalize_and_validate_mint(mint, label="mint")

    if normalize_mint(m) == normalize_mint(SOL_MINT):
        _STATS["noop_same_mint"] += 1
        rec = _mk_receipt(
            ok=True,
            side="SELL",
            base_url=BASE_URLS[0],
            input_mint=m,
            output_mint=SOL_MINT,
            slippage_bps=sbps,
            quote={"inAmount": str(amt), "outAmount": str(amt), "priceImpactPct": 0.0, "routePlan": []},
            sig=None,
            err="noop_same_mint",
            meta={"expected_out_lamports": amt, "expected_out_sol": float(amt) / float(LAMPORTS_PER_SOL)},
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        return rec if LIGHTNING_ALWAYS_RETURN_META else None

    if KILL_SWITCH or DISABLE_SELLS or amt <= 0:
        rec = _mk_receipt(
            ok=False,
            side="SELL",
            base_url=BASE_URLS[0],
            input_mint=m,
            output_mint=SOL_MINT,
            slippage_bps=sbps,
            quote=None,
            sig=None,
            err="sells_disabled" if (KILL_SWITCH or DISABLE_SELLS) else "amount_raw<=0",
            meta=None,
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        return rec if LIGHTNING_ALWAYS_RETURN_META else None

    base_used = BASE_URLS[0]
    q: Dict[str, Any] = {}
    supplied = _extract_prequote(prequote, quote, prefetched_quote)

    try:
        if supplied is not None:
            q = supplied
        else:
            base_used, q = await _get_quote(
                input_mint=m,
                output_mint=SOL_MINT,
                amount=amt,
                slippage_bps=sbps,
            )

        exp_lamports, exp_sol = _expected_out_sol(q)

        if DRY_RUN and not DRYRUN_BUILD_TX:
            rec = _mk_receipt(
                ok=True,
                side="SELL",
                base_url=base_used,
                input_mint=m,
                output_mint=SOL_MINT,
                slippage_bps=sbps,
                quote=q,
                sig=None,
                err=None,
                meta={"expected_out_lamports": exp_lamports, "expected_out_sol": exp_sol},
            )
            if return_receipt or LIGHTNING_RETURN_RECEIPTS:
                return rec
            return rec if LIGHTNING_ALWAYS_RETURN_META else None

        _, user_pk = await _get_keypair_cached()

        if supplied is not None:
            try:
                base_used, tx_b64 = await _try_build_from_prequote_anywhere(user_pk=user_pk, quote=q)
            except Exception as e:
                _STATS["prequote_build_fail"] += 1
                _warn("PREQUOTE_BUILD_FAIL_SELL", err=_short(e, 240))
                _STATS["prequote_requote"] += 1
                base_used, q = await _get_quote(
                    input_mint=m,
                    output_mint=SOL_MINT,
                    amount=amt,
                    slippage_bps=sbps,
                )
                tx_b64 = await _build_swap_tx(base_url=base_used, user_pubkey=user_pk, quote=q)
        else:
            tx_b64 = await _build_swap_tx(base_url=base_used, user_pubkey=user_pk, quote=q)

        if DRY_RUN:
            rec = _mk_receipt(
                ok=True,
                side="SELL",
                base_url=base_used,
                input_mint=m,
                output_mint=SOL_MINT,
                slippage_bps=sbps,
                quote=q,
                sig=None,
                err=None,
                meta={"expected_out_lamports": exp_lamports, "expected_out_sol": exp_sol},
            )
            if return_receipt or LIGHTNING_RETURN_RECEIPTS:
                return rec
            return rec if LIGHTNING_ALWAYS_RETURN_META else None

        sig = await _send_tx_b64(tx_b64)
        log.event("EXIT_TX_OK", sig=sig, mint=m, amount_raw=amt, base=base_used, expected_out_sol=exp_sol)

        rec = _mk_receipt(
            ok=True,
            side="SELL",
            base_url=base_used,
            input_mint=m,
            output_mint=SOL_MINT,
            slippage_bps=sbps,
            quote=q,
            sig=sig,
            err=None,
            meta={"expected_out_lamports": exp_lamports, "expected_out_sol": exp_sol},
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        return rec if LIGHTNING_ALWAYS_RETURN_META else sig

    except RuntimeError as e:
        tag = str(e)
        if tag in {"jup_no_route", "jup_no_route_cached", "jup_breaker_active", "jup_unauthorized", "jup_token_not_tradable", "jup_all_bases_cooldown"}:
            log.every("EXIT_SKIP", n=20, msg="EXIT_SKIP", mint=m, tag=tag)
            if tag.startswith("jup_no_route"):
                _STATS["no_route"] += 1
        rec = _mk_receipt(
            ok=False,
            side="SELL",
            base_url=base_used,
            input_mint=m,
            output_mint=SOL_MINT,
            slippage_bps=sbps,
            quote=q or None,
            sig=None,
            err=tag,
            meta={"expected_out_lamports": None, "expected_out_sol": None},
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        return rec if LIGHTNING_ALWAYS_RETURN_META else None

    except Exception as e:
        _STATS["send_fail"] += 1
        _warn("EXIT_TX_FAIL", err=_short(e, 240), mint=m, base=base_used)
        rec = _mk_receipt(
            ok=False,
            side="SELL",
            base_url=base_used,
            input_mint=m,
            output_mint=SOL_MINT,
            slippage_bps=sbps,
            quote=q or None,
            sig=None,
            err=str(e),
            meta={"expected_out_lamports": None, "expected_out_sol": None},
        )
        if return_receipt or LIGHTNING_RETURN_RECEIPTS:
            return rec
        return rec if LIGHTNING_ALWAYS_RETURN_META else None


# =============================================================================
# Debug helpers
# =============================================================================
async def debug_get_quote_sol_to_usdc(amount_sol: float = 0.002, slippage_bps: int = DEFAULT_SLIPPAGE_BPS) -> Dict[str, Any]:
    lamports = max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))
    return await get_quote(SOL_MINT, USDC_MINT, lamports, int(slippage_bps))


async def debug_build_only_sol_to_usdc(amount_sol: float = 0.002, slippage_bps: int = DEFAULT_SLIPPAGE_BPS) -> Tuple[int, int, str]:
    lamports = max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))
    base, q = await _get_quote(
        input_mint=SOL_MINT,
        output_mint=USDC_MINT,
        amount=lamports,
        slippage_bps=_clamp_i(int(slippage_bps), 1, 5000),
    )
    _, user_pk = await _get_keypair_cached()
    tx_b64 = await _build_swap_tx(base_url=base, user_pubkey=user_pk, quote=q)
    return _intish(q.get("inAmount"), lamports), len(tx_b64), base


def stats() -> Dict[str, Any]:
    return {
        "lightning_version": LIGHTNING_VERSION,
        "base_urls": list(BASE_URLS),
        "quote_path": QUOTE_PATH,
        "swap_path": SWAP_PATH,
        "last_good_base": _LAST_GOOD_BASE,
        "breaker_enabled": bool(BREAKER_ENABLED),
        "breaker_fail_limit": int(_FAIL_LIMIT),
        "breaker_until": float(_BREAKER_UNTIL),
        "base_fail_limit": int(BASE_FAIL_LIMIT),
        "base_cooldown_sec": float(BASE_COOLDOWN_SEC),
        "base_fails": dict(_BASE_FAILS),
        "base_until": dict(_BASE_UNTIL),
        "base_score": dict(_BASE_SCORE),
        "unroutable_pairs": len(_UNROUTABLE_UNTIL),
        "quote_ttl_sec": float(QUOTE_TTL_SEC),
        "strict_auth": bool(STRICT_AUTH),
        "http2_requested": bool(_env_bool("JUP_HTTP2", False)),
        "http2_effective": bool(_effective_http2()),
        "stats": dict(_STATS),
    }


def _variant_flags() -> Dict[str, Any]:
    api_key = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", _env_str("JUP_KEY", "")))
    return {
        "lightning_version": LIGHTNING_VERSION,
        "base_urls": BASE_URLS,
        "quote_path": QUOTE_PATH,
        "swap_path": SWAP_PATH,
        "only_direct": ONLY_DIRECT,
        "restrict_intermediate_tokens": RESTRICT_INTERMEDIATE,
        "max_accounts": MAX_ACCOUNTS,
        "platform_fee_bps": PLATFORM_FEE_BPS,
        "instruction_version": INSTRUCTION_VERSION,
        "as_legacy_tx": AS_LEGACY_TX,
        "wrap_unwrap_sol": WRAP_UNWRAP_SOL,
        "compute_unit_limit": COMPUTE_UNIT_LIMIT,
        "compute_unit_price_micro": COMPUTE_UNIT_PRICE_MICRO,
        "use_shared_accounts": USE_SHARED_ACCOUNTS,
        "use_token_ledger": USE_TOKEN_LEDGER,
        "fee_account_set": bool(FEE_ACCOUNT),
        "tracking_account_set": bool(TRACKING_ACCOUNT),
        "payer_set": bool(PAYER),
        "dynamic_slippage": DYNAMIC_SLIPPAGE,
        "skip_user_accounts_rpc_calls": SKIP_USER_ACCOUNTS_RPC_CALLS,
        "blockhash_slots_to_expiry": BLOCKHASH_SLOTS_TO_EXPIRY,
        "priority_lamports": PF_PRIORITY_LAMPORTS,
        "priority_sol": PF_PRIORITY_SOL,
        "timeout_sec": HTTP_TIMEOUT_SEC,
        "retry_max": HTTP_RETRY_MAX,
        "http2_requested": bool(_env_bool("JUP_HTTP2", False)),
        "http2_effective": bool(_effective_http2()),
        "dryrun": DRY_RUN,
        "dryrun_build_tx": DRYRUN_BUILD_TX,
        "return_receipts_default": LIGHTNING_RETURN_RECEIPTS,
        "always_return_meta": LIGHTNING_ALWAYS_RETURN_META,
        "include_full_quote": LIGHTNING_INCLUDE_FULL_QUOTE,
        "qps": JUP_MAX_QPS,
        "max_concurrency": JUP_MAX_CONCURRENCY,
        "unroutable_ttl_sec": UNROUTABLE_TTL_SEC,
        "requote_bump_pct": REQUOTE_BUMP_PCT,
        "base_fail_limit": BASE_FAIL_LIMIT,
        "base_cooldown_sec": BASE_COOLDOWN_SEC,
        "disable_buys": DISABLE_BUYS,
        "disable_sells": DISABLE_SELLS,
        "kill_switch": KILL_SWITCH,
        "skip_preflight": SEND_SKIP_PREFLIGHT,
        "quote_ttl_sec": QUOTE_TTL_SEC,
        "strict_auth": STRICT_AUTH,
        "has_api_key": bool(api_key),
        "api_key_mask": _mask_key(api_key),
        "stats": dict(_STATS),
    }


__all__ = [
    "BASE",
    "BASE_URLS",
    "ONLY_DIRECT",
    "_variant_flags",
    "_jup_headers",
    "get_quote",
    "quote",
    "get_buy_quote",
    "get_sell_quote",
    "buy_via_lightning",
    "sell_via_lightning",
    "debug_get_quote_sol_to_usdc",
    "debug_build_only_sol_to_usdc",
    "stats",
    "close_http",
]
