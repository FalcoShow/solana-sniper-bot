# utils_pkg/router.py
from __future__ import annotations

"""
utils_pkg/router.py — V3+++ God-Tier Router (LOCK-IN FINAL)
==========================================================

Role
----
Thin, SAFE orchestration layer used across scanners / sniper / exit workers.

Hard requirements
-----------------
- Import safely (NO network at import time).
- Async functions NEVER block event loop on sync I/O.
- Stable public aliases used elsewhere:
    - get_buy_quote(...)
    - get_sell_quote(...)
    - get_quote(...)
    - normalize_pump_id_to_mint(...)
    - is_valid_pubkey_str(...)
- Decimals are canonicalized via utils_pkg.solana_token (SOL=9 always).

RouteGuard preflight contract
-----------------------------
routeguard.preflight(mint: str, lamports_in: int) -> (ok: bool, quote_or_none: Optional[dict])

Behavior:
- If routeguard module is MISSING: router proceeds normally.
- If routeguard PRESENT and returns (True, None): interpret as “fallback-only” (DO NOT hit Jupiter).
- If routeguard PRESENT but DISABLED: it should return (True, {}), allowing Jupiter path.

Auth / 401 robustness (CRITICAL)
--------------------------------
If lightning_impl quote raises 401/403, we MUST NOT silently fall back.
We re-raise auth errors (when ROUTER_STRICT_AUTH=1).

Jupiter quote path correctness (CRITICAL)
-----------------------------------------
Default quote path is /swap/v1/quote (modern Swap API), override via JUP_QUOTE_PATH.
"""

import asyncio
import contextlib
import os
import random
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import httpx

# =============================================================================
# Logging (shim-safe)
# =============================================================================
try:
    from logging_setup import get_logger  # type: ignore
    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging
    log = logging.getLogger(__name__)  # type: ignore[assignment]


def _install_log_shims() -> None:
    if not hasattr(log, "event"):
        def _event(name: str, **kw: Any) -> None:  # pragma: no cover
            with contextlib.suppress(Exception):
                log.info("%s %s", name, kw)
        setattr(log, "event", _event)  # type: ignore[attr-defined]

    if not hasattr(log, "every"):
        def _every(key: str, n: int = 10, msg: Optional[str] = None, **kw: Any) -> None:  # pragma: no cover
            try:
                now = time.monotonic()
                store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
                last = float(store.get(key, 0.0))
                if (now - last) >= float(max(1, int(n))):
                    store[key] = now
                    setattr(_every, "_store", store)  # type: ignore[attr-defined]
                    ev = getattr(log, "event", None)
                    if callable(ev):
                        ev(msg or key, **kw)
                    else:
                        log.info("%s %s", msg or key, kw)
            except Exception:
                return None
        setattr(log, "every", _every)  # type: ignore[attr-defined]


_install_log_shims()


def _short(x: Any, n: int = 260) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


def _mono() -> float:
    return time.monotonic()


# =============================================================================
# Exceptions (routeguard reads .status_code when present)
# =============================================================================
class RouterError(RuntimeError):
    pass


class JupHTTPError(RouterError):
    def __init__(
        self,
        status_code: int,
        detail: Any = None,
        *,
        url: str = "",
        retry_after_s: Optional[float] = None,
    ) -> None:
        super().__init__(f"Jupiter HTTP {int(status_code)} {url}".strip())
        self.status_code = int(status_code)
        self.detail = detail
        self.url = url
        self.retry_after_s = float(retry_after_s) if retry_after_s is not None else None

    def __str__(self) -> str:
        base = f"Jupiter HTTP {self.status_code}"
        if self.url:
            base += f" {self.url}"
        if self.retry_after_s is not None:
            base += f" retry_after_s={self.retry_after_s:.2f}"
        if self.detail is not None:
            base += f" detail={_short(self.detail, 220)}"
        return base


def _exc_status_code(e: Any) -> Optional[int]:
    try:
        sc = getattr(e, "status_code", None)
        if sc is not None:
            return int(sc)
    except Exception:
        pass
    try:
        resp = getattr(e, "response", None)
        if resp is not None:
            sc2 = getattr(resp, "status_code", None)
            if sc2 is not None:
                return int(sc2)
    except Exception:
        pass
    s = _short(e, 300).lower()
    if " 401 " in s or "http 401" in s or "status=401" in s:
        return 401
    if " 403 " in s or "http 403" in s or "status=403" in s:
        return 403
    return None


def _is_auth_error(e: Any) -> bool:
    return _exc_status_code(e) in (401, 403)


def _is_retryable_http(sc: Optional[int]) -> bool:
    return sc in (408, 425, 429, 500, 502, 503, 504)


# =============================================================================
# Optional budget hooks
# =============================================================================
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover
    class _BudgetNoop:
        def before_quote(self, units: int = 1) -> bool: return True
        def after_quote(self, units: int = 1) -> None: return None
        def before_rpc(self, method: str, *, weight: Optional[int] = None) -> bool: return True
        def after_rpc(self, method: str, *, weight: Optional[int] = None) -> None: return None
    budget = _BudgetNoop()  # type: ignore


def _budget_before_quote(units: int = 1) -> bool:
    try:
        fn = getattr(budget, "before_quote", None)
        return bool(fn(units=int(units))) if callable(fn) else True
    except Exception:
        return True


def _budget_after_quote(units: int = 1) -> None:
    try:
        fn = getattr(budget, "after_quote", None)
        if callable(fn):
            fn(units=int(units))
    except Exception:
        return None


# =============================================================================
# Wallet utils (compat exports)
# =============================================================================
try:
    from utils_pkg.solana_wallet import (  # type: ignore
        get_balance_lamports,
        get_pubkey_str,
        load_keypair,
        sign_and_send_b64_tx,
    )
except Exception:  # pragma: no cover
    def load_keypair(*args: Any, **kwargs: Any) -> Any: raise RuntimeError("solana_wallet.load_keypair unavailable")
    def get_pubkey_str(*args: Any, **kwargs: Any) -> str: raise RuntimeError("solana_wallet.get_pubkey_str unavailable")
    def sign_and_send_b64_tx(*args: Any, **kwargs: Any) -> str: raise RuntimeError("solana_wallet.sign_and_send_b64_tx unavailable")
    def get_balance_lamports(*args: Any, **kwargs: Any) -> int: raise RuntimeError("solana_wallet.get_balance_lamports unavailable")

try:  # pragma: no cover
    from utils_pkg.solana_wallet import _send_b64_with_diag  # type: ignore
except Exception:  # pragma: no cover
    _send_b64_with_diag = None  # type: ignore


# =============================================================================
# Canonical SOL + decimals source-of-truth
# =============================================================================
from utils_pkg import solana_token as st  # uses cached + deduped mint info

ROUTER_VERSION = "v3+++_lockin_2025-12-28"

SOL_MINT = (os.getenv("SOL_MINT") or st.NATIVE_SOL_MINT).strip() or st.NATIVE_SOL_MINT
LAMPORTS_PER_SOL = 1_000_000_000
LAMPORTS_PER_SOL_F = float(LAMPORTS_PER_SOL)

# Keep consistent with solana_token
async def sol_decimals() -> int:
    return st.NATIVE_SOL_DECIMALS

async def get_sol_decimals() -> int:
    return st.NATIVE_SOL_DECIMALS


# =============================================================================
# Env helpers
# =============================================================================
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()

def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if not v:
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE: return True
    if s in _FALSE: return False
    return bool(d)

def _envi(k: str, d: int) -> int:
    try: return int(float(_envs(k, str(d))))
    except Exception: return int(d)

def _envf(k: str, d: float) -> float:
    try: return float(_envs(k, str(d)))
    except Exception: return float(d)

def _clamp_i(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x

def _clamp_f(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


# =============================================================================
# Router toggles
# =============================================================================
RPC_URL = _envs("SOL_RPC_URL", _envs("RPC_URL", "https://api.mainnet-beta.solana.com"))
RPC_TIMEOUT_SEC = max(3.0, _envf("ROUTER_RPC_TIMEOUT_SEC", 10.0))

TOKENS_BASE = (_envs("JUP_TOKENS_BASE") or _envs("TOKENS_JUP_BASE") or "https://tokens.jup.ag").rstrip("/")
TOKENS_TTL_SEC = max(30.0, _envf("ROUTER_TOKENS_TTL_SEC", 900.0))

DRY_RUN = _envb("DRYRUN", False) or _envb("DRY_RUN", False) or _envb("IS_DRYRUN", False)

ROUTER_ALLOW_JUPITER = _envb("ROUTER_ALLOW_JUPITER", True) and _envb("JUP_ENABLED", True)
ROUTER_STRICT_AUTH = _envb("ROUTER_STRICT_AUTH", True)

ALLOW_PUMP = _envb("ROUTER_ALLOW_PUMP", True)
ALLOW_RAYDIUM = _envb("ROUTER_ALLOW_RAYDIUM", True)

BUY_IMPL = _envs("ROUTER_BUY_IMPL", "lightning").lower().strip()
SELL_IMPL = _envs("ROUTER_SELL_IMPL", "lightning").lower().strip()

MIN_SOL_BALANCE = _envf("MIN_SOL_BALANCE", 0.0)
ROUTER_SOL_BAL_TTL_SEC = max(0.1, _envf("ROUTER_SOL_BAL_TTL_SEC", 0.8))
ROUTER_BLOCK_ON_BAL_UNKNOWN = _envb("ROUTER_BLOCK_ON_BAL_UNKNOWN", False)

QUOTE_TTL_SEC = max(0.0, _envf("ROUTER_QUOTE_TTL_SEC", 6.0))
PRICE_TTL_SEC = max(0.0, _envf("PF_PRICE_TTL_SEC", 6.0))
BAD_ID_TTL_SEC = max(10, _envi("ROUTER_BAD_ID_TTL_SEC", 60))

ROUTER_QUOTE_CACHE_MAX = max(100, _envi("ROUTER_QUOTE_CACHE_MAX", 2000))
ROUTER_MAX_PARALLEL_QUOTES = max(1, _envi("ROUTER_MAX_PARALLEL_QUOTES", 10))
ROUTER_QUOTE_RETRIES = _clamp_i(_envi("ROUTER_QUOTE_RETRIES", 1), 0, 8)
ROUTER_QUOTE_RETRY_BASE_MS = _clamp_i(_envi("ROUTER_QUOTE_RETRY_BASE_MS", 150), 25, 3000)

ROUTER_ROTATE_ENDPOINTS = _envb("ROUTER_ROTATE_ENDPOINTS", True) and (not DRY_RUN)
_ROUND_ROBIN = 0

JUP_BASE_URL = (_envs("JUP_BASE_URL") or "https://api.jup.ag").rstrip("/")
JUP_ALT_BASE_URLS = [u.strip() for u in (_envs("JUP_ALT_BASE_URLS") or "").split(",") if u.strip()]
JUP_PUBLIC_BASE_URL = (_envs("JUP_PUBLIC_BASE_URL") or "https://public.jupiterapi.com").rstrip("/")

JUP_QUOTE_PATH = (_envs("JUP_QUOTE_PATH") or "/swap/v1/quote").strip()
JUP_AS_LEGACY = _envb("JUP_AS_LEGACY", False)
JUP_DEFAULT_ONLY_DIRECT = _envb("JUP_ONLY_DIRECT", False)

JUP_RESTRICT_INTERMEDIATE = _envb("JUP_RESTRICT_INTERMEDIATE_TOKENS", False)
JUP_MAX_ACCOUNTS = _envi("JUP_MAX_ACCOUNTS", 0)
JUP_PLATFORM_FEE_BPS = _envi("JUP_PLATFORM_FEE_BPS", 0)
JUP_INSTRUCTION_VERSION = (_envs("JUP_INSTRUCTION_VERSION", "")).strip()

HTTP_MAX_KEEPALIVE = max(5, _envi("ROUTER_HTTP_MAX_KEEPALIVE", 10))
HTTP_MAX_CONN = max(10, _envi("ROUTER_HTTP_MAX_CONN", 30))
HTTP_CONNECT_TIMEOUT = max(2.0, _envf("ROUTER_HTTP_CONNECT_TIMEOUT", 5.0))
HTTP_POOL_TIMEOUT = max(2.0, _envf("ROUTER_HTTP_POOL_TIMEOUT", 5.0))
HTTP_READ_TIMEOUT = max(3.0, _envf("ROUTER_HTTP_READ_TIMEOUT", float(RPC_TIMEOUT_SEC)))
HTTP_HTTP2 = _envb("ROUTER_HTTP2", False)


# =============================================================================
# Slippage defaults
# =============================================================================
def _slippage_bps_default() -> int:
    v = (_envs("PF_SLIPPAGE_BPS") or _envs("SLIPPAGE_BPS")).strip()
    if v:
        with contextlib.suppress(Exception):
            return _clamp_i(int(v), 1, 5000)
    return 30

def _clamp_slippage_bps(v: Optional[int]) -> int:
    with contextlib.suppress(Exception):
        return _clamp_i(int(v if v is not None else _slippage_bps_default()), 1, 5000)
    return _slippage_bps_default()


# =============================================================================
# Base58 pubkey validation (fast, no deps)
# =============================================================================
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
    """STABLE PUBLIC API (routeguard depends on this)."""
    return _is_valid_pubkey_str(s)

def _sanitize_mint(m: Any) -> str:
    mm = (str(m) if m is not None else "").strip()
    if not mm:
        raise ValueError("empty mint")
    if not _is_valid_pubkey_str(mm):
        raise ValueError(f"Invalid mint (must be 32-byte base58): {mm}")
    return mm


# =============================================================================
# Lazy imports (avoid circulars; keep import safe)
# =============================================================================
_L_mod = None
_RAY_mod = None
_PUMP_mod = None
_EXT_NORM = None

def _get_lightning():
    global _L_mod
    if _L_mod is not None:
        return _L_mod
    try:
        from utils_pkg import lightning_impl as L  # type: ignore
        _L_mod = L
        return _L_mod
    except Exception:
        _L_mod = None
        return None

def _get_raydium():
    global _RAY_mod
    if _RAY_mod is not None:
        return _RAY_mod
    try:
        from utils_pkg import raydium_impl as raydium  # type: ignore
        _RAY_mod = raydium
        return _RAY_mod
    except Exception:
        _RAY_mod = None
        return None

def _get_pump():
    global _PUMP_mod
    if _PUMP_mod is not None:
        return _PUMP_mod
    try:
        from utils_pkg import pump_trader  # type: ignore
        _PUMP_mod = pump_trader
        return _PUMP_mod
    except Exception:
        _PUMP_mod = None
        return None

def _get_external_normalize():
    global _EXT_NORM
    if _EXT_NORM is not None:
        return _EXT_NORM
    try:
        from utils_pkg.mints import normalize_mint as nm  # type: ignore
        _EXT_NORM = nm
        return _EXT_NORM
    except Exception:
        _EXT_NORM = None
        return None

def normalize_pump_id_to_mint(raw: str) -> str:
    """
    STABLE PUBLIC API (routeguard depends on this).
    Prefer utils_pkg.mints.normalize_mint if available; otherwise accept only pubkey strings.
    """
    raw_s = (raw or "").strip()
    nm = _get_external_normalize()
    if callable(nm):
        with contextlib.suppress(Exception):
            out = str(nm(raw_s))  # type: ignore[misc]
            return _sanitize_mint(out)
    if _is_valid_pubkey_str(raw_s):
        return raw_s
    raise ValueError(f"cannot normalize to mint: {raw}")


# =============================================================================
# Async HTTP client (tokens + direct Jupiter quote fallback)
# =============================================================================
_http_client: Optional[httpx.AsyncClient] = None
_http_lock = asyncio.Lock()

async def _get_http() -> httpx.AsyncClient:
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        return _http_client
    async with _http_lock:
        if _http_client is None or _http_client.is_closed:
            limits = httpx.Limits(
                max_keepalive_connections=int(HTTP_MAX_KEEPALIVE),
                max_connections=int(HTTP_MAX_CONN),
            )
            headers = {"user-agent": "pnlsniper/router"}
            timeout = httpx.Timeout(
                connect=float(HTTP_CONNECT_TIMEOUT),
                read=float(HTTP_READ_TIMEOUT),
                write=float(HTTP_READ_TIMEOUT),
                pool=float(HTTP_POOL_TIMEOUT),
            )
            _http_client = httpx.AsyncClient(
                timeout=timeout,
                limits=limits,
                headers=headers,
                follow_redirects=True,
                http2=bool(HTTP_HTTP2),
            )
    return _http_client

async def _close_http_local() -> None:
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        with contextlib.suppress(Exception):
            await _http_client.aclose()
    _http_client = None


# =============================================================================
# Quote cache + singleflight
# =============================================================================
_quote_sema = asyncio.Semaphore(int(ROUTER_MAX_PARALLEL_QUOTES))

_QUOTE_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_QUOTE_FLIGHT: Dict[str, asyncio.Future[Any]] = {}
_quote_lock = asyncio.Lock()

def _qkey(input_mint: str, output_mint: str, amount: int, slippage_bps: int, only_direct: Optional[bool]) -> str:
    od = "N" if only_direct is None else ("T" if only_direct else "F")
    return f"{input_mint}:{output_mint}:{int(amount)}:{int(slippage_bps)}:{od}"

def _trim_quote_cache() -> None:
    if len(_QUOTE_CACHE) <= int(ROUTER_QUOTE_CACHE_MAX):
        return
    items = sorted(_QUOTE_CACHE.items(), key=lambda kv: kv[1][0])
    cut = max(1, int(len(items) * 0.2))
    for k, _ in items[:cut]:
        _QUOTE_CACHE.pop(k, None)

def _get_cached_quote(key: str) -> Optional[Dict[str, Any]]:
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

def _set_cached_quote(key: str, payload: Dict[str, Any]) -> None:
    if QUOTE_TTL_SEC <= 0:
        return
    _QUOTE_CACHE[key] = (_mono() + float(QUOTE_TTL_SEC), payload)
    _trim_quote_cache()

async def _singleflight(key: str, maker: Callable[[], Awaitable[Any]]) -> Any:
    async with _quote_lock:
        fut = _QUOTE_FLIGHT.get(key)
        if fut is None or fut.done():
            fut = asyncio.create_task(maker())
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


# =============================================================================
# Tokens catalog (optional, for symbol/metadata)
# =============================================================================
_TOKENS_CACHE: Optional[Tuple[float, List[Dict[str, Any]]]] = None
_TOK_META: Dict[str, Dict[str, Any]] = {}
_TOK_FLIGHT: Optional[asyncio.Future[Any]] = None

async def _tokens_catalog() -> List[Dict[str, Any]]:
    global _TOKENS_CACHE, _TOK_FLIGHT
    now = _mono()

    if _TOKENS_CACHE and _TOKENS_CACHE[0] >= now:
        return _TOKENS_CACHE[1]

    if _TOK_FLIGHT is not None and not _TOK_FLIGHT.done():
        with contextlib.suppress(Exception):
            return await _TOK_FLIGHT

    async def _fetch() -> List[Dict[str, Any]]:
        c = await _get_http()
        arr: List[Dict[str, Any]] = []
        candidates = [
            f"{TOKENS_BASE}/tokens",
            f"{TOKENS_BASE}/tokens?tags=verified",
            f"{TOKENS_BASE}/tokens?limit=5000",
        ]
        for url in candidates:
            try:
                r = await c.get(url, timeout=20.0)
                if r.status_code != 200:
                    continue
                js = r.json()
                if isinstance(js, list):
                    arr = [t for t in js if isinstance(t, dict)]
                    if arr:
                        break
            except Exception:
                continue

        if arr:
            for t in arr:
                with contextlib.suppress(Exception):
                    address = (t.get("address") or t.get("mint") or "").strip()
                    if address:
                        _TOK_META[address] = t

        _TOKENS_CACHE = (now + float(TOKENS_TTL_SEC), arr)
        return arr

    _TOK_FLIGHT = asyncio.create_task(_fetch())
    return await _TOK_FLIGHT

async def get_tokens() -> List[Dict[str, Any]]:
    return await _tokens_catalog()

def get_token_symbol(mint: str) -> Optional[str]:
    with contextlib.suppress(Exception):
        m = _sanitize_mint(mint)
        meta = _TOK_META.get(m) or {}
        s = meta.get("symbol")
        return str(s).strip() if s else None
    return None


# =============================================================================
# Canonical decimals API (router-level) — ALWAYS available for check_router
# =============================================================================
async def get_decimals(mint: str) -> Optional[int]:
    m = _sanitize_mint(mint)
    if m == SOL_MINT or m == st.NATIVE_SOL_MINT:
        return st.NATIVE_SOL_DECIMALS
    return await st.get_token_decimals(m, default=None)

async def mint_decimals(mint: str) -> Optional[int]:
    return await get_decimals(mint)


# =============================================================================
# Route scoring (analytics)
# =============================================================================
def _route_score(q: Dict[str, Any]) -> float:
    try:
        out_amt = int(q.get("outAmount") or q.get("out_amount_raw") or q.get("out_amount_lamports") or 0)
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


# =============================================================================
# RouteGuard hook (presence-aware)
# =============================================================================
async def _rg_preflight(*, mint: str, lamports_in: int) -> Tuple[bool, Optional[Dict[str, Any]], bool]:
    try:
        from utils_pkg import routeguard  # local import avoids cycles
        fn = getattr(routeguard, "preflight", None)
        if callable(fn):
            ok, q = await fn(mint=str(mint), lamports_in=int(lamports_in))  # type: ignore[misc]
            return bool(ok), (q if isinstance(q, dict) else None), True
    except Exception as e:
        log.every("ROUTER_RG_ERR", 10, msg="ROUTER_RG_ERR", err=_short(e, 220))
    return True, None, False


# =============================================================================
# SOL reserve guard (cached, never blocks event loop)
# =============================================================================
_bal_cache: Tuple[float, int] = (0.0, 0)
_bal_lock = asyncio.Lock()

async def _sol_balance_lamports_cached() -> Optional[int]:
    global _bal_cache
    now = _mono()
    exp, lam = _bal_cache
    if exp >= now and lam >= 0:
        return int(lam)

    async with _bal_lock:
        now2 = _mono()
        exp2, lam2 = _bal_cache
        if exp2 >= now2 and lam2 >= 0:
            return int(lam2)
        try:
            lam3 = await asyncio.to_thread(lambda: int(get_balance_lamports()))
            _bal_cache = (now2 + float(ROUTER_SOL_BAL_TTL_SEC), int(lam3))
            return int(lam3)
        except Exception as e:
            log.every("ROUTER_BAL_FAIL", 10, msg="ROUTER_BAL_FAIL", err=_short(e, 200))
            return None

async def _enforce_min_sol(required_spend_lamports: int = 0) -> bool:
    if MIN_SOL_BALANCE <= 0:
        return True

    bal_lamports = await _sol_balance_lamports_cached()
    if bal_lamports is None:
        if ROUTER_BLOCK_ON_BAL_UNKNOWN:
            log.event("ROUTER_BLOCK_SOL_BAL_UNKNOWN", spend=int(required_spend_lamports))
            return False
        return True

    reserve = int(float(MIN_SOL_BALANCE) * LAMPORTS_PER_SOL_F)
    spend = int(required_spend_lamports or 0)

    if int(bal_lamports) - spend < reserve:
        log.event(
            "ROUTER_BLOCK_SOL_RESERVE",
            bal_sol=float(bal_lamports) / LAMPORTS_PER_SOL_F,
            reserve_sol=float(reserve) / LAMPORTS_PER_SOL_F,
            spend_sol=float(spend) / LAMPORTS_PER_SOL_F,
        )
        return False
    return True


# =============================================================================
# Jupiter quote plumbing (direct fallback; tries multiple bases)
# =============================================================================
def _looks_like_placeholder_key(k: str) -> bool:
    s = (k or "").strip()
    if not s:
        return True
    low = s.lower()
    if low in {"$jup_api_key", "$jup_pro_key", "your-api-key", "your_key", "my key", "changeme"}:
        return True
    if "$" in s and "key" in low:
        return True
    return False

def _jup_api_key() -> str:
    k = _envs("JUP_API_KEY", _envs("JUP_PRO_KEY", ""))
    return "" if _looks_like_placeholder_key(k) else k

def _jup_headers(*, for_public: bool) -> Dict[str, str]:
    h2: Dict[str, str] = {"user-agent": "pnlsniper/router"}
    if not for_public:
        k = _jup_api_key()
        if k:
            h2["x-api-key"] = k
    return h2

def _join_url(base: str, path: str) -> str:
    b = (base or "").rstrip("/")
    p = (path or "").strip()
    if not p:
        return b
    if not p.startswith("/"):
        p = "/" + p
    return b + p

def _quote_url_from_base(base: str) -> str:
    b = (base or "").rstrip("/") or "https://api.jup.ag"
    low = b.lower()
    if low.endswith("/quote"):
        return b
    if low.endswith("/swap/v1"):
        return b + "/quote"
    if low.endswith("/v6"):
        return b + "/quote"
    return _join_url(b, JUP_QUOTE_PATH)

def _jup_quote_urls() -> List[Tuple[str, bool]]:
    if JUP_AS_LEGACY:
        return [(_quote_url_from_base(JUP_PUBLIC_BASE_URL), True)]

    bases = [JUP_BASE_URL] + list(JUP_ALT_BASE_URLS)
    out: List[Tuple[str, bool]] = []
    seen: set[str] = set()

    for b in bases:
        u = _quote_url_from_base(b)
        if u in seen:
            continue
        seen.add(u)
        out.append((u, False))

    # Only add public fallback when NO key is present
    if not _jup_api_key():
        pu = _quote_url_from_base(JUP_PUBLIC_BASE_URL)
        if pu not in seen:
            out.append((pu, True))

    if not out:
        out = [(_quote_url_from_base("https://api.jup.ag"), False)]

    global _ROUND_ROBIN
    if ROUTER_ROTATE_ENDPOINTS and len(out) > 1:
        _ROUND_ROBIN = (_ROUND_ROBIN + 1) % 10_000_000
        non_pub = [x for x in out if not x[1]]
        pub = [x for x in out if x[1]]
        if len(non_pub) > 1:
            k = _ROUND_ROBIN % len(non_pub)
            non_pub = non_pub[k:] + non_pub[:k]
        out = non_pub + pub

    return out

def _apply_optional_quote_params(params: Dict[str, Any]) -> None:
    if JUP_RESTRICT_INTERMEDIATE:
        params["restrictIntermediateTokens"] = True
    if JUP_MAX_ACCOUNTS and int(JUP_MAX_ACCOUNTS) > 0:
        params["maxAccounts"] = int(JUP_MAX_ACCOUNTS)
    if JUP_PLATFORM_FEE_BPS and int(JUP_PLATFORM_FEE_BPS) > 0:
        params["platformFeeBps"] = int(JUP_PLATFORM_FEE_BPS)
    if JUP_INSTRUCTION_VERSION:
        params["instructionVersion"] = str(JUP_INSTRUCTION_VERSION)

def _parse_retry_after(headers: Any) -> Optional[float]:
    try:
        ra = headers.get("retry-after") or headers.get("Retry-After")
        if ra is None:
            return None
        s = str(ra).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None

async def _jup_quote(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: Optional[bool] = None,
) -> Dict[str, Any]:
    c = await _get_http()
    params: Dict[str, Any] = {
        "inputMint": str(input_mint),
        "outputMint": str(output_mint),
        "amount": int(amount),
        "slippageBps": int(slippage_bps),
    }
    if only_direct is True:
        params["onlyDirectRoutes"] = True

    _apply_optional_quote_params(params)

    last_err: Optional[Exception] = None

    for url, is_public in _jup_quote_urls():
        try:
            headers = _jup_headers(for_public=bool(is_public))
            r = await c.get(url, params=params, headers=headers, timeout=10.0)

            if r.status_code != 200:
                detail: Any = None
                with contextlib.suppress(Exception):
                    detail = r.json()
                retry_after = _parse_retry_after(r.headers)
                raise JupHTTPError(r.status_code, detail, url=url, retry_after_s=retry_after)

            js = r.json()
            if not isinstance(js, dict):
                raise RouterError("jup_quote_non_dict")

            # Swap API format returns { data: [route...] , ... }
            if "data" in js and isinstance(js.get("data"), list):
                data = js.get("data") or []
                if data and isinstance(data[0], dict):
                    route0 = dict(data[0])
                    return route0
                return {"error": "NO_ROUTE", "errorCode": "NO_ROUTE", "data": []}

            return js

        except JupHTTPError as e:
            last_err = e
            sc = int(e.status_code)
            if _is_auth_error(e):
                log.every("JUP_QUOTE_AUTH", 8, msg="JUP_QUOTE_AUTH", code=sc, url=e.url, public=int(is_public))
                continue
            if _is_retryable_http(sc):
                log.every("JUP_QUOTE_RETRYABLE", 8, msg="JUP_QUOTE_RETRYABLE", code=sc, url=e.url, public=int(is_public))
            else:
                log.every("JUP_QUOTE_HTTP_ERR", 10, msg="JUP_QUOTE_HTTP_ERR", code=sc, url=e.url, public=int(is_public))
            continue

        except Exception as e:
            last_err = e
            log.every("JUP_QUOTE_ERR", 10, msg="JUP_QUOTE_ERR", err=_short(e, 220), url=url, public=int(is_public))
            continue

    if isinstance(last_err, Exception):
        raise last_err
    raise RouterError("jup_quote_failed_no_error")


# =============================================================================
# Quote retries + lightning quote bridge (AUTH-STRICT)
# =============================================================================
def _retry_sleep_s(e: Exception, i: int) -> float:
    sc = _exc_status_code(e)
    ra = getattr(e, "retry_after_s", None)
    if isinstance(ra, (int, float)) and ra and ra > 0 and _is_retryable_http(sc):
        return float(min(5.0, max(0.15, float(ra))))
    base = float(ROUTER_QUOTE_RETRY_BASE_MS) / 1000.0
    sleep_s = base * (2**i) * (0.6 + random.random() * 0.8)
    return float(min(2.5, sleep_s))

async def _quote_with_retries(fetcher: Callable[[], Awaitable[Dict[str, Any]]]) -> Dict[str, Any]:
    tries = int(ROUTER_QUOTE_RETRIES) + 1
    last: Optional[Exception] = None
    for i in range(tries):
        try:
            return await fetcher()
        except Exception as e:
            last = e
            if i >= tries - 1:
                break
            sc = _exc_status_code(e)
            if _is_auth_error(e):
                raise
            if sc is not None and not _is_retryable_http(sc):
                break
            await asyncio.sleep(_retry_sleep_s(e, i))
    raise last or RouterError("quote_failed")

def _kw_variants(kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = [dict(kwargs)]
    k0 = dict(kwargs)

    if "only_direct" in k0 and "onlyDirectRoutes" not in k0:
        v = dict(k0); v["onlyDirectRoutes"] = v.get("only_direct"); out.append(v)
    if "onlyDirectRoutes" in k0 and "only_direct" not in k0:
        v = dict(k0); v["only_direct"] = v.get("onlyDirectRoutes"); out.append(v)

    if "slippage_bps" in k0 and "slippageBps" not in k0:
        v = dict(k0); v["slippageBps"] = v.get("slippage_bps"); out.append(v)
    if "slippageBps" in k0 and "slippage_bps" not in k0:
        v = dict(k0); v["slippage_bps"] = v.get("slippageBps"); out.append(v)

    v2 = dict(k0)
    if "only_direct" in v2 and "onlyDirectRoutes" not in v2:
        v2["onlyDirectRoutes"] = v2.get("only_direct")
    if "slippage_bps" in v2 and "slippageBps" not in v2:
        v2["slippageBps"] = v2.get("slippage_bps")
    if v2 != k0:
        out.append(v2)

    seen: set[str] = set()
    uniq: List[Dict[str, Any]] = []
    for d in out:
        sig = "|".join(f"{k}={d.get(k)!r}" for k in sorted(d.keys()))
        if sig in seen:
            continue
        seen.add(sig)
        uniq.append(d)
    return uniq

def _drop_keys(d: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    dd = dict(d)
    for k in keys:
        dd.pop(k, None)
    return dd

async def _call_lightning_quote(fn_name: str, **kwargs: Any) -> Optional[Dict[str, Any]]:
    L = _get_lightning()
    if L is None:
        return None
    fn = getattr(L, fn_name, None)
    if not callable(fn):
        return None

    variants = _kw_variants(dict(kwargs))
    tries: List[Dict[str, Any]] = []
    for v in variants:
        tries.append(v)
        tries.append(_drop_keys(v, "only_direct", "onlyDirectRoutes"))
        tries.append(_drop_keys(v, "slippage_bps", "slippageBps"))
        tries.append(_drop_keys(v, "only_direct", "onlyDirectRoutes", "slippage_bps", "slippageBps"))

    last_non_auth: Optional[Exception] = None
    for kw in tries:
        try:
            out = await fn(**kw)  # type: ignore[misc]
            return out if isinstance(out, dict) else None
        except TypeError:
            continue
        except Exception as e:
            if ROUTER_STRICT_AUTH and _is_auth_error(e):
                raise e
            last_non_auth = e
            break

    if last_non_auth is not None:
        log.every("LIGHTNING_QUOTE_FAIL", 10, msg="LIGHTNING_QUOTE_FAIL", fn=fn_name, err=_short(last_non_auth, 220))
    return None

def _quote_has_out_amount(q: Dict[str, Any]) -> bool:
    for k in ("outAmount", "out_amount_raw", "out_amount_lamports", "out_amount"):
        v = q.get(k)
        if v is None:
            continue
        with contextlib.suppress(Exception):
            if int(v) > 0:
                return True
    return False

async def _prefer_lightning_or_jup(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: Optional[bool],
    lightning_fn: str,
    lightning_kwargs: Dict[str, Any],
) -> Dict[str, Any]:
    q = await _call_lightning_quote(lightning_fn, **lightning_kwargs)
    if isinstance(q, dict) and _quote_has_out_amount(q):
        return q
    return await _jup_quote(
        input_mint=input_mint,
        output_mint=output_mint,
        amount=int(amount),
        slippage_bps=int(slippage_bps),
        only_direct=only_direct,
    )


# =============================================================================
# Diagnostics / counters
# =============================================================================
_C: Dict[str, int] = {
    "quotes": 0, "buy_quotes": 0, "sell_quotes": 0, "price_quotes": 0,
    "buy_swaps": 0, "sell_swaps": 0,
    "rg_blocks": 0, "fallback_only": 0,
    "raydium_fallbacks": 0, "pump_fallbacks": 0,
    "quote_budget_blocks": 0, "auth_bubbles": 0,
}

def _inc(k: str, n: int = 1) -> None:
    try:
        _C[k] = int(_C.get(k, 0)) + int(n)
    except Exception:
        _C[k] = 0

def stats() -> Dict[str, Any]:
    return {
        "router_version": ROUTER_VERSION,
        **_C,
        "quote_cache": len(_QUOTE_CACHE),
        "quote_flight": len(_QUOTE_FLIGHT),
        "tokens_known": len(_TOK_META),
        "http_alive": int(_http_client is not None and not _http_client.is_closed),
        "allow_jupiter": int(bool(ROUTER_ALLOW_JUPITER)),
        "allow_raydium": int(bool(ALLOW_RAYDIUM)),
        "allow_pump": int(bool(ALLOW_PUMP)),
        "dry_run": int(bool(DRY_RUN)),
        "strict_auth": int(bool(ROUTER_STRICT_AUTH)),
        "jup_base": JUP_BASE_URL,
        "jup_quote_path": JUP_QUOTE_PATH,
        "jup_urls": [u for (u, _) in _jup_quote_urls()][:8],
        "http2": int(bool(HTTP_HTTP2)),
        "rotate_endpoints": int(bool(ROUTER_ROTATE_ENDPOINTS)),
    }


# =============================================================================
# Generic quote API (drift-tolerant)
# =============================================================================
async def get_quote(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    _inc("quotes", 1)

    if args and not kwargs:
        if len(args) in (3, 4):
            kwargs = {"inputMint": args[0], "outputMint": args[1], "amount": args[2]}
            if len(args) == 4:
                kwargs["slippageBps"] = args[3]
        else:
            raise TypeError("get_quote positional form must be (inputMint, outputMint, amount[, slippageBps])")

    if not ROUTER_ALLOW_JUPITER:
        raise RouterError("Jupiter disabled by env (ROUTER_ALLOW_JUPITER/JUP_ENABLED)")

    input_mint = kwargs.get("inputMint", kwargs.get("input_mint"))
    output_mint = kwargs.get("outputMint", kwargs.get("output_mint"))
    amount = int(kwargs.get("amount") or 0)

    sbps = kwargs.get("slippageBps", kwargs.get("slippage_bps"))
    sbps_i = _clamp_slippage_bps(int(sbps) if sbps is not None else None)

    od = kwargs.get("only_direct")
    if od is None:
        od = kwargs.get("onlyDirectRoutes")
    only_direct = bool(od) if isinstance(od, (bool, int)) else (True if JUP_DEFAULT_ONLY_DIRECT else None)

    im = _sanitize_mint(input_mint)
    om = _sanitize_mint(output_mint)
    if amount <= 0:
        raise ValueError("amount must be a positive integer")

    key = _qkey(im, om, int(amount), int(sbps_i), only_direct)
    cached = _get_cached_quote(key)
    if cached is not None:
        return dict(cached)

    async def _fetch() -> Dict[str, Any]:
        async with _quote_sema:
            if not _budget_before_quote(units=1):
                _inc("quote_budget_blocks", 1)
                raise RouterError("quote_blocked_by_budget")
            try:
                q = await _quote_with_retries(
                    lambda: _prefer_lightning_or_jup(
                        input_mint=im,
                        output_mint=om,
                        amount=int(amount),
                        slippage_bps=int(sbps_i),
                        only_direct=only_direct,
                        lightning_fn="get_quote",
                        lightning_kwargs={
                            "inputMint": im, "outputMint": om, "amount": int(amount),
                            "slippageBps": int(sbps_i),
                            "onlyDirectRoutes": only_direct,
                            "only_direct": only_direct,
                        },
                    )
                )
                out = dict(q)
                out["inputMint"] = out.get("inputMint") or im
                out["outputMint"] = out.get("outputMint") or om
                out["router_only_direct"] = bool(only_direct) if only_direct is not None else None
                out["router_score"] = float(_route_score(out))
                _set_cached_quote(key, out)
                return out
            finally:
                _budget_after_quote(units=1)

    return await _singleflight(key, _fetch)

quote = get_quote
swap_quote = get_quote
get_swap_quote = get_quote
jup_quote = get_quote
jupiter_quote = get_quote


# =============================================================================
# Quote helpers (STABLE buy/sell APIs)
# =============================================================================
async def get_buy_quote(
    mint: str,
    lamports_in: Optional[int] = None,
    amount_sol: Optional[float] = None,
    slippage_bps: Optional[int] = None,
    only_direct: Optional[bool] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    _inc("buy_quotes", 1)

    if not ROUTER_ALLOW_JUPITER:
        raise RouterError("Jupiter disabled by env (ROUTER_ALLOW_JUPITER/JUP_ENABLED)")

    if slippage_bps is None:
        sb = kwargs.get("slippageBps")
        if sb is not None:
            with contextlib.suppress(Exception):
                slippage_bps = int(sb)

    if only_direct is None:
        od = kwargs.get("onlyDirectRoutes")
        if isinstance(od, (bool, int)):
            only_direct = bool(od)

    m = _sanitize_mint(mint)

    if lamports_in is None:
        if amount_sol is None:
            amount_sol = _envf("PF_TRADE_SOL", 0.0)
        lamports_in = int(round(float(amount_sol or 0.0) * LAMPORTS_PER_SOL_F))

    lamports_in = int(lamports_in)
    if lamports_in <= 0:
        raise ValueError("Provide a positive lamports_in or amount_sol > 0")

    sbps = _clamp_slippage_bps(slippage_bps)
    od2 = only_direct if isinstance(only_direct, bool) else (True if JUP_DEFAULT_ONLY_DIRECT else None)

    if m == SOL_MINT:
        lam_in = max(1, int(lamports_in))
        return {
            "inputMint": SOL_MINT,
            "outputMint": SOL_MINT,
            "inAmount": str(lam_in),
            "outAmount": str(lam_in),
            "lamports_in": int(lam_in),
            "out_amount_raw": int(lam_in),
            "out_amount_ui": float(lam_in) / LAMPORTS_PER_SOL_F,
            "decimals": st.NATIVE_SOL_DECIMALS,
            "priceImpactPct": 0.0,
            "routePlan": [],
            "router_note": "SOL->SOL shortcut",
            "router_score": 1.0,
        }

    key = _qkey(SOL_MINT, m, int(lamports_in), int(sbps), od2)
    cached = _get_cached_quote(key)
    if cached is not None:
        return dict(cached)

    async def _fetch() -> Dict[str, Any]:
        async with _quote_sema:
            if not _budget_before_quote(units=1):
                _inc("quote_budget_blocks", 1)
                raise RouterError("quote_blocked_by_budget")
            try:
                try:
                    q = await _quote_with_retries(
                        lambda: _prefer_lightning_or_jup(
                            input_mint=SOL_MINT,
                            output_mint=m,
                            amount=int(lamports_in),
                            slippage_bps=int(sbps),
                            only_direct=od2,
                            lightning_fn="get_buy_quote",
                            lightning_kwargs={
                                "mint": m,
                                "lamports_in": int(lamports_in),
                                "slippage_bps": int(sbps),
                                "slippageBps": int(sbps),
                                "only_direct": od2,
                                "onlyDirectRoutes": od2,
                            },
                        )
                    )
                except Exception as e:
                    if _is_auth_error(e):
                        _inc("auth_bubbles", 1)
                    raise

                out_raw = int(q.get("outAmount") or q.get("out_amount_raw") or 0)
                dec_i = await st.get_token_decimals(m, default=0) or 0
                out_ui = float(out_raw) / float(10**dec_i) if (out_raw > 0 and dec_i > 0) else 0.0

                out = {
                    **q,
                    "inputMint": q.get("inputMint") or SOL_MINT,
                    "outputMint": q.get("outputMint") or m,
                    "lamports_in": int(lamports_in),
                    "out_amount_raw": int(out_raw),
                    "out_amount_ui": float(out_ui),
                    "decimals": int(dec_i),
                    "router_only_direct": bool(od2) if od2 is not None else None,
                }
                out["router_score"] = float(_route_score(out))
                _set_cached_quote(key, out)
                return out
            finally:
                _budget_after_quote(units=1)

    return await _singleflight(key, _fetch)


async def get_sell_quote(
    mint: str,
    amount_raw: int,
    slippage_bps: Optional[int] = None,
    only_direct: Optional[bool] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    _inc("sell_quotes", 1)

    if not ROUTER_ALLOW_JUPITER:
        raise RouterError("Jupiter disabled by env (ROUTER_ALLOW_JUPITER/JUP_ENABLED)")

    if not isinstance(amount_raw, int) or amount_raw <= 0:
        if "amount" in kwargs:
            with contextlib.suppress(Exception):
                amount_raw = int(kwargs["amount"])
        if not isinstance(amount_raw, int) or amount_raw <= 0:
            raise ValueError("amount_raw must be a positive integer")

    if slippage_bps is None:
        sb = kwargs.get("slippageBps")
        if sb is not None:
            with contextlib.suppress(Exception):
                slippage_bps = int(sb)

    if only_direct is None:
        od = kwargs.get("onlyDirectRoutes")
        if isinstance(od, (bool, int)):
            only_direct = bool(od)

    m = _sanitize_mint(mint)
    sbps = _clamp_slippage_bps(slippage_bps)
    od2 = only_direct if isinstance(only_direct, bool) else (True if JUP_DEFAULT_ONLY_DIRECT else None)

    if m == SOL_MINT:
        lamports_out = int(amount_raw)
        return {
            "inputMint": SOL_MINT,
            "outputMint": SOL_MINT,
            "inAmount": str(amount_raw),
            "outAmount": str(lamports_out),
            "amount_in_raw": int(amount_raw),
            "out_amount_lamports": int(lamports_out),
            "out_amount_sol": float(lamports_out) / LAMPORTS_PER_SOL_F,
            "decimals": st.NATIVE_SOL_DECIMALS,
            "priceImpactPct": 0.0,
            "routePlan": [],
            "router_note": "SOL->SOL shortcut",
            "router_score": 1.0,
        }

    key = _qkey(m, SOL_MINT, int(amount_raw), int(sbps), od2)
    cached = _get_cached_quote(key)
    if cached is not None:
        return dict(cached)

    async def _fetch() -> Dict[str, Any]:
        async with _quote_sema:
            if not _budget_before_quote(units=1):
                _inc("quote_budget_blocks", 1)
                raise RouterError("quote_blocked_by_budget")
            try:
                try:
                    q = await _quote_with_retries(
                        lambda: _prefer_lightning_or_jup(
                            input_mint=m,
                            output_mint=SOL_MINT,
                            amount=int(amount_raw),
                            slippage_bps=int(sbps),
                            only_direct=od2,
                            lightning_fn="get_sell_quote",
                            lightning_kwargs={
                                "mint": m,
                                "amount_raw": int(amount_raw),
                                "amount": int(amount_raw),
                                "slippage_bps": int(sbps),
                                "slippageBps": int(sbps),
                                "only_direct": od2,
                                "onlyDirectRoutes": od2,
                            },
                        )
                    )
                except Exception as e:
                    if _is_auth_error(e):
                        _inc("auth_bubbles", 1)
                    raise

                out_lamports = int(q.get("outAmount") or q.get("out_amount_lamports") or 0)
                dec_i = await st.get_token_decimals(m, default=0) or 0

                out = {
                    **q,
                    "inputMint": q.get("inputMint") or m,
                    "outputMint": q.get("outputMint") or SOL_MINT,
                    "amount_in_raw": int(amount_raw),
                    "out_amount_lamports": int(out_lamports),
                    "out_amount_sol": float(out_lamports) / LAMPORTS_PER_SOL_F,
                    "decimals": int(dec_i),
                    "router_only_direct": bool(od2) if od2 is not None else None,
                }
                out["router_score"] = float(_route_score(out))
                _set_cached_quote(key, out)
                return out
            finally:
                _budget_after_quote(units=1)

    return await _singleflight(key, _fetch)


quote_buy = get_buy_quote
quote_sell = get_sell_quote


# =============================================================================
# Price helper (mint -> SOL per 1 token)
# =============================================================================
_BAD_PRICE_BLACKLIST: Dict[str, float] = {}
_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}
_PRICE_FLIGHT: Dict[str, asyncio.Future[Any]] = {}
_price_lock = asyncio.Lock()

def _price_probe_amounts(dec: int) -> List[int]:
    d = _clamp_i(int(dec or 0), 0, 18)
    ui_steps = (1.0, 0.1, 0.01, 0.001)
    raws = sorted({int(round(ui * (10**d))) for ui in ui_steps if (ui * (10**d)) >= 1}, reverse=True)
    return raws or [1]

async def quote_price_via_jupiter(mint: str) -> Dict[str, Any]:
    _inc("price_quotes", 1)
    m = _sanitize_mint(mint)
    if m == SOL_MINT:
        return {"mint": SOL_MINT, "price_sol": 1.0, "note": "SOL"}

    dec = await st.get_token_decimals(m, default=6) or 6
    for amt_raw in _price_probe_amounts(dec):
        with contextlib.suppress(Exception):
            q = await get_sell_quote(mint=m, amount_raw=int(amt_raw))
            lamports = int(q.get("out_amount_lamports") or q.get("outAmount") or 0)
            if lamports <= 0:
                continue
            sol_out = float(lamports) / LAMPORTS_PER_SOL_F
            tokens_ui = float(amt_raw) / float(10**dec)
            px = sol_out / tokens_ui if tokens_ui > 0 else 0.0
            if px > 0:
                return {**q, "price_sol": float(px), "probe_raw": int(amt_raw)}
    return {"mint": m, "price_sol": 0.0, "error": "no_route"}

async def get_price_ui(mint: str) -> float:
    if mint == SOL_MINT:
        return 1.0

    raw = (mint or "").strip()
    if not raw:
        return 0.0

    exp_bad = _BAD_PRICE_BLACKLIST.get(raw, 0.0)
    if exp_bad and exp_bad >= _mono():
        return 0.0

    try:
        m = _sanitize_mint(raw)
    except Exception:
        _BAD_PRICE_BLACKLIST[raw] = _mono() + float(BAD_ID_TTL_SEC)
        log.every("PRICE_BAD_ID", n=25, msg="PRICE_BAD_ID", raw=raw)
        return 0.0

    if PRICE_TTL_SEC > 0:
        rec = _PRICE_CACHE.get(m)
        if rec and rec[0] >= _mono():
            return float(rec[1])

    async with _price_lock:
        fut = _PRICE_FLIGHT.get(m)
        if fut is None or fut.done():
            fut = asyncio.create_task(quote_price_via_jupiter(m))
            _PRICE_FLIGHT[m] = fut

    try:
        q = await fut
        px = float(q.get("price_sol") or 0.0)
        if px > 0 and PRICE_TTL_SEC > 0:
            _PRICE_CACHE[m] = (_mono() + float(PRICE_TTL_SEC), px)
        if px <= 0:
            _BAD_PRICE_BLACKLIST[raw] = _mono() + float(BAD_ID_TTL_SEC)
        return px
    except Exception:
        _BAD_PRICE_BLACKLIST[raw] = _mono() + float(BAD_ID_TTL_SEC)
        return 0.0
    finally:
        async with _price_lock:
            cur = _PRICE_FLIGHT.get(m)
            if cur is fut:
                _PRICE_FLIGHT.pop(m, None)

def clear_price_cache() -> None:
    _PRICE_CACHE.clear()
    _BAD_PRICE_BLACKLIST.clear()
    _PRICE_FLIGHT.clear()

price_of_ui = get_price_ui
price_of = get_price_ui


# =============================================================================
# Execution adapters (signature drift tolerant)
# =============================================================================
async def _call_lightning_buy(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: int,
    prequote: Optional[Dict[str, Any]] = None,
    return_meta: bool = False,
) -> Any:
    L = _get_lightning()
    if L is None:
        raise RouterError("lightning_impl unavailable")

    fn = getattr(L, "buy_via_lightning", None)
    if not callable(fn):
        raise RouterError("lightning_impl.buy_via_lightning missing")

    aliases = ("prequote", "quote", "prefetched_quote")

    if return_meta:
        for kw in aliases:
            try:
                return await fn(
                    mint=mint,
                    amount_sol=float(amount_sol),
                    slippage_bps=int(slippage_bps),
                    return_meta=True,
                    **({kw: prequote} if prequote else {}),
                )
            except TypeError:
                continue
        return await fn(mint=mint, amount_sol=float(amount_sol), slippage_bps=int(slippage_bps), return_meta=True)

    for kw in aliases:
        try:
            return await fn(
                mint=mint,
                amount_sol=float(amount_sol),
                slippage_bps=int(slippage_bps),
                **({kw: prequote} if prequote else {}),
            )
        except TypeError:
            continue
    return await fn(mint=mint, amount_sol=float(amount_sol), slippage_bps=int(slippage_bps))


async def _call_lightning_sell(
    *,
    mint: str,
    token_program: Optional[str],
    amount_raw: int,
    slippage_bps: int,
    prequote: Optional[Dict[str, Any]] = None,
) -> Any:
    L = _get_lightning()
    if L is None:
        raise RouterError("lightning_impl unavailable")

    fn = getattr(L, "sell_via_lightning", None)
    if not callable(fn):
        raise RouterError("lightning_impl.sell_via_lightning missing")

    aliases = ("prequote", "quote", "prefetched_quote")
    for kw in aliases:
        try:
            return await fn(
                mint=mint,
                token_program=token_program,
                amount_raw=int(amount_raw),
                slippage_bps=int(slippage_bps),
                **({kw: prequote} if prequote else {}),
            )
        except TypeError:
            continue
    return await fn(mint=mint, token_program=token_program, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))


# =============================================================================
# Fallback execution helpers
# =============================================================================
async def _raydium_buy_direct(*, mint: str, amount_sol: float, slippage_bps: int) -> Optional[str]:
    raydium = _get_raydium()
    if raydium is None:
        return None
    try:
        fn = getattr(raydium, "buy_via_raydium", None) or getattr(raydium, "buy", None)
        if callable(fn):
            return await fn(mint=mint, amount_sol=float(amount_sol), slippage_bps=int(slippage_bps))  # type: ignore[misc]
    except Exception as e:
        log.event("ROUTER_RAYDIUM_BUY_FAIL", mint=mint, err=_short(e, 220))
    return None


async def _raydium_sell_direct(*, mint: str, token_program: Optional[str], amount_raw: int, slippage_bps: int) -> Optional[str]:
    raydium = _get_raydium()
    if raydium is None:
        return None
    try:
        fn = getattr(raydium, "sell_via_raydium", None) or getattr(raydium, "sell", None)
        if callable(fn):
            return await fn(mint=mint, token_program=token_program, amount_raw=int(amount_raw), slippage_bps=int(slippage_bps))  # type: ignore[misc]
    except Exception as e:
        log.event("ROUTER_RAYDIUM_SELL_FAIL", mint=mint, err=_short(e, 220))
    return None


async def _pump_direct_buy_lamports(*, mint: str, in_amount_lamports: int, slippage_bps: int) -> Dict[str, Any]:
    pump_trader = _get_pump()
    if pump_trader is None:
        return {"ok": False, "error": "pump_trader not available", "mint": mint}
    try:
        fn1 = getattr(pump_trader, "buy_direct_lamports", None)
        if callable(fn1):
            return await fn1(mint=mint, in_amount_lamports=int(in_amount_lamports), slippage_bps=int(slippage_bps))  # type: ignore[misc]

        fn2 = getattr(pump_trader, "buy_pump_token", None)
        if callable(fn2):
            sol_amount = float(in_amount_lamports) / LAMPORTS_PER_SOL_F
            res = await fn2(mint=mint, sol_amount=float(sol_amount), slippage_bps=int(slippage_bps))  # type: ignore[misc]
            return res if isinstance(res, dict) else {"ok": True, "txid": str(res), "mint": mint}
    except Exception as e:
        log.event("PUMP_DIRECT_BUY_ERROR", mint=mint, err=_short(e, 240))
    return {"ok": False, "error": "no suitable pump_trader buy function found", "mint": mint}


# =============================================================================
# Swap wrappers (primary = lightning_impl, fallbacks optional)
# =============================================================================
async def buy_via_lightning(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: Optional[int] = None,
    return_meta: bool = False,
) -> Union[Optional[str], Tuple[Optional[str], Dict[str, Any]]]:
    _inc("buy_swaps", 1)

    sbps = _clamp_slippage_bps(slippage_bps)
    meta: Dict[str, Any] = {}

    m = _sanitize_mint(mint)
    lamports_in = int(round(float(amount_sol) * LAMPORTS_PER_SOL_F))
    if lamports_in <= 0:
        return (None, meta) if return_meta else None

    if not await _enforce_min_sol(required_spend_lamports=lamports_in):
        return (None, meta) if return_meta else None

    ok_rg, prequote, rg_present = await _rg_preflight(mint=m, lamports_in=lamports_in)
    if not ok_rg:
        _inc("rg_blocks", 1)
        log.event("ROUTER_BLOCK_RG", mint=m, lamports_in=lamports_in)
        return (None, meta) if return_meta else None

    fallback_only = bool(rg_present and prequote is None)
    if fallback_only:
        _inc("fallback_only", 1)
        if not DRY_RUN and ALLOW_RAYDIUM:
            _inc("raydium_fallbacks", 1)
            sig2 = await _raydium_buy_direct(mint=m, amount_sol=float(amount_sol), slippage_bps=sbps)
            return (sig2, meta) if return_meta else sig2
        return (None, meta) if return_meta else None

    try:
        if not ROUTER_ALLOW_JUPITER:
            raise RouterError("jupiter_disabled")

        if return_meta:
            res = await _call_lightning_buy(
                mint=m,
                amount_sol=float(amount_sol),
                slippage_bps=sbps,
                prequote=prequote,
                return_meta=True,
            )
            if isinstance(res, tuple) and len(res) == 2:
                sig, meta2 = res
                if isinstance(meta2, dict):
                    meta.update(meta2)
                return (sig, meta)
            if isinstance(res, dict):
                sig = res.get("sig") or res.get("signature") or res.get("tx_sig")
                meta2 = res.get("meta") if isinstance(res.get("meta"), dict) else {}
                if isinstance(meta2, dict):
                    meta.update(meta2)
                return (str(sig) if sig else None, meta)
            return (res if isinstance(res, str) else None, meta)

        res2 = await _call_lightning_buy(
            mint=m,
            amount_sol=float(amount_sol),
            slippage_bps=sbps,
            prequote=prequote,
            return_meta=False,
        )
        if isinstance(res2, dict):
            sig = res2.get("sig") or res2.get("signature") or res2.get("tx_sig")
            if sig:
                return (str(sig), meta) if return_meta else str(sig)
            return (None, meta) if return_meta else None

        if res2:
            return (str(res2), meta) if return_meta else str(res2)
        if DRY_RUN:
            return ("DRYRUN", meta) if return_meta else "DRYRUN"
    except Exception as e:
        log.event("ROUTER_LIGHTNING_BUY_ERROR", mint=m, err=_short(e, 240), amount_sol=float(amount_sol))

    if not DRY_RUN and ALLOW_RAYDIUM:
        _inc("raydium_fallbacks", 1)
        sig2 = await _raydium_buy_direct(mint=m, amount_sol=float(amount_sol), slippage_bps=sbps)
        return (sig2, meta) if return_meta else sig2

    return (None, meta) if return_meta else None


async def sell_via_lightning(
    *,
    mint: str,
    token_program: Optional[str],
    amount_raw: int,
    slippage_bps: Optional[int] = None,
) -> Optional[str]:
    _inc("sell_swaps", 1)

    sbps = _clamp_slippage_bps(slippage_bps)
    m = _sanitize_mint(mint)

    if not isinstance(amount_raw, int) or amount_raw <= 0:
        return None

    try:
        if not ROUTER_ALLOW_JUPITER:
            raise RouterError("jupiter_disabled")
        res = await _call_lightning_sell(
            mint=m,
            token_program=token_program,
            amount_raw=int(amount_raw),
            slippage_bps=sbps,
            prequote=None,
        )
        if isinstance(res, dict):
            sig = res.get("sig") or res.get("signature") or res.get("tx_sig")
            return str(sig) if sig else None
        if res:
            return str(res)
        if DRY_RUN:
            return "DRYRUN"
    except Exception as e:
        log.event("ROUTER_LIGHTNING_SELL_ERROR", mint=m, err=_short(e, 240), amount_raw=int(amount_raw))

    if not DRY_RUN and ALLOW_RAYDIUM:
        _inc("raydium_fallbacks", 1)
        return await _raydium_sell_direct(mint=m, token_program=token_program, amount_raw=int(amount_raw), slippage_bps=sbps)

    return None


async def pump_buy_with_fallback(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: Optional[int] = None,
    label: str = "pump-buy",
) -> Optional[str]:
    _inc("pump_fallbacks", 1)

    sbps = _clamp_slippage_bps(slippage_bps)
    raw_id = (mint or "").strip()
    lamports_in = int(round(float(amount_sol) * LAMPORTS_PER_SOL_F))
    if lamports_in <= 0:
        return None

    if not await _enforce_min_sol(required_spend_lamports=lamports_in):
        return None

    ok_rg, prequote, rg_present = await _rg_preflight(mint=raw_id, lamports_in=lamports_in)
    if not ok_rg:
        _inc("rg_blocks", 1)
        log.event("PUMP_BLOCK_RG", raw=raw_id, lamports_in=lamports_in)
        return None

    fallback_only = bool(rg_present and prequote is None)

    norm_mint: Optional[str] = None
    with contextlib.suppress(Exception):
        norm_mint = normalize_pump_id_to_mint(raw_id)

    if (not fallback_only) and norm_mint and ROUTER_ALLOW_JUPITER:
        try:
            res = await _call_lightning_buy(
                mint=_sanitize_mint(norm_mint),
                amount_sol=float(amount_sol),
                slippage_bps=sbps,
                prequote=prequote,
                return_meta=False,
            )
            if isinstance(res, dict):
                sig = res.get("sig") or res.get("signature") or res.get("tx_sig")
                if sig:
                    return str(sig)
            if res:
                return str(res)
            if DRY_RUN:
                return f"DRYRUN:{label}"
        except Exception as e:
            log.every("PUMP_JUP_FAIL", n=15, msg="PUMP_FALLBACK_JUP_ERROR", mint=raw_id, err=_short(e, 220), label=label)

    if DRY_RUN or not ALLOW_PUMP:
        return None

    res2 = await _pump_direct_buy_lamports(mint=raw_id, in_amount_lamports=int(lamports_in), slippage_bps=sbps)
    if res2.get("ok"):
        txid = res2.get("txid") or res2.get("signature")
        log.event("PUMP_FALLBACK_DIRECT_OK", mint=raw_id, txid=txid)
        return str(txid) if txid else None

    log.event("PUMP_FALLBACK_DIRECT_FAIL", mint=raw_id, err=_short(res2.get("error"), 220))
    return None


# =============================================================================
# Registries + dispatch (compat)
# =============================================================================
BUY_IMPLS: Dict[str, Callable[..., Any]] = {}
SELL_IMPLS: Dict[str, Callable[..., Any]] = {}

def _register_impls() -> None:
    BUY_IMPLS.setdefault("lightning", buy_via_lightning)
    SELL_IMPLS.setdefault("lightning", sell_via_lightning)

    if ALLOW_RAYDIUM:
        BUY_IMPLS.setdefault(
            "raydium",
            lambda **kw: _raydium_buy_direct(
                mint=_sanitize_mint(kw["mint"]),
                amount_sol=float(kw["amount_sol"]),
                slippage_bps=_clamp_slippage_bps(kw.get("slippage_bps")),
            ),
        )
        SELL_IMPLS.setdefault(
            "raydium",
            lambda **kw: _raydium_sell_direct(
                mint=_sanitize_mint(kw["mint"]),
                token_program=kw.get("token_program"),
                amount_raw=int(kw["amount_raw"]),
                slippage_bps=_clamp_slippage_bps(kw.get("slippage_bps")),
            ),
        )

    if ALLOW_PUMP:
        BUY_IMPLS.setdefault("pump", pump_buy_with_fallback)
        SELL_IMPLS.setdefault("pump", sell_via_lightning)

_register_impls()


async def buy(
    *,
    mint: str,
    amount_sol: float,
    impl: Optional[str] = None,
    slippage_bps: Optional[int] = None,
    return_meta: bool = False,
) -> Any:
    key = (impl or BUY_IMPL or "lightning").lower().strip()
    fn = BUY_IMPLS.get(key) or BUY_IMPLS["lightning"]

    if fn is pump_buy_with_fallback:
        sig = await fn(mint=mint, amount_sol=float(amount_sol), slippage_bps=slippage_bps)  # type: ignore[misc]
        return (sig, {}) if return_meta else sig

    res = await fn(mint=mint, amount_sol=float(amount_sol), slippage_bps=slippage_bps, return_meta=return_meta)  # type: ignore[misc]
    if return_meta:
        if isinstance(res, tuple) and len(res) == 2 and isinstance(res[1], dict):
            return res
        return (res if isinstance(res, str) else None, {})
    return res


async def sell(
    *,
    mint: str,
    token_program: Optional[str],
    amount_raw: int,
    impl: Optional[str] = None,
    slippage_bps: Optional[int] = None,
) -> Optional[str]:
    key = (impl or SELL_IMPL or "lightning").lower().strip()
    fn = SELL_IMPLS.get(key) or SELL_IMPLS["lightning"]
    return await fn(mint=mint, token_program=token_program, amount_raw=int(amount_raw), slippage_bps=slippage_bps)  # type: ignore[misc]


# =============================================================================
# Router object + get_router()
# =============================================================================
class Router:
    async def start(self) -> None:
        return None

    async def shutdown(self) -> None:
        await shutdown()

    async def close_http(self) -> None:
        await close_http()

    async def get_quote(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await get_quote(*args, **kwargs)

    async def get_buy_quote(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await get_buy_quote(*args, **kwargs)

    async def get_sell_quote(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return await get_sell_quote(*args, **kwargs)

    async def get_price_ui(self, mint: str) -> float:
        return await get_price_ui(mint)

    async def buy(self, *args: Any, **kwargs: Any) -> Any:
        return await buy(*args, **kwargs)

    async def sell(self, *args: Any, **kwargs: Any) -> Optional[str]:
        return await sell(*args, **kwargs)

    def stats(self) -> Dict[str, Any]:
        return stats()

_ROUTER_SINGLETON: Optional[Router] = None

def get_router() -> Router:
    global _ROUTER_SINGLETON
    if _ROUTER_SINGLETON is None:
        _ROUTER_SINGLETON = Router()
    return _ROUTER_SINGLETON


# =============================================================================
# Lifecycle hooks
# =============================================================================
async def start() -> None:
    return None

async def close_http() -> None:
    await _close_http_local()

async def shutdown() -> None:
    await _close_http_local()
    L = _get_lightning()
    if L is not None:
        with contextlib.suppress(Exception):
            close_fn = getattr(L, "close_http", None)
            if callable(close_fn):
                await close_fn()  # type: ignore[misc]
    return None


__all__ = [
    "Router",
    "get_router",
    "start",
    "shutdown",
    "close_http",
    "get_quote",
    "quote",
    "swap_quote",
    "get_swap_quote",
    "jup_quote",
    "jupiter_quote",
    "get_buy_quote",
    "get_sell_quote",
    "quote_buy",
    "quote_sell",
    "get_price_ui",
    "price_of_ui",
    "price_of",
    "quote_price_via_jupiter",
    "get_tokens",
    "get_token_symbol",
    "mint_decimals",
    "get_decimals",
    "sol_decimals",
    "get_sol_decimals",
    "buy_via_lightning",
    "sell_via_lightning",
    "pump_buy_with_fallback",
    "buy",
    "sell",
    "BUY_IMPLS",
    "SELL_IMPLS",
    "normalize_pump_id_to_mint",
    "is_valid_pubkey_str",
    "clear_quote_cache",
    "clear_price_cache",
    "stats",
    "SOL_MINT",
    "LAMPORTS_PER_SOL",
    "RPC_URL",
    "ROUTER_VERSION",
    "load_keypair",
    "get_pubkey_str",
    "sign_and_send_b64_tx",
    "_send_b64_with_diag",
]
