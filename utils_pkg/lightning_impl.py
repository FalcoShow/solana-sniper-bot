from __future__ import annotations

"""
utils_pkg/lightning_impl.py — Jupiter execution layer (V3+++ LOCK-IN)
====================================================================

Execution engine behind utils_pkg/router.py.

Key upgrades in this LOCK-IN rewrite
------------------------------------
1) Correct endpoint model (no more invalid URL combos):
   - Supports Metis swap API:
       https://api.jup.ag/swap/v1/quote
       https://api.jup.ag/swap/v1/swap
   - Supports Public v6 API:
       https://quote-api.jup.ag/v6/quote
       https://quote-api.jup.ag/v6/swap
   - Uses env FIRST if provided (full URLs, comma-separated):
       JUP_QUOTE_URL, JUP_SWAP_URL
   - Otherwise falls back to:
       JUP_BASE_URL (+ JUP_QUOTE_PATH/JUP_SWAP_PATH)

2) Per-endpoint failover:
   - cooldown + score per *endpoint URL* (not only base)
   - last_good boosts the next choice

3) Safe, production-friendly:
   - no network at import time
   - single lazy httpx.AsyncClient
   - qps gate + concurrency guard
   - breaker + retry with jitter
   - DRYRUN semantics preserved

4) Router compatibility:
   - get_quote / quote
   - get_buy_quote / get_sell_quote
   - buy_via_lightning / sell_via_lightning
   - close_http, stats, _variant_flags

Notes
-----
- If quote-api.jup.ag DNS fails on your machine, keep Metis endpoints first
  in JUP_QUOTE_URL/JUP_SWAP_URL, or remove quote-api from the list.
"""

import asyncio
import contextlib
import importlib.util
import inspect
import os
import random
import time
from dataclasses import dataclass
from typing import Any

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

    log.event = _event  # type: ignore[attr-defined]


if not hasattr(log, "every"):

    def _every(
        key: str, n: int = 30, msg: str | None = None, **kw: Any
    ) -> None:  # pragma: no cover
        try:
            now = time.monotonic()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(max(1, int(n))):
                store[key] = now
                _every._store = store  # type: ignore[attr-defined]
                log.event(msg or key, **kw)
        except Exception:
            return None

    log.every = _every  # type: ignore[attr-defined]


def _warn(name: str, **kw: Any) -> None:
    with contextlib.suppress(Exception):
        log.event(name, **kw)


# ──────────────────────────────────────────────────────────────────────────────
# Optional budget hooks (sync OR async supported)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover

    class _BudgetNoop:
        def before_quote(self, units: int = 1) -> bool:
            return True

        def after_quote(self, units: int = 1) -> None:
            return None

        def before_swap(self, units: int = 1) -> bool:
            return True

        def after_swap(self, units: int = 1) -> None:
            return None

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
# Wallet helpers (lazy)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg.solana_wallet import (  # type: ignore
        get_pubkey_str,
        load_keypair,
        sign_and_send_b64_tx,
    )

    try:
        from utils_pkg.solana_wallet import _send_b64_with_diag  # type: ignore
    except Exception:  # pragma: no cover
        _send_b64_with_diag = None  # type: ignore
except Exception:  # pragma: no cover
    get_pubkey_str = None  # type: ignore
    load_keypair = None  # type: ignore
    sign_and_send_b64_tx = None  # type: ignore
    _send_b64_with_diag = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Mint helpers
# ──────────────────────────────────────────────────────────────────────────────
KNOWN_SOL_MINT = "So11111111111111111111111111111111111111112"
KNOWN_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
KNOWN_BAD_USDC_TYPO = "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v"

try:
    from utils_pkg.mints import (  # type: ignore
        SOL_MINT as CANON_SOL_MINT,
        USDC_MINT as CANON_USDC_MINT,
        normalize_mint,
    )
except Exception:  # pragma: no cover

    def normalize_mint(x: str) -> str:  # type: ignore
        return (x or "").strip()

    CANON_SOL_MINT = KNOWN_SOL_MINT
    CANON_USDC_MINT = KNOWN_USDC_MINT


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
    mm = normalize_mint(m)
    mm = _fix_common_mint_typos(mm)
    if not _looks_like_mint(mm):
        raise ValueError(f"invalid_mint:{label}")
    return mm


def _same_mint(a: str, b: str) -> bool:
    return normalize_mint(a) == normalize_mint(b)


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
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


def _env_tristate(name: str) -> bool | None:
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


def _mono() -> float:
    return time.monotonic()


# ──────────────────────────────────────────────────────────────────────────────
# HTTP/2 detection (optional)
# ──────────────────────────────────────────────────────────────────────────────
_H2_AVAILABLE = importlib.util.find_spec("h2") is not None


def _effective_http2() -> bool:
    req = _env_bool("JUP_HTTP2", False)
    if not req:
        return False
    return _H2_AVAILABLE


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
LAMPORTS_PER_SOL = 1_000_000_000
LIGHTNING_VERSION = "v3+++_lockin_lightning_2026-01-05"

SOL_MINT = _env_str("SOL_MINT", str(CANON_SOL_MINT or KNOWN_SOL_MINT))
USDC_MINT = _env_str("USDC_MINT", str(CANON_USDC_MINT or KNOWN_USDC_MINT))

DEFAULT_METIS_BASE = "https://api.jup.ag"
DEFAULT_PUBLIC_BASE = "https://quote-api.jup.ag"

DEFAULT_METIS_QUOTE_PATH = "/swap/v1/quote"
DEFAULT_METIS_SWAP_PATH = "/swap/v1/swap"
DEFAULT_V6_QUOTE_PATH = "/v6/quote"
DEFAULT_V6_SWAP_PATH = "/v6/swap"


def _mask_key(k: str) -> str:
    k = (k or "").strip()
    if not k:
        return ""
    if len(k) <= 8:
        return "*" * len(k)
    return k[:3] + ("*" * (len(k) - 6)) + k[-3:]


def _parse_csv(val: str) -> list[str]:
    out: list[str] = []
    for part in (val or "").split(","):
        s = (part or "").strip()
        if s:
            out.append(s)
    return out


def _join_url(base: str, path: str) -> str:
    b = (base or "").rstrip("/")
    p = (path or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    return b + p


def _ensure_full_quote_url(s: str) -> str:
    """
    Accepts either a full URL, or a base host. Converts base host into correct default:
      - quote-api.* -> /v6/quote
      - api.jup.ag  -> /swap/v1/quote
    """
    t = (s or "").strip().rstrip("/")
    if not t:
        return ""
    if t.startswith("http://") or t.startswith("https://"):
        # If it's already a full endpoint URL (has /...), leave it.
        # If it's only a base (no slash beyond scheme), append best default.
        try:
            after = t.split("://", 1)[1]
            if "/" not in after:
                if "quote-api" in after:
                    return _join_url(t, DEFAULT_V6_QUOTE_PATH)
                return _join_url(t, DEFAULT_METIS_QUOTE_PATH)
        except Exception:
            pass
        return t
    # treat as host-ish
    if "quote-api" in t:
        return "https://" + t + DEFAULT_V6_QUOTE_PATH
    return "https://" + t + DEFAULT_METIS_QUOTE_PATH


def _ensure_full_swap_url(s: str) -> str:
    t = (s or "").strip().rstrip("/")
    if not t:
        return ""
    if t.startswith("http://") or t.startswith("https://"):
        try:
            after = t.split("://", 1)[1]
            if "/" not in after:
                if "quote-api" in after:
                    return _join_url(t, DEFAULT_V6_SWAP_PATH)
                return _join_url(t, DEFAULT_METIS_SWAP_PATH)
        except Exception:
            pass
        return t
    if "quote-api" in t:
        return "https://" + t + DEFAULT_V6_SWAP_PATH
    return "https://" + t + DEFAULT_METIS_SWAP_PATH


@dataclass(frozen=True)
class _Cfg:
    # endpoint URLs (full)
    quote_urls: list[str]
    swap_urls: list[str]

    # quote params
    only_direct: bool
    restrict_intermediate: bool
    default_slippage_bps: int
    as_legacy_tx: bool

    # swap build options
    wrap_unwrap_sol: bool
    dynamic_slippage: bool
    skip_user_accounts_rpc_calls: bool
    use_token_ledger: bool
    use_shared_accounts: bool
    force_shared: bool
    blockhash_slots_to_expiry: int
    compute_unit_limit: int
    compute_unit_price_micro: int
    platform_fee_bps: int
    fee_account: str
    tracking_account: str
    payer: str

    # http
    timeout_sec: float
    connect_timeout: float
    pool_timeout: float
    max_keepalive: int
    max_conn: int
    tls_verify: bool
    http2_effective: bool

    # rate
    max_qps: int
    max_concurrency: int

    # retry + breaker
    retry_max: int
    backoff_sec: float
    jitter_sec: float
    breaker_enabled: bool
    fail_limit: int
    breaker_cooldown_sec: float

    # per-endpoint cooldown
    endpoint_fail_limit: int
    endpoint_cooldown_sec: float

    # misc
    unroutable_ttl_sec: int
    requote_bump_pct: int
    quote_ttl_sec: float
    strict_auth: bool

    # dryrun + safety
    dry_run: bool
    dryrun_build_tx: bool
    disable_buys: bool
    disable_sells: bool
    kill_switch: bool
    skip_preflight: bool

    # headers
    has_api_key: bool
    api_key_mask: str


_CFG: _Cfg | None = None
_CFG_TS: float = 0.0


def _load_cfg() -> _Cfg:
    # endpoints from env (full URLs, comma-separated)
    q_urls_raw = _env_str("JUP_QUOTE_URLS", _env_str("JUP_QUOTE_URL", "")).strip()
    s_urls_raw = _env_str("JUP_SWAP_URLS", _env_str("JUP_SWAP_URL", "")).strip()

    quote_urls: list[str] = []
    swap_urls: list[str] = []

    if q_urls_raw:
        for x in _parse_csv(q_urls_raw):
            u = _ensure_full_quote_url(x)
            if u and u not in quote_urls:
                quote_urls.append(u)

    if s_urls_raw:
        for x in _parse_csv(s_urls_raw):
            u = _ensure_full_swap_url(x)
            if u and u not in swap_urls:
                swap_urls.append(u)

    # fallback to base+path model
    base = (
        _env_str("JUP_BASE_URL", "") or _env_str("JUP_UPSTREAM", "") or DEFAULT_METIS_BASE
    ).rstrip("/")
    alt = _env_str("JUP_ALT_BASE_URLS", "")
    bases = [base]
    for b in _parse_csv(alt):
        bb = (b or "").strip().rstrip("/")
        if bb and bb not in bases:
            bases.append(bb)

    quote_path = _env_str("JUP_QUOTE_PATH", "").strip()
    swap_path = _env_str("JUP_SWAP_PATH", "").strip()

    # smart defaults if not specified
    # if the base looks like quote-api => v6 defaults, else metis defaults
    def _default_qp(b: str) -> str:
        return DEFAULT_V6_QUOTE_PATH if "quote-api" in b else DEFAULT_METIS_QUOTE_PATH

    def _default_sp(b: str) -> str:
        return DEFAULT_V6_SWAP_PATH if "quote-api" in b else DEFAULT_METIS_SWAP_PATH

    for b in bases:
        if not quote_urls:
            quote_urls.append(_join_url(b, quote_path or _default_qp(b)))
        if not swap_urls:
            swap_urls.append(_join_url(b, swap_path or _default_sp(b)))

    # Always allow quote failover between “known good” defaults if env gave only one side
    # (Keeps the bot alive if one domain is blocked)
    if DEFAULT_METIS_BASE not in [u.split("/")[0] + "//" + u.split("/")[2] for u in quote_urls]:
        # add metis quote as last resort
        with contextlib.suppress(Exception):
            u = _join_url(DEFAULT_METIS_BASE, DEFAULT_METIS_QUOTE_PATH)
            if u not in quote_urls:
                quote_urls.append(u)

    if DEFAULT_METIS_BASE not in [u.split("/")[0] + "//" + u.split("/")[2] for u in swap_urls]:
        with contextlib.suppress(Exception):
            u = _join_url(DEFAULT_METIS_BASE, DEFAULT_METIS_SWAP_PATH)
            if u not in swap_urls:
                swap_urls.append(u)

    only_direct = _env_bool("JUP_ONLY_DIRECT", _env_bool("JUP_ONLY_DIRECT_ROUTES", False))
    restrict_intermediate = _env_bool("JUP_RESTRICT_INTERMEDIATE_TOKENS", True)

    default_slippage_bps = _clamp_i(
        _env_int("PF_SLIPPAGE_BPS", _env_int("SLIPPAGE_BPS", 30)), 1, 5000
    )
    as_legacy_tx = _env_bool("JUP_AS_LEGACY_TX", _env_bool("JUP_AS_LEGACY", False))

    wrap_unwrap = _env_bool("PF_WRAP_UNWRAP_SOL", True)
    dynamic_slippage = _env_bool("JUP_DYNAMIC_SLIPPAGE", False)
    skip_user_accounts_rpc = _env_bool("JUP_SKIP_USER_ACCOUNTS_RPC_CALLS", False)

    use_token_ledger = _env_bool("JUP_USE_TOKEN_LEDGER", False)

    force_shared = _env_bool("JUP_FORCE_SHARED_ACCOUNTS", False)
    tri = _env_tristate("JUP_USE_SHARED_ACCOUNTS")
    use_shared = bool(tri) if tri is not None else _env_bool("JUP_USE_SHARED_ACCOUNTS", False)
    if force_shared:
        use_shared = True

    blockhash_slots_to_expiry = max(0, _env_int("JUP_BLOCKHASH_SLOTS_TO_EXPIRY", 0))
    compute_unit_limit = max(0, _env_int("PF_COMPUTE_UNIT_LIMIT", 0))
    compute_unit_price_micro = max(
        0, _env_int("PF_COMPUTE_UNIT_PRICE_MICRO", _env_int("JUP_COMPUTE_UNIT_PRICE_MICRO", 0))
    )

    platform_fee_bps = _clamp_i(_env_int("JUP_PLATFORM_FEE_BPS", 0), 0, 10_000)
    fee_account = _env_str("JUP_FEE_ACCOUNT", "")
    tracking_account = _env_str("JUP_TRACKING_ACCOUNT", "")
    payer = _env_str("JUP_PAYER", "")

    timeout_sec = max(5.0, _env_float("JUP_HTTP_TIMEOUT_SEC", 20.0))
    connect_timeout = max(2.0, _env_float("JUP_HTTP_CONNECT_TIMEOUT", 5.0))
    pool_timeout = max(2.0, _env_float("JUP_HTTP_POOL_TIMEOUT", 5.0))
    max_keepalive = max(5, _env_int("JUP_HTTP_MAX_KEEPALIVE", 20))
    max_conn = max(10, _env_int("JUP_HTTP_MAX_CONN", 60))
    tls_verify = _env_bool("JUP_TLS_VERIFY", True)
    http2_effective = bool(_effective_http2())

    max_qps = max(1, _env_int("JUP_MAX_QPS", 2))
    max_concurrency = max(1, _env_int("JUP_MAX_CONCURRENCY", 8))

    retry_max = max(0, _env_int("JUP_HTTP_RETRY_MAX", 2))
    backoff = max(0.1, _env_float("JUP_HTTP_RETRY_BACKOFF_SEC", 0.8))
    jitter = max(0.0, _env_float("JUP_HTTP_RETRY_JITTER_SEC", 0.25))

    breaker_enabled = _env_bool("JUP_BREAKER_ENABLED", True)
    fail_limit = max(1, _env_int("JUP_FAIL_LIMIT", 8))
    breaker_cooldown = max(5.0, _env_float("JUP_BREAKER_COOLDOWN_SEC", 30.0))

    endpoint_fail_limit = max(1, _env_int("JUP_ENDPOINT_FAIL_LIMIT", 3))
    endpoint_cooldown = max(5.0, _env_float("JUP_ENDPOINT_COOLDOWN_SEC", 20.0))

    unroutable_ttl = max(30, _env_int("JUP_UNROUTABLE_TTL_SEC", 1800))
    requote_bump_pct = max(1, _env_int("JUP_REQUOTE_BUMP_PCT", 5))
    quote_ttl_sec = max(0.0, float(_env_float("LIGHTNING_QUOTE_TTL_SEC", 0.0)))

    strict_auth = _env_bool("ROUTER_STRICT_AUTH", _env_bool("JUP_STRICT_AUTH", False))

    dry_run = (
        _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)
    )
    dryrun_build_tx = _env_bool("LIGHTNING_DRYRUN_BUILD_TX", False)

    disable_buys = _env_bool("DISABLE_BUYS", False)
    disable_sells = _env_bool("DISABLE_SELLS", False)
    kill_switch = _env_bool("KILL_SWITCH", False)
    skip_preflight = _env_bool("SOLANA_SKIP_PREFLIGHT", True)

    api_key = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", _env_str("JUP_KEY", "")))
    has_key = bool(api_key.strip())
    api_key_mask = _mask_key(api_key)

    return _Cfg(
        quote_urls=quote_urls,
        swap_urls=swap_urls,
        only_direct=only_direct,
        restrict_intermediate=restrict_intermediate,
        default_slippage_bps=default_slippage_bps,
        as_legacy_tx=as_legacy_tx,
        wrap_unwrap_sol=wrap_unwrap,
        dynamic_slippage=dynamic_slippage,
        skip_user_accounts_rpc_calls=skip_user_accounts_rpc,
        use_token_ledger=use_token_ledger,
        use_shared_accounts=use_shared,
        force_shared=force_shared,
        blockhash_slots_to_expiry=blockhash_slots_to_expiry,
        compute_unit_limit=compute_unit_limit,
        compute_unit_price_micro=compute_unit_price_micro,
        platform_fee_bps=platform_fee_bps,
        fee_account=fee_account,
        tracking_account=tracking_account,
        payer=payer,
        timeout_sec=timeout_sec,
        connect_timeout=connect_timeout,
        pool_timeout=pool_timeout,
        max_keepalive=max_keepalive,
        max_conn=max_conn,
        tls_verify=tls_verify,
        http2_effective=http2_effective,
        max_qps=max_qps,
        max_concurrency=max_concurrency,
        retry_max=retry_max,
        backoff_sec=backoff,
        jitter_sec=jitter,
        breaker_enabled=breaker_enabled,
        fail_limit=fail_limit,
        breaker_cooldown_sec=breaker_cooldown,
        endpoint_fail_limit=endpoint_fail_limit,
        endpoint_cooldown_sec=endpoint_cooldown,
        unroutable_ttl_sec=unroutable_ttl,
        requote_bump_pct=requote_bump_pct,
        quote_ttl_sec=quote_ttl_sec,
        strict_auth=strict_auth,
        dry_run=dry_run,
        dryrun_build_tx=dryrun_build_tx,
        disable_buys=disable_buys,
        disable_sells=disable_sells,
        kill_switch=kill_switch,
        skip_preflight=skip_preflight,
        has_api_key=has_key,
        api_key_mask=api_key_mask,
    )


def _cfg() -> _Cfg:
    global _CFG, _CFG_TS
    # reload if env changes in-process (helps your PowerShell tests)
    # You can tune by setting LIGHTNING_CFG_TTL_SEC if you want.
    ttl = max(0.0, float(_env_float("LIGHTNING_CFG_TTL_SEC", 0.0)))
    now = time.time()
    if _CFG is None or (ttl > 0 and (now - _CFG_TS) > ttl):
        _CFG = _load_cfg()
        _CFG_TS = now
    return _CFG


# ──────────────────────────────────────────────────────────────────────────────
# Quote cache + singleflight
# ──────────────────────────────────────────────────────────────────────────────
_QUOTE_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_QUOTE_FLIGHT: dict[str, asyncio.Future[Any]] = {}
_QUOTE_LOCK = asyncio.Lock()


def _qkey(inp: str, out: str, amount: int, sbps: int, only_direct: bool | None) -> str:
    c = _cfg()
    od = c.only_direct if only_direct is None else bool(only_direct)
    return f"{inp}:{out}:{int(amount)}:{int(sbps)}:{'D' if od else 'A'}"


def _cache_get(key: str) -> dict[str, Any] | None:
    ttl = float(_cfg().quote_ttl_sec)
    if ttl <= 0:
        return None
    rec = _QUOTE_CACHE.get(key)
    if not rec:
        return None
    exp, payload = rec
    if exp >= _mono():
        return payload
    _QUOTE_CACHE.pop(key, None)
    return None


def _cache_set(key: str, payload: dict[str, Any]) -> None:
    ttl = float(_cfg().quote_ttl_sec)
    if ttl <= 0:
        return
    _QUOTE_CACHE[key] = (_mono() + ttl, payload)
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


# ──────────────────────────────────────────────────────────────────────────────
# Response classifiers
# ──────────────────────────────────────────────────────────────────────────────
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
            return "not tradable" in msg.lower()
        if isinstance(body, str):
            return "not tradable" in body.lower()
    except Exception:
        pass
    return False


def _is_transient_status(code: int) -> bool:
    return int(code) in (408, 425, 429, 500, 502, 503, 504)


# ──────────────────────────────────────────────────────────────────────────────
# Stats + breaker + endpoint scoring
# ──────────────────────────────────────────────────────────────────────────────
_STATS: dict[str, int] = {
    "quote_ok": 0,
    "quote_fail": 0,
    "swap_build_ok": 0,
    "swap_build_fail": 0,
    "send_ok": 0,
    "send_fail": 0,
    "no_route": 0,
    "breaker_trip": 0,
    "auth_fail": 0,
    "invalid_mint": 0,
    "token_not_tradable": 0,
    "prequote_build_fail": 0,
    "prequote_requote": 0,
    "noop_same_mint": 0,
    "wallet_missing": 0,
    "budget_block": 0,
    "endpoint_cooldown": 0,
    "all_endpoints_cooldown": 0,
}

_BREAKER_UNTIL: float = 0.0
_FAIL_COUNT: int = 0

_ENDPOINT_FAILS: dict[str, int] = {}
_ENDPOINT_UNTIL: dict[str, float] = {}
_ENDPOINT_SCORE: dict[str, int] = {}
_LAST_GOOD_QUOTE_URL: str | None = None
_LAST_GOOD_SWAP_URL: str | None = None

_UNROUTABLE_TTL: dict[tuple[str, str], float] = {}
_UNROUTABLE_LOCK = asyncio.Lock()

_RATE_LOCK = asyncio.Lock()
_LAST_TS: float = 0.0

_CONCURRENCY: asyncio.Semaphore | None = None


def _sem() -> asyncio.Semaphore:
    global _CONCURRENCY
    c = _cfg()
    if _CONCURRENCY is None or _CONCURRENCY._value != int(c.max_concurrency):  # type: ignore[attr-defined]
        _CONCURRENCY = asyncio.Semaphore(int(c.max_concurrency))
    return _CONCURRENCY


def _breaker_check() -> None:
    c = _cfg()
    if not c.breaker_enabled:
        return
    if time.time() < _BREAKER_UNTIL:
        raise RuntimeError("jup_breaker_active")


def _breaker_success() -> None:
    global _FAIL_COUNT, _BREAKER_UNTIL
    _FAIL_COUNT = 0
    _BREAKER_UNTIL = 0.0


def _breaker_fail(tag: str = "") -> None:
    global _FAIL_COUNT, _BREAKER_UNTIL
    c = _cfg()
    if not c.breaker_enabled:
        return
    if tag in {"no_route", "token_not_tradable", "invalid_mint"}:
        return
    _FAIL_COUNT += 1
    if int(c.fail_limit) <= _FAIL_COUNT:
        _BREAKER_UNTIL = time.time() + float(c.breaker_cooldown_sec)
        _STATS["breaker_trip"] += 1
        _warn("JUP_BREAKER_TRIPPED", until=_BREAKER_UNTIL, fails=_FAIL_COUNT)


async def _qps_gate() -> None:
    global _LAST_TS
    c = _cfg()
    min_interval = 1.0 / float(max(1, int(c.max_qps)))
    async with _RATE_LOCK:
        now = time.monotonic()
        wait_for = float(min_interval) - float(now - _LAST_TS)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _LAST_TS = time.monotonic()


async def _sleep_backoff(attempt: int) -> None:
    c = _cfg()
    base = float(c.backoff_sec) * (2 ** max(0, int(attempt)))
    jitter = float(c.jitter_sec) * (0.5 + random.random()) if c.jitter_sec > 0 else 0.0
    await asyncio.sleep(base + jitter)


def _safe_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except Exception:
        with contextlib.suppress(Exception):
            return {"text": (r.text or "")[:2000]}
        return {"text": ""}


def _endpoint_allowed(url: str) -> bool:
    until = float(_ENDPOINT_UNTIL.get(url, 0.0) or 0.0)
    return time.time() >= until


def _endpoint_note_ok(url: str) -> None:
    _ENDPOINT_FAILS[url] = 0
    _ENDPOINT_UNTIL.pop(url, None)
    _ENDPOINT_SCORE[url] = int(_ENDPOINT_SCORE.get(url, 0)) + 1


def _endpoint_note_fail(url: str) -> None:
    c = _cfg()
    n = int(_ENDPOINT_FAILS.get(url, 0)) + 1
    _ENDPOINT_FAILS[url] = n
    _ENDPOINT_SCORE[url] = int(_ENDPOINT_SCORE.get(url, 0)) - 1
    if n >= int(c.endpoint_fail_limit):
        _ENDPOINT_UNTIL[url] = time.time() + float(c.endpoint_cooldown_sec)
        _STATS["endpoint_cooldown"] += 1
        _warn("JUP_ENDPOINT_COOLDOWN", url=url[:140], fails=n, cooldown_sec=c.endpoint_cooldown_sec)


def _prefer_order(urls: list[str], last_good: str | None) -> list[str]:
    allowed = [u for u in urls if _endpoint_allowed(u)]
    if not allowed:
        _STATS["all_endpoints_cooldown"] += 1
        return []
    allowed.sort(key=lambda u: int(_ENDPOINT_SCORE.get(u, 0)), reverse=True)
    if last_good and last_good in allowed:
        allowed = [last_good] + [u for u in allowed if u != last_good]
    return allowed


async def _is_unroutable_pair(inp: str, out: str) -> bool:
    async with _UNROUTABLE_LOCK:
        exp = _UNROUTABLE_TTL.get((inp, out))
        return bool(exp and exp > time.time())


async def _mark_unroutable(inp: str, out: str, ttl: int | None = None) -> None:
    c = _cfg()
    async with _UNROUTABLE_LOCK:
        _UNROUTABLE_TTL[(inp, out)] = time.time() + float(ttl or c.unroutable_ttl_sec)
        if len(_UNROUTABLE_TTL) > 5000:
            now = time.time()
            for k, exp in list(_UNROUTABLE_TTL.items()):
                if exp <= now:
                    _UNROUTABLE_TTL.pop(k, None)


# ──────────────────────────────────────────────────────────────────────────────
# HTTP client (singleton, lazy)
# ──────────────────────────────────────────────────────────────────────────────
_client: httpx.AsyncClient | None = None
_headers_snapshot: tuple[str, str, str, bool, bool] | None = (
    None  # (api_key, bearer, ua, http2, verify)
)


def _jup_headers() -> dict[str, str]:
    api_key = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", _env_str("JUP_KEY", "")))
    bearer = _env_str("JUP_BEARER", _env_str("JUP_ACCESS_TOKEN", ""))
    ua = _env_str("JUP_USER_AGENT", "pnlsniper")

    h: dict[str, str] = {
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


def _get_client() -> httpx.AsyncClient:
    global _client, _headers_snapshot
    c = _cfg()

    api_key = _env_str("JUP_API_KEY", _env_str("JUP_PRO_KEY", _env_str("JUP_KEY", "")))
    bearer = _env_str("JUP_BEARER", _env_str("JUP_ACCESS_TOKEN", ""))
    ua = _env_str("JUP_USER_AGENT", "pnlsniper")
    snap = (api_key, bearer, ua, bool(c.http2_effective), bool(c.tls_verify))

    if (
        _client is None
        or _client.is_closed
        or (_headers_snapshot is not None and snap != _headers_snapshot)
    ):
        old = _client
        limits = httpx.Limits(
            max_keepalive_connections=int(c.max_keepalive), max_connections=int(c.max_conn)
        )
        timeout = httpx.Timeout(
            connect=float(c.connect_timeout),
            read=float(c.timeout_sec),
            write=float(c.timeout_sec),
            pool=float(c.pool_timeout),
        )
        _client = httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            headers=_jup_headers(),
            http2=bool(c.http2_effective),
            follow_redirects=True,
            verify=bool(c.tls_verify),
            trust_env=True,
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


# ──────────────────────────────────────────────────────────────────────────────
# Budget wrapper + hardened request
# ──────────────────────────────────────────────────────────────────────────────
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
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    budget_units: int = 1,
) -> Any:
    _breaker_check()
    await _qps_gate()

    is_get = method.upper() == "GET"
    budget_granted = False
    t0 = time.monotonic()

    try:
        if not await _budget_before(is_get=is_get, units=int(budget_units)):
            _STATS["budget_block"] += 1
            raise RuntimeError("quote_blocked_by_budget" if is_get else "swap_blocked_by_budget")
        budget_granted = True

        c = _cfg()
        for attempt in range(0, int(c.retry_max) + 1):
            cli = _get_client()
            try:
                async with _sem():
                    if is_get:
                        r = await cli.get(url, params=params)
                    else:
                        r = await cli.post(url, json=json_body)

                body = _safe_json(r)
                if r.status_code in (200, 201):
                    if isinstance(body, dict) and body.get("error"):
                        _breaker_fail(tag="")
                        raise RuntimeError(f"jup_error_in_200:{_short(body, 220)}")
                    _breaker_success()
                    return body

                if _is_auth_fail(r.status_code, body):
                    _STATS["auth_fail"] += 1
                    _breaker_fail(tag="auth")
                    raise httpx.HTTPStatusError("auth", request=r.request, response=r)

                if _route_not_found(body):
                    _breaker_fail(tag="no_route")
                    raise RuntimeError("jup_no_route")

                if _is_token_not_tradable(body):
                    _breaker_fail(tag="token_not_tradable")
                    raise RuntimeError("jup_token_not_tradable")

                if _is_transient_status(r.status_code) and attempt < int(c.retry_max):
                    _warn(
                        "JUP_HTTP_RETRY",
                        url=url[:140],
                        status=r.status_code,
                        attempt=attempt,
                        body=_short(body, 240),
                    )
                    await _sleep_backoff(attempt)
                    continue

                _breaker_fail(tag="")
                raise httpx.HTTPStatusError("http", request=r.request, response=r)

            except (httpx.TimeoutException, httpx.NetworkError) as e:
                if attempt < int(c.retry_max):
                    _warn("JUP_NET_RETRY", url=url[:140], attempt=attempt, err=_short(e, 220))
                    await _sleep_backoff(attempt)
                    continue
                _breaker_fail(tag="")
                raise

            except Exception as e:
                msg = str(e)
                if (
                    "'h2' package is not installed" in msg or "http2=True" in msg
                ) and attempt < int(c.retry_max):
                    log.every("JUP_HTTP2_DISABLED_RUNTIME", n=120, msg="JUP_HTTP2_DISABLED_RUNTIME")
                    await close_http()
                    await _sleep_backoff(attempt)
                    continue
                raise

    finally:
        dt_ms = int((time.monotonic() - t0) * 1000)
        if dt_ms > 2500:
            log.every(
                "JUP_SLOW_HTTP", n=20, msg="JUP_SLOW_HTTP", method=method, url=url[:120], ms=dt_ms
            )
        if budget_granted:
            with contextlib.suppress(Exception):
                await _budget_after(is_get=is_get, units=int(budget_units))


# ──────────────────────────────────────────────────────────────────────────────
# Quote params
# ──────────────────────────────────────────────────────────────────────────────
def _quote_params(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: bool | None = None,
) -> dict[str, Any]:
    c = _cfg()
    od = c.only_direct if only_direct is None else bool(only_direct)
    return {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount)),
        "slippageBps": int(slippage_bps),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": od,
        "asLegacyTransaction": bool(c.as_legacy_tx),
        "restrictIntermediateTokens": bool(c.restrict_intermediate),
    }


def _compact_route_summary(q: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
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


# ──────────────────────────────────────────────────────────────────────────────
# Quote core (multi-url + requote bump + unroutable)
# ──────────────────────────────────────────────────────────────────────────────
async def _get_quote_kwonly(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    only_direct: bool | None = None,
) -> tuple[str, dict[str, Any]]:
    c = _cfg()
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

    urls = _prefer_order(c.quote_urls, _LAST_GOOD_QUOTE_URL)
    if not urls:
        raise RuntimeError("jup_all_endpoints_cooldown")

    last_err: BaseException | None = None
    bumped_done = False
    saw_no_route = False

    for url in urls:
        try:
            js = await _http_json("GET", url, params=params, budget_units=1)
            if not isinstance(js, dict):
                raise RuntimeError("jup_quote_non_dict")
            _STATS["quote_ok"] += 1
            _endpoint_note_ok(url)
            globals()["_LAST_GOOD_QUOTE_URL"] = url
            log.every("JUP_QUOTE_OK", n=40, msg="JUP_QUOTE_OK", url=url[:80], inp=inp, out=out)
            return url, js

        except RuntimeError as e:
            last_err = e
            tag = str(e)

            if tag == "jup_no_route":
                saw_no_route = True
                if not bumped_done:
                    bumped_done = True
                    bumped = max(
                        int(int(amount) * (100 + int(c.requote_bump_pct)) / 100), int(amount) + 1
                    )
                    log.every(
                        "JUP_REQUOTE_BUMP",
                        n=25,
                        msg="JUP_REQUOTE_BUMP",
                        url=url[:80],
                        amt=int(amount),
                        bumped=bumped,
                    )
                    try:
                        params2 = dict(params)
                        params2["amount"] = str(int(bumped))
                        js2 = await _http_json("GET", url, params=params2, budget_units=1)
                        if isinstance(js2, dict):
                            _STATS["quote_ok"] += 1
                            _endpoint_note_ok(url)
                            globals()["_LAST_GOOD_QUOTE_URL"] = url
                            log.every(
                                "JUP_QUOTE_OK_BUMPED", n=25, msg="JUP_QUOTE_OK_BUMPED", url=url[:80]
                            )
                            return url, js2
                    except Exception:
                        pass

                _STATS["quote_fail"] += 1
                _endpoint_note_fail(url)
                continue

            if tag == "jup_token_not_tradable":
                _STATS["token_not_tradable"] += 1
                _endpoint_note_fail(url)
                raise

            _STATS["quote_fail"] += 1
            _endpoint_note_fail(url)
            continue

        except httpx.HTTPStatusError as e:
            last_err = e
            resp = e.response
            body = None
            with contextlib.suppress(Exception):
                body = resp.json()

            if _is_auth_fail(resp.status_code, body):
                _endpoint_note_fail(url)
                _warn(
                    "JUP_AUTH_FAIL",
                    url=url[:140],
                    status=int(resp.status_code),
                    has_api_key=bool(_cfg().has_api_key),
                    api_key_mask=_cfg().api_key_mask,
                    hint="Auth failed (401/403). If this is Metis /swap/v1, ensure JUP_API_KEY is valid.",
                    body=_short(body, 240),
                )
                if _cfg().strict_auth:
                    raise RuntimeError("jup_unauthorized") from e
                continue

            if _is_transient_status(resp.status_code):
                _endpoint_note_fail(url)
                continue

            _endpoint_note_fail(url)
            continue

        except BaseException as e:
            last_err = e
            _STATS["quote_fail"] += 1
            _endpoint_note_fail(url)
            _warn("JUP_QUOTE_UNKNOWN_FAIL", url=url[:140], err=_short(e, 220))
            continue

    if saw_no_route:
        await _mark_unroutable(inp, out)
        _STATS["no_route"] += 1
        raise RuntimeError("jup_no_route")

    raise last_err or RuntimeError("jup_quote_failed")


async def _get_quote(*args: Any, **kwargs: Any) -> tuple[str, dict[str, Any]]:
    if args and not kwargs and len(args) == 4:
        return await _get_quote_kwonly(
            input_mint=str(args[0]),
            output_mint=str(args[1]),
            amount=int(args[2]),
            slippage_bps=int(args[3]),
        )
    return await _get_quote_kwonly(**kwargs)


# ──────────────────────────────────────────────────────────────────────────────
# Public quote functions (router expects these)
# ──────────────────────────────────────────────────────────────────────────────
async def get_quote(
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int = 0,
    only_direct: bool | None = None,
) -> dict[str, Any]:
    c = _cfg()
    sbps = _clamp_i(int(slippage_bps or c.default_slippage_bps), 1, 5000)
    _, q = await _get_quote(
        input_mint=str(input_mint),
        output_mint=str(output_mint),
        amount=int(amount),
        slippage_bps=sbps,
        only_direct=only_direct,
    )
    return q


quote = get_quote


async def get_buy_quote(
    *,
    mint: str,
    lamports_in: int | None = None,
    amount_sol: float | None = None,
    slippage_bps: int = 0,
    only_direct: bool | None = None,
    **_kw: Any,
) -> dict[str, Any]:
    c = _cfg()
    sbps = _clamp_i(int(slippage_bps or c.default_slippage_bps), 1, 5000)
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
    slippage_bps: int = 0,
    only_direct: bool | None = None,
    **_kw: Any,
) -> dict[str, Any]:
    c = _cfg()
    sbps = _clamp_i(int(slippage_bps or c.default_slippage_bps), 1, 5000)
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


# ──────────────────────────────────────────────────────────────────────────────
# Swap build + send
# ──────────────────────────────────────────────────────────────────────────────
_KP_LOCK = asyncio.Lock()
_KP = None
_PUBKEY_STR: str | None = None


async def _to_thread(fn, *a, **kw):
    try:
        return await asyncio.to_thread(fn, *a, **kw)
    except AttributeError:  # pragma: no cover
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(*a, **kw))


async def _get_keypair_cached():
    global _KP, _PUBKEY_STR
    if _KP is not None and _PUBKEY_STR:
        return _KP, _PUBKEY_STR
    if load_keypair is None or get_pubkey_str is None:
        _STATS["wallet_missing"] += 1
        raise RuntimeError("wallet_layer_unavailable")
    async with _KP_LOCK:
        if _KP is not None and _PUBKEY_STR:
            return _KP, _PUBKEY_STR
        kp = await _to_thread(load_keypair)
        pk = await _to_thread(get_pubkey_str, kp)
        _KP = kp
        _PUBKEY_STR = str(pk)
        return _KP, _PUBKEY_STR


def _prequote_matches(quote_obj: dict[str, Any], inp: str, out: str, amount: int) -> bool:
    if not isinstance(quote_obj, dict) or not quote_obj:
        return False
    q_in = str(quote_obj.get("inputMint") or quote_obj.get("inputMintAddress") or "").strip()
    q_out = str(quote_obj.get("outputMint") or quote_obj.get("outputMintAddress") or "").strip()
    if q_in and not _same_mint(q_in, inp):
        return False
    if q_out and not _same_mint(q_out, out):
        return False
    q_amt = quote_obj.get("inAmount")
    if q_amt is not None:
        with contextlib.suppress(Exception):
            if int(str(q_amt)) != int(amount):
                return False
    return True


def _extract_prequote(*qs: dict[str, Any] | None) -> dict[str, Any] | None:
    for q in qs:
        if isinstance(q, dict) and q:
            return q
    return None


async def _build_swap_tx(*, swap_url: str, user_pubkey: str, quote_obj: dict[str, Any]) -> str:
    c = _cfg()
    dynamic_cu = not (c.compute_unit_limit and c.compute_unit_limit > 0)

    body: dict[str, Any] = {
        "quoteResponse": quote_obj,  # DO NOT MODIFY
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": bool(c.wrap_unwrap_sol),
        "dynamicComputeUnitLimit": bool(dynamic_cu),
        "asLegacyTransaction": bool(c.as_legacy_tx),
        "useSharedAccounts": bool(c.use_shared_accounts),
        "skipUserAccountsRpcCalls": bool(c.skip_user_accounts_rpc_calls),
        "dynamicSlippage": bool(c.dynamic_slippage),
    }
    if c.use_token_ledger:
        body["useTokenLedger"] = True
    if c.payer:
        body["payer"] = c.payer
    if c.platform_fee_bps > 0 and c.fee_account:
        body["platformFeeBps"] = int(c.platform_fee_bps)
        body["feeAccount"] = c.fee_account
    if c.tracking_account:
        body["trackingAccount"] = c.tracking_account
    if c.compute_unit_price_micro > 0:
        body["computeUnitPriceMicroLamports"] = int(c.compute_unit_price_micro)
    if not dynamic_cu and c.compute_unit_limit > 0:
        body["computeUnitLimit"] = int(c.compute_unit_limit)
    if c.blockhash_slots_to_expiry > 0:
        body["blockhashSlotsToExpiry"] = int(c.blockhash_slots_to_expiry)

    js = await _http_json("POST", swap_url, json_body=body, budget_units=2)
    if not isinstance(js, dict):
        _STATS["swap_build_fail"] += 1
        raise RuntimeError("jup_swap_non_dict")
    tx_b64 = js.get("swapTransaction")
    if not tx_b64:
        _STATS["swap_build_fail"] += 1
        raise RuntimeError(f"jup_swap_missing_tx:{_short(js, 360)}")

    _STATS["swap_build_ok"] += 1
    _endpoint_note_ok(swap_url)
    globals()["_LAST_GOOD_SWAP_URL"] = swap_url
    log.every(
        "JUP_SWAP_BUILT", n=30, msg="JUP_SWAP_BUILT", url=swap_url[:80], legacy=int(c.as_legacy_tx)
    )
    return str(tx_b64)


async def _send_tx_b64(tx_b64: str) -> str:
    c = _cfg()
    if sign_and_send_b64_tx is None:
        _STATS["wallet_missing"] += 1
        raise RuntimeError("wallet_layer_unavailable")

    # optional enhanced sender
    try:
        if _send_b64_with_diag is not None:
            with contextlib.suppress(Exception):
                sig = _send_b64_with_diag(tx_b64, skip_preflight=bool(c.skip_preflight))  # type: ignore[misc]
                if inspect.isawaitable(sig):
                    sig = await sig
                if sig:
                    _STATS["send_ok"] += 1
                    return str(sig)
    except Exception:
        pass

    kp, _ = await _get_keypair_cached()
    try:
        sig2 = sign_and_send_b64_tx(tx_b64, kp, skip_preflight=bool(c.skip_preflight))
        if inspect.isawaitable(sig2):
            sig2 = await sig2
    except TypeError:
        sig2 = sign_and_send_b64_tx(tx_b64, kp)  # type: ignore[misc]
        if inspect.isawaitable(sig2):
            sig2 = await sig2
    _STATS["send_ok"] += 1
    return str(sig2)


def _expected_out_sol(quote_obj: dict[str, Any] | None) -> tuple[int | None, float | None]:
    if not isinstance(quote_obj, dict):
        return None, None
    out_raw = _intish(quote_obj.get("outAmount"), 0)
    if out_raw <= 0:
        return None, None
    return int(out_raw), float(out_raw) / float(LAMPORTS_PER_SOL)


def _mk_receipt(
    *,
    ok: bool,
    side: str,
    quote_url: str,
    swap_url: str,
    input_mint: str,
    output_mint: str,
    slippage_bps: int,
    quote_obj: dict[str, Any] | None = None,
    sig: str | None = None,
    err: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    q = quote_obj or {}
    in_raw = _intish(q.get("inAmount"), 0) if isinstance(q, dict) else 0
    out_raw = _intish(q.get("outAmount"), 0) if isinstance(q, dict) else 0
    receipt: dict[str, Any] = {
        "ok": bool(ok),
        "side": str(side).upper(),
        "sig": sig,
        "signature": sig,
        "tx_sig": sig,
        "quote_url": str(quote_url),
        "swap_url": str(swap_url),
        "inputMint": str(input_mint),
        "outputMint": str(output_mint),
        "in_amount_raw": int(in_raw),
        "out_amount_raw": int(out_raw),
        "priceImpactPct": q.get("priceImpactPct") if isinstance(q, dict) else None,
        "slippage_bps": int(slippage_bps),
        "routeSummary": _compact_route_summary(q) if isinstance(q, dict) else {},
        "err": err,
        "ts": float(time.time()),
        "lightning_version": LIGHTNING_VERSION,
    }
    if meta is not None:
        receipt["meta"] = meta
    # keep full quote optional (default on, but safe)
    if _env_bool("LIGHTNING_INCLUDE_FULL_QUOTE", True):
        receipt["quote"] = q
    return receipt


async def _try_build_from_prequote_anywhere(
    *, user_pk: str, quote_obj: dict[str, Any]
) -> tuple[str, str]:
    c = _cfg()
    swap_urls = _prefer_order(c.swap_urls, _LAST_GOOD_SWAP_URL)
    if not swap_urls:
        raise RuntimeError("jup_all_endpoints_cooldown")
    last_err: BaseException | None = None
    for su in swap_urls:
        try:
            tx_b64 = await _build_swap_tx(swap_url=su, user_pubkey=user_pk, quote_obj=quote_obj)
            return su, tx_b64
        except BaseException as e:
            last_err = e
            _endpoint_note_fail(su)
            continue
    raise last_err or RuntimeError("swap_build_failed_all")


# ──────────────────────────────────────────────────────────────────────────────
# Public swap API (router expects these)
# ──────────────────────────────────────────────────────────────────────────────
async def buy_via_lightning(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: int = 0,
    return_meta: bool = False,
    return_receipt: bool = False,
    prequote: dict[str, Any] | None = None,
    quote: dict[str, Any] | None = None,
    prefetched_quote: dict[str, Any] | None = None,
    quoteResponse: dict[str, Any] | None = None,
    **_ignored: Any,
) -> str | None | tuple[str | None, dict[str, Any]] | dict[str, Any]:
    c = _cfg()
    sbps = _clamp_i(int(slippage_bps or c.default_slippage_bps), 1, 5000)
    lamports_in = max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))
    m = _normalize_and_validate_mint(mint, label="mint")

    # safety
    if _same_mint(m, SOL_MINT):
        _STATS["noop_same_mint"] += 1
        rec = _mk_receipt(
            ok=True,
            side="BUY",
            quote_url=c.quote_urls[0],
            swap_url=c.swap_urls[0],
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote_obj={
                "inputMint": SOL_MINT,
                "outputMint": m,
                "inAmount": str(lamports_in),
                "outAmount": str(lamports_in),
                "priceImpactPct": 0.0,
                "routePlan": [],
            },
            sig=None,
            err="noop_same_mint",
            meta={"mint": m, "in_amount_raw": lamports_in, "amount_raw": lamports_in},
        )
        return rec if return_receipt else ((None, rec.get("meta", {})) if return_meta else None)

    if c.kill_switch or c.disable_buys:
        rec = _mk_receipt(
            ok=False,
            side="BUY",
            quote_url=c.quote_urls[0],
            swap_url=c.swap_urls[0],
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote_obj=None,
            sig=None,
            err="buys_disabled",
        )
        return rec if return_receipt else ((None, {}) if return_meta else None)

    # get or accept quote
    supplied = _extract_prequote(quoteResponse, prequote, quote, prefetched_quote)

    quote_url_used = c.quote_urls[0]
    swap_url_used = c.swap_urls[0]
    q: dict[str, Any] = {}
    meta: dict[str, Any] = {"mint": m, "in_amount_raw": lamports_in}

    try:
        if supplied is not None and not _prequote_matches(supplied, SOL_MINT, m, lamports_in):
            supplied = None

        if supplied is not None:
            q = supplied
        else:
            quote_url_used, q = await _get_quote(
                input_mint=SOL_MINT, output_mint=m, amount=lamports_in, slippage_bps=sbps
            )

        meta["quote_url"] = quote_url_used
        meta["outAmount"] = _intish(q.get("outAmount"), 0)

        # DRYRUN semantics
        if c.dry_run and not c.dryrun_build_tx:
            rec = _mk_receipt(
                ok=True,
                side="BUY",
                quote_url=quote_url_used,
                swap_url=swap_url_used,
                input_mint=SOL_MINT,
                output_mint=m,
                slippage_bps=sbps,
                quote_obj=q,
                sig=None,
                err=None,
                meta=meta,
            )
            return rec if return_receipt else ((None, meta) if return_meta else None)

        _, user_pk = await _get_keypair_cached()

        if supplied is not None:
            try:
                swap_url_used, tx_b64 = await _try_build_from_prequote_anywhere(
                    user_pk=user_pk, quote_obj=q
                )
            except Exception as e:
                _STATS["prequote_build_fail"] += 1
                _warn("PREQUOTE_BUILD_FAIL", err=_short(e, 220))
                _STATS["prequote_requote"] += 1
                quote_url_used, q = await _get_quote(
                    input_mint=SOL_MINT, output_mint=m, amount=lamports_in, slippage_bps=sbps
                )
                swap_url_used, tx_b64 = await _try_build_from_prequote_anywhere(
                    user_pk=user_pk, quote_obj=q
                )
        else:
            swap_urls = _prefer_order(c.swap_urls, _LAST_GOOD_SWAP_URL)
            if not swap_urls:
                raise RuntimeError("jup_all_endpoints_cooldown")
            last_build_err: BaseException | None = None
            for su in swap_urls:
                try:
                    tx_b64 = await _build_swap_tx(swap_url=su, user_pubkey=user_pk, quote_obj=q)
                    swap_url_used = su
                    break
                except BaseException as e:
                    last_build_err = e
                    _endpoint_note_fail(su)
                    continue
            else:
                raise last_build_err or RuntimeError("swap_build_failed_all")

        meta["swap_url"] = swap_url_used

        if c.dry_run:
            rec = _mk_receipt(
                ok=True,
                side="BUY",
                quote_url=quote_url_used,
                swap_url=swap_url_used,
                input_mint=SOL_MINT,
                output_mint=m,
                slippage_bps=sbps,
                quote_obj=q,
                sig=None,
                err=None,
                meta=meta,
            )
            return rec if return_receipt else ((None, meta) if return_meta else None)

        sig = await _send_tx_b64(tx_b64)
        log.event("ENTRY_TX_OK", sig=sig, mint=m, lamports_in=lamports_in, quote_url=quote_url_used)

        rec = _mk_receipt(
            ok=True,
            side="BUY",
            quote_url=quote_url_used,
            swap_url=swap_url_used,
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote_obj=q,
            sig=sig,
            err=None,
            meta=meta,
        )
        if return_receipt:
            return rec
        if return_meta:
            return sig, meta
        return sig

    except Exception as e:
        _STATS["send_fail"] += 1
        rec = _mk_receipt(
            ok=False,
            side="BUY",
            quote_url=quote_url_used,
            swap_url=swap_url_used,
            input_mint=SOL_MINT,
            output_mint=m,
            slippage_bps=sbps,
            quote_obj=q or None,
            sig=None,
            err=str(e),
            meta=meta or None,
        )
        if return_receipt:
            return rec
        if return_meta:
            return None, (meta or {})
        return None


async def sell_via_lightning(
    *,
    mint: str,
    token_program: str | None = None,
    amount_raw: int,
    slippage_bps: int = 0,
    return_receipt: bool = False,
    prequote: dict[str, Any] | None = None,
    quote: dict[str, Any] | None = None,
    prefetched_quote: dict[str, Any] | None = None,
    quoteResponse: dict[str, Any] | None = None,
    **_ignored: Any,
) -> str | None | dict[str, Any]:
    del token_program

    c = _cfg()
    sbps = _clamp_i(int(slippage_bps or c.default_slippage_bps), 1, 5000)
    amt = int(amount_raw)
    m = _normalize_and_validate_mint(mint, label="mint")

    if c.kill_switch or c.disable_sells or amt <= 0:
        rec = _mk_receipt(
            ok=False,
            side="SELL",
            quote_url=c.quote_urls[0],
            swap_url=c.swap_urls[0],
            input_mint=m,
            output_mint=SOL_MINT,
            slippage_bps=sbps,
            quote_obj=None,
            sig=None,
            err="sells_disabled" if (c.kill_switch or c.disable_sells) else "amount_raw<=0",
        )
        return rec if return_receipt else None

    quote_url_used = c.quote_urls[0]
    swap_url_used = c.swap_urls[0]
    supplied = _extract_prequote(quoteResponse, prequote, quote, prefetched_quote)
    q: dict[str, Any] = {}

    try:
        if supplied is not None and not _prequote_matches(supplied, m, SOL_MINT, amt):
            supplied = None

        if supplied is not None:
            q = supplied
        else:
            quote_url_used, q = await _get_quote(
                input_mint=m, output_mint=SOL_MINT, amount=amt, slippage_bps=sbps
            )

        exp_lamports, exp_sol = _expected_out_sol(q)

        if c.dry_run and not c.dryrun_build_tx:
            rec = _mk_receipt(
                ok=True,
                side="SELL",
                quote_url=quote_url_used,
                swap_url=swap_url_used,
                input_mint=m,
                output_mint=SOL_MINT,
                slippage_bps=sbps,
                quote_obj=q,
                sig=None,
                err=None,
                meta={"expected_out_lamports": exp_lamports, "expected_out_sol": exp_sol},
            )
            return rec if return_receipt else None

        _, user_pk = await _get_keypair_cached()

        if supplied is not None:
            swap_url_used, tx_b64 = await _try_build_from_prequote_anywhere(
                user_pk=user_pk, quote_obj=q
            )
        else:
            swap_urls = _prefer_order(c.swap_urls, _LAST_GOOD_SWAP_URL)
            if not swap_urls:
                raise RuntimeError("jup_all_endpoints_cooldown")
            last_build_err: BaseException | None = None
            for su in swap_urls:
                try:
                    tx_b64 = await _build_swap_tx(swap_url=su, user_pubkey=user_pk, quote_obj=q)
                    swap_url_used = su
                    break
                except BaseException as e:
                    last_build_err = e
                    _endpoint_note_fail(su)
                    continue
            else:
                raise last_build_err or RuntimeError("swap_build_failed_all")

        if c.dry_run:
            rec = _mk_receipt(
                ok=True,
                side="SELL",
                quote_url=quote_url_used,
                swap_url=swap_url_used,
                input_mint=m,
                output_mint=SOL_MINT,
                slippage_bps=sbps,
                quote_obj=q,
                sig=None,
                err=None,
                meta={"expected_out_lamports": exp_lamports, "expected_out_sol": exp_sol},
            )
            return rec if return_receipt else None

        sig = await _send_tx_b64(tx_b64)
        log.event(
            "EXIT_TX_OK",
            sig=sig,
            mint=m,
            amount_raw=amt,
            quote_url=quote_url_used,
            expected_out_sol=exp_sol,
        )

        rec = _mk_receipt(
            ok=True,
            side="SELL",
            quote_url=quote_url_used,
            swap_url=swap_url_used,
            input_mint=m,
            output_mint=SOL_MINT,
            slippage_bps=sbps,
            quote_obj=q,
            sig=sig,
            err=None,
            meta={"expected_out_lamports": exp_lamports, "expected_out_sol": exp_sol},
        )
        return rec if return_receipt else sig

    except Exception as e:
        _STATS["send_fail"] += 1
        rec = _mk_receipt(
            ok=False,
            side="SELL",
            quote_url=quote_url_used,
            swap_url=swap_url_used,
            input_mint=m,
            output_mint=SOL_MINT,
            slippage_bps=sbps,
            quote_obj=q or None,
            sig=None,
            err=str(e),
            meta={"expected_out_lamports": None, "expected_out_sol": None},
        )
        return rec if return_receipt else None


# ──────────────────────────────────────────────────────────────────────────────
# Debug + stats
# ──────────────────────────────────────────────────────────────────────────────
async def debug_get_quote_sol_to_usdc(
    amount_sol: float = 0.002, slippage_bps: int = 0
) -> dict[str, Any]:
    c = _cfg()
    lamports = max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))
    return await get_quote(
        SOL_MINT, USDC_MINT, lamports, int(slippage_bps or c.default_slippage_bps)
    )


async def debug_build_only_sol_to_usdc(
    amount_sol: float = 0.002, slippage_bps: int = 0
) -> tuple[int, int, str]:
    c = _cfg()
    lamports = max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))
    quote_url, q = await _get_quote(
        input_mint=SOL_MINT,
        output_mint=USDC_MINT,
        amount=lamports,
        slippage_bps=_clamp_i(int(slippage_bps or c.default_slippage_bps), 1, 5000),
    )
    _, user_pk = await _get_keypair_cached()
    swap_url = _prefer_order(c.swap_urls, _LAST_GOOD_SWAP_URL)[0]
    tx_b64 = await _build_swap_tx(swap_url=swap_url, user_pubkey=user_pk, quote_obj=q)
    return _intish(q.get("inAmount"), lamports), len(tx_b64), quote_url


def stats() -> dict[str, Any]:
    c = _cfg()
    return {
        "lightning_version": LIGHTNING_VERSION,
        "quote_urls": list(c.quote_urls),
        "swap_urls": list(c.swap_urls),
        "last_good_quote_url": _LAST_GOOD_QUOTE_URL,
        "last_good_swap_url": _LAST_GOOD_SWAP_URL,
        "breaker_until": float(_BREAKER_UNTIL),
        "endpoint_fails": dict(_ENDPOINT_FAILS),
        "endpoint_until": dict(_ENDPOINT_UNTIL),
        "endpoint_score": dict(_ENDPOINT_SCORE),
        "unroutable_pairs": len(_UNROUTABLE_TTL),
        "http2_requested": bool(_env_bool("JUP_HTTP2", False)),
        "http2_effective": bool(c.http2_effective),
        "tls_verify": bool(c.tls_verify),
        "has_api_key": bool(c.has_api_key),
        "api_key_mask": c.api_key_mask,
        "cfg": {
            "only_direct": bool(c.only_direct),
            "restrict_intermediate": bool(c.restrict_intermediate),
            "default_slippage_bps": int(c.default_slippage_bps),
            "as_legacy_tx": bool(c.as_legacy_tx),
            "max_qps": int(c.max_qps),
            "max_concurrency": int(c.max_concurrency),
            "retry_max": int(c.retry_max),
            "endpoint_fail_limit": int(c.endpoint_fail_limit),
            "endpoint_cooldown_sec": float(c.endpoint_cooldown_sec),
            "strict_auth": bool(c.strict_auth),
            "dry_run": bool(c.dry_run),
            "dryrun_build_tx": bool(c.dryrun_build_tx),
        },
        "stats": dict(_STATS),
    }


def _variant_flags() -> dict[str, Any]:
    c = _cfg()
    return {
        "lightning_version": LIGHTNING_VERSION,
        "quote_urls": list(c.quote_urls),
        "swap_urls": list(c.swap_urls),
        "only_direct": bool(c.only_direct),
        "restrict_intermediate_tokens": bool(c.restrict_intermediate),
        "as_legacy_tx": bool(c.as_legacy_tx),
        "wrap_unwrap_sol": bool(c.wrap_unwrap_sol),
        "dynamic_slippage": bool(c.dynamic_slippage),
        "skip_user_accounts_rpc_calls": bool(c.skip_user_accounts_rpc_calls),
        "use_shared_accounts": bool(c.use_shared_accounts),
        "use_token_ledger": bool(c.use_token_ledger),
        "compute_unit_limit": int(c.compute_unit_limit),
        "compute_unit_price_micro": int(c.compute_unit_price_micro),
        "platform_fee_bps": int(c.platform_fee_bps),
        "fee_account_set": bool(c.fee_account),
        "tracking_account_set": bool(c.tracking_account),
        "payer_set": bool(c.payer),
        "timeout_sec": float(c.timeout_sec),
        "qps": int(c.max_qps),
        "max_concurrency": int(c.max_concurrency),
        "unroutable_ttl_sec": int(c.unroutable_ttl_sec),
        "requote_bump_pct": int(c.requote_bump_pct),
        "quote_ttl_sec": float(c.quote_ttl_sec),
        "strict_auth": bool(c.strict_auth),
        "has_api_key": bool(c.has_api_key),
        "api_key_mask": c.api_key_mask,
        "stats": dict(_STATS),
    }


__all__ = [
    "_jup_headers",
    "_variant_flags",
    "buy_via_lightning",
    "close_http",
    "debug_build_only_sol_to_usdc",
    "debug_get_quote_sol_to_usdc",
    "get_buy_quote",
    "get_quote",
    "get_sell_quote",
    "quote",
    "sell_via_lightning",
    "stats",
]
