from __future__ import annotations

"""
utils_pkg/router.py — Clean, end‑to‑end rewrite

Key features
- Jupiter v6-first (public/pro relays), with optional legacy (/swap/v1/*) toggle
- No mint auto-trimming/“fixing”: strict 32‑byte base58 validation only
- Robust quote/swap with multiple upstream bases, basic rate limiting, and circuit breaker
- On-chain decimals probe (RPC getAccountInfo) with optional /tokens fallback cache
- Price helper (token→SOL) with short TTL, blacklisting of obviously bad IDs
- Buy/Sell helpers with Telegram notifications (optional) and reserve guard
- Dynamic priority fee tuner (observes confirmation to adjust target tip)
- Clean public API and compatibility shims for your existing imports

Environment knobs (most notable)
- JUP_UPSTREAM, JUP_ALT_BASE_URLS, JUP_API_KEY, JUP_AS_LEGACY, JUP_ONLY_DIRECT
- PF_SLIPPAGE_BPS, PF_TRADE_SOL, PF_PRIORITY_SOL, PF_PRIORITY_DYNAMIC
- MIN_SOL_BALANCE, DRY_RUN
- SOL_RPC_URL / RPC_URL
- ROUTER_* timeouts and retries

This file is intentionally self‑contained and conservative: no background tasks, no
nonlocal state mutations beyond caches and circuit breaker.
"""

import asyncio
import base64
import json
import os
import random
import re
import time
from typing import Any, Dict, Optional, Tuple, Callable, List

import requests
from requests.exceptions import RequestException

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
try:
    from logging_setup import get_logger
    log = get_logger()
except Exception:  # pragma: no cover
    class _L:
        def event(self, *a, **k): pass
        def every(self, *a, **k): pass
        def info(self, *a, **k): pass
    log = _L()  # type: ignore

if not hasattr(log, "every"):
    def _every(_key: str, n: int = 10, msg: str | None = None, **kw):
        return log.event(msg or _key, **kw)
    setattr(log, "every", _every)  # type: ignore[attr-defined]

# ──────────────────────────────────────────────────────────────────────────────
# Wallet / utils imports
# ──────────────────────────────────────────────────────────────────────────────
from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,   # fallback sender
    _send_b64_with_diag,    # robust sender w/ simulate diagnostics
    get_balance_lamports,   # sync balance helper
)

try:
    # External normalization helper for callers (router itself is strict)
    from utils_pkg.mints import normalize_mint as _external_normalize_mint  # type: ignore
except Exception:
    _external_normalize_mint = None  # type: ignore

# Optional Telegram helper
try:
    from utils_pkg.core_utils import send_telegram as _core_send_telegram  # type: ignore
except Exception:  # pragma: no cover
    async def _core_send_telegram(_msg: str, **_kw) -> None:  # type: ignore
        return

# Optional budget hooks
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover
    class _BudgetNoop:
        def before_quote(self, units: int = 1) -> bool: return True
        def after_quote(self, units: int = 1) -> None:  return None
        def before_rpc(self, method: str, *, weight: Optional[int] = None) -> bool: return True
        def after_rpc(self, method: str, *, weight: Optional[int] = None) -> None:  return None
    budget = _BudgetNoop()  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
def _envb(k: str, default: bool = False) -> bool:
    v = (os.getenv(k, "") or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on"}

def _envf(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, str(d)))
    except Exception:
        return float(d)

def _envi(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, str(d)))
    except Exception:
        return int(d)

# ──────────────────────────────────────────────────────────────────────────────
# Chain / RPC config
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg.txlearn import RPC_URL as _TX_RPC_URL, TX_COMMITMENT as _TX_COMMITMENT  # type: ignore
except Exception:  # pragma: no cover
    _TX_RPC_URL = os.getenv("SOL_RPC_URL") or os.getenv("RPC_URL") or "https://api.mainnet-beta.solana.com"
    _TX_COMMITMENT = (os.getenv("CONFIRM_COMMITMENT") or "confirmed").strip().lower()

TX_RPC_URL = _TX_RPC_URL
TX_COMMITMENT = _TX_COMMITMENT

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000

# ──────────────────────────────────────────────────────────────────────────────
# Toggles / timeouts / retries
# ──────────────────────────────────────────────────────────────────────────────
DRY_RUN = _envb("DRYRUN", False) or _envb("DRY_RUN", False) or _envb("IS_DRYRUN", False)
MIN_SOL_BALANCE = _envf("MIN_SOL_BALANCE", 3.0)

HTTP_TIMEOUT_GET  = max(5.0, _envf("ROUTER_HTTP_TIMEOUT_GET_SEC", 20.0))
HTTP_TIMEOUT_POST = max(5.0, _envf("ROUTER_HTTP_TIMEOUT_POST_SEC", 25.0))
RPC_TIMEOUT_SEC   = max(3.0, _envf("ROUTER_RPC_TIMEOUT_SEC", 10.0))

QUOTE_RETRIES = max(1, _envi("ROUTER_QUOTE_RETRIES", 6))
SWAP_RETRIES  = max(1, _envi("ROUTER_SWAP_RETRIES", 9))
RETRY_BACKOFF = max(0.05, _envf("ROUTER_RETRY_BACKOFF_SEC", 1.2))

JUP_MAX_QPS       = max(1, _envi("JUP_MAX_QPS", 2))
_JUP_MIN_INTERVAL = 1.0 / float(JUP_MAX_QPS)
_JUP_RATE_LOCK    = asyncio.Lock()
_JUP_LAST_TS: float = 0.0

QUOTE_TTL_SEC  = max(0.0, _envf("ROUTER_QUOTE_TTL_SEC", 8.0))
PRICE_TTL_SEC  = max(0.0, _envf("PF_PRICE_TTL_SEC", 6.0))
TOKENS_TTL_SEC = max(30.0, _envf("ROUTER_TOKENS_TTL_SEC", 900.0))

_QUOTE_CACHE: Dict[str, Tuple[float, dict]] = {}
_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}
_TOKENS_CACHE: Tuple[float, List[dict]] | None = None
_QUOTE_FLIGHT: Dict[str, asyncio.Future[Any]] = {}
_quote_lock = asyncio.Lock()

# Bad ID blacklist for price calls
BAD_ID_TTL_SEC = max(10, _envi("ROUTER_BAD_ID_TTL_SEC", 60))
_BAD_PRICE_BLACKLIST: Dict[str, float] = {}

_UA = {"user-agent": "pnlsniper/router", "accept": "application/json", "content-type": "application/json"}

# ──────────────────────────────────────────────────────────────────────────────
# Jupiter upstream (v6-first)
# ──────────────────────────────────────────────────────────────────────────────
JUP_UPSTREAM: str = (
    os.getenv("JUP_UPSTREAM")
    or os.getenv("JUP_BASE_URL")
    or "https://public.jupiterapi.com"
).rstrip("/")
JUP_ALT_BASE_URLS: List[str] = [u.strip().rstrip("/") for u in (os.getenv("JUP_ALT_BASE_URLS") or "").split(",") if u.strip()]
AS_LEGACY = _envb("JUP_AS_LEGACY", False)
ONLY_DIRECT_DEFAULT = _envb("JUP_ONLY_DIRECT", False)

_JUP_LAST_GOOD_BASE: Optional[str] = None


def _quote_url(base: str) -> str:
    return f"{base.rstrip('/')}/swap/v1/quote" if AS_LEGACY else f"{base.rstrip('/')}/quote"

def _swap_url(base: str) -> str:
    return f"{base.rstrip('/')}/swap/v1/swap" if AS_LEGACY else f"{base.rstrip('/')}/swap"


def _jup_headers() -> Dict[str, str]:
    h = {"accept": "application/json", "content-type": "application/json", "origin": "https://jup.ag", "referer": "https://jup.ag/"}
    key = (os.getenv("JUP_API_KEY") or os.getenv("JUP_PRO_KEY") or "").strip()
    if key:
        h["x-api-key"] = key
    bearer = (os.getenv("JUP_BEARER") or "").strip()
    if bearer:
        h["x-jup-auth"] = f"Bearer {bearer}"
    return h


def _bases() -> List[str]:
    # Order: last good → primary → alternates (deduped)
    out: List[str] = []
    if _JUP_LAST_GOOD_BASE:
        out.append(_JUP_LAST_GOOD_BASE)
    if JUP_UPSTREAM and JUP_UPSTREAM not in out:
        out.append(JUP_UPSTREAM)
    for a in JUP_ALT_BASE_URLS:
        if a and a not in out:
            out.append(a)
    return out

# Circuit breaker
_JUP_FAIL_COUNT = 0
_JUP_FAIL_LIMIT = _envi("JUP_FAIL_LIMIT", 8)
_JUP_BREAKER_COOLDOWN = _envf("JUP_BREAKER_COOLDOWN_SEC", 30.0)
_JUP_BREAKER_UNTIL: float = 0.0


def _jup_breaker_check() -> None:
    if time.time() < _JUP_BREAKER_UNTIL:
        raise RuntimeError("JUP_CIRCUIT_BREAKER active")


def _jup_record_success(base: str) -> None:
    global _JUP_FAIL_COUNT, _JUP_BREAKER_UNTIL, _JUP_LAST_GOOD_BASE
    if _JUP_FAIL_COUNT > 0:
        log.event("JUP_BREAKER_RESET", fails=_JUP_FAIL_COUNT)
    _JUP_FAIL_COUNT = 0
    _JUP_BREAKER_UNTIL = 0
    _JUP_LAST_GOOD_BASE = (base or "").strip().rstrip("/")


def _jup_record_failure() -> None:
    global _JUP_FAIL_COUNT, _JUP_BREAKER_UNTIL
    _JUP_FAIL_COUNT += 1
    if _JUP_FAIL_COUNT >= _JUP_FAIL_LIMIT:
        _JUP_BREAKER_UNTIL = time.time() + _JUP_BREAKER_COOLDOWN
        log.event("JUP_BREAKER_TRIPPED", until=_JUP_BREAKER_UNTIL)

# ──────────────────────────────────────────────────────────────────────────────
# Telegram helpers (optional)
# ──────────────────────────────────────────────────────────────────────────────
_MARKDOWNV2_SPECIALS = r"_*[]()~`>#+-=|{}.!\\"

def _tg_plain(msg: str) -> str:
    return "".join(ch for ch in msg if ch not in _MARKDOWNV2_SPECIALS)


def _tg_ready() -> tuple[bool, str]:
    if not _envb("TELEGRAM_ENABLED", False):
        return False, "TELEGRAM_ENABLED=0"
    tok = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    cid = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not tok:
        return False, "no TELEGRAM_BOT_TOKEN"
    if not cid:
        return False, "no TELEGRAM_CHAT_ID"
    return True, "ok"


async def _tg_send(msg: str, *, src: str) -> None:
    ok, why = _tg_ready()
    if not ok:
        log.every("tg.skip", n=30, msg="TG_SKIP", why=why, src=src)
        return
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": _tg_plain(msg), "disable_web_page_preview": True}
    try:
        def _post():
            return requests.post(url, json=payload, timeout=8)
        resp = await asyncio.to_thread(_post)
        try:
            ok_json = resp.json()
        except Exception:
            ok_json = {}
        if resp.status_code != 200 or not ok_json.get("ok", False):
            log.event("TG_SEND_ERROR", src=src, http=resp.status_code, body=str(ok_json)[:200])
        else:
            log.every("tg.sent", n=60, msg="TG_SENT_ONCE", src=src)
    except Exception as e:
        log.event("TG_SEND_ERROR", src=src, err=str(e))


async def _send_safe(msg: str, *, src: str) -> None:
    try:
        await _tg_send(msg, src=src)
    except Exception:
        log.event("TG_SEND_FAIL", src=src)

# ──────────────────────────────────────────────────────────────────────────────
# Base58 & mint sanitation (STRICT)
# ──────────────────────────────────────────────────────────────────────────────
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_IDX = {c: i for i, c in enumerate(_B58)}


def _b58decode(s: str) -> bytes:
    s = (s or "").strip()
    if not s:
        return b""
    n = 0
    for ch in s:
        i = _IDX.get(ch)
        if i is None:
            raise ValueError("invalid base58 char")
        n = n * 58 + i
    full = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(s) - len(s.lstrip("1"))
    return b"\x00" * pad + full


def _is_valid_pubkey_str(s: str) -> bool:
    try:
        return len(_b58decode(s)) == 32
    except Exception:
        return False


def is_valid_pubkey_str(s: str) -> bool:
    return _is_valid_pubkey_str(s)


def _assert_pubkey(s: str, name: str) -> None:
    if not _is_valid_pubkey_str(s):
        raise ValueError(f"Invalid {name} (must be 32-byte base58): {s}")


def _sanitize_mint(m: str) -> str:
    if m is None:
        raise ValueError("empty mint")
    m = str(m).strip()
    if not m:
        raise ValueError("empty mint")
    if not _is_valid_pubkey_str(m):
        raise ValueError(f"Invalid mint (must be 32-byte base58): {m}")
    return m


def normalize_pump_id_to_mint(raw: str) -> str:
    """Retained for API compatibility: try external normalize_mint, otherwise accept
    already-valid base58 pubkeys as-is. Router itself never mutates a caller's mint.
    """
    if _external_normalize_mint is not None:
        try:
            return _external_normalize_mint(raw)  # type: ignore
        except Exception:
            pass
    raw_s = str(raw).strip()
    if _is_valid_pubkey_str(raw_s):
        return raw_s
    raise ValueError(f"cannot normalize to mint: {raw}")

# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers (Jupiter)
# ──────────────────────────────────────────────────────────────────────────────
async def _jup_gate() -> None:
    global _JUP_LAST_TS
    async with _JUP_RATE_LOCK:
        now = time.time()
        wait_for = _JUP_MIN_INTERVAL - (now - _JUP_LAST_TS)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _JUP_LAST_TS = time.time()


async def _retry(coro_fn: Callable, *args, tries: int, backoff: float, label: str, **kwargs) -> Any:
    last_err: Optional[Exception] = None
    for i in range(1, tries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            # For hard user errors, don't spin forever
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (400, 404):
                log.event("RETRY_GIVEUP", label=label, err=str(e))
                raise
            if i < tries:
                await asyncio.sleep(backoff * i * (0.75 + random.random() * 0.5))
            else:
                log.event("RETRY_FAIL", label=label, err=str(e))
                raise
    raise last_err or RuntimeError(label + " failed")


async def _http_get(kind: str, *, params: dict, timeout: float) -> dict:
    _jup_breaker_check()
    last_err: Optional[Exception] = None
    for base in _bases():
        try:
            url = _quote_url(base) if kind == "quote" else f"{base.rstrip('/')}/{kind}"
            r = requests.get(url, params=params, timeout=timeout, headers=_jup_headers())
            try:
                js = r.json()
            except Exception:
                r.raise_for_status()
                raise
            if r.status_code in (200, 201):
                _jup_record_success(base)
                return js
            last_err = RuntimeError(f"{url} -> HTTP {r.status_code}: {str(js)[:180]}")
        except RequestException as e:
            last_err = e
        except Exception as e:
            last_err = e
        await asyncio.sleep(0.1)
    _jup_record_failure()
    raise last_err or RuntimeError(f"all {kind} endpoints failed")


async def _http_post_swap(*, json_body: Optional[dict], params: Optional[dict], timeout: float) -> dict:
    _jup_breaker_check()
    last_err: Optional[Exception] = None
    for base in _bases():
        try:
            url = _swap_url(base)
            r = requests.post(url, json=json_body, params=params, timeout=timeout, headers=_jup_headers())
            try:
                js = r.json()
            except Exception:
                r.raise_for_status()
                raise
            if r.status_code in (200, 201):
                _jup_record_success(base)
                return js
            last_err = RuntimeError(f"{url} -> HTTP {r.status_code}: {str(js)[:180]}")
        except RequestException as e:
            last_err = e
        except Exception as e:
            last_err = e
        await asyncio.sleep(0.12)
    _jup_record_failure()
    raise last_err or RuntimeError("all swap endpoints failed")

# ──────────────────────────────────────────────────────────────────────────────
# Tokens catalog + decimals
# ──────────────────────────────────────────────────────────────────────────────
_TOK_META: Dict[str, Dict[str, Any]] = {}
_KNOWN_DECIMALS: Dict[str, int] = {
    SOL_MINT: 9,
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": 6,  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": 6,  # USDT
    "DezXAZ8z7PnrnRJjz3wXBoRqiXCa6xjnB7YaB1pPB263": 5,  # BONK (current standard is 5)
}
_DECIMALS: Dict[str, int] = {}


async def _tokens_catalog() -> List[dict]:
    global _TOKENS_CACHE
    now = time.time()
    if _TOKENS_CACHE and _TOKENS_CACHE[0] >= now:
        return _TOKENS_CACHE[1]
    try:
        js = await _http_get("tokens", params={}, timeout=HTTP_TIMEOUT_GET)
        arr: List[dict] = js if isinstance(js, list) else []
        for t in arr:
            try:
                address = (t.get("address") or "").strip()
                if address:
                    _TOK_META[address] = t
            except Exception:
                continue
        _TOKENS_CACHE = (now + TOKENS_TTL_SEC, arr)
        return arr
    except Exception:
        return _TOKENS_CACHE[1] if _TOKENS_CACHE else []


def _rpc_post(method: str, params: list) -> dict:
    if not TX_RPC_URL:
        return {}
    if not budget.before_rpc(method):
        return {}
    try:
        r = requests.post(TX_RPC_URL, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=RPC_TIMEOUT_SEC, headers=_UA)
        r.raise_for_status()
        return r.json()
    finally:
        try:
            budget.after_rpc(method)
        except Exception:
            pass


def _mint_decimals_from_data(b: bytes) -> Optional[int]:
    try:
        if len(b) >= 45:
            return int(b[44])
    except Exception:
        pass
    return None


async def _decimals_from_tokens_api(mint: str) -> Optional[int]:
    arr = await _tokens_catalog()
    if not arr:
        return None
    meta = _TOK_META.get(mint)
    if not meta:
        return None
    try:
        dv = int(meta.get("decimals"))
        return dv if 0 <= dv <= 18 else None
    except Exception:
        return None


async def _get_decimals(mint: str) -> Optional[int]:
    k = _KNOWN_DECIMALS.get(mint)
    if k is not None:
        _DECIMALS[mint] = k
        return k
    d = _DECIMALS.get(mint)
    if d is not None:
        return d

    def _probe() -> Optional[int]:
        try:
            js = _rpc_post("getAccountInfo", [mint, {"encoding": "base64"}])
            val = js.get("result", {}).get("value", {})
            data = val.get("data")
            if isinstance(data, list) and data:
                raw_b64 = data[0]
                buf = base64.b64decode(raw_b64)
                dec = _mint_decimals_from_data(buf)
                if dec is not None and 0 <= dec <= 18:
                    return int(dec)
        except Exception:
            return None
        return None

    dec = await asyncio.to_thread(_probe)
    if dec is None:
        try:
            dec = await _decimals_from_tokens_api(mint)
        except Exception:
            dec = None

    if isinstance(dec, int) and 0 <= dec <= 18:
        _DECIMALS[mint] = int(dec)
        return int(dec)
    return None


async def get_decimals(mint: str) -> Optional[int]:
    m = _sanitize_mint(mint)
    _assert_pubkey(m, "mint")
    return await _get_decimals(m)

# ──────────────────────────────────────────────────────────────────────────────
# Jupiter quote/swap wrappers
# ──────────────────────────────────────────────────────────────────────────────

def _slippage_bps_default() -> int:
    v = os.getenv("PF_SLIPPAGE_BPS")
    if v:
        try:
            x = int(v)
            return max(1, min(5000, x))
        except Exception:
            pass
    s = _envf("JUPITER_MAX_SLIPPAGE", 0.006)
    return max(1, min(5000, int(round(s * 10_000))))


def _qkey(input_mint: str, output_mint: str, amount_iu: int, slippage_bps: int, swap_mode: str, direct: bool, legacy: bool) -> str:
    return f"{input_mint}:{output_mint}:{amount_iu}:{slippage_bps}:{swap_mode}:{int(direct)}:{int(legacy)}"


async def _get_cached_quote(key: str) -> Optional[dict]:
    if QUOTE_TTL_SEC <= 0:
        return None
    rec = _QUOTE_CACHE.get(key)
    if not rec:
        return None
    exp, payload = rec
    if exp >= time.time():
        return payload
    _QUOTE_CACHE.pop(key, None)
    return None


async def _set_cached_quote(key: str, payload: dict, ttl: float) -> None:
    if ttl > 0:
        _QUOTE_CACHE[key] = (time.time() + ttl, payload)


async def _singleflight_quote(key: str, maker: Callable[[], Any]) -> dict:
    async with _quote_lock:
        fut = _QUOTE_FLIGHT.get(key)
        if fut is None:
            fut = asyncio.ensure_future(maker())
            _QUOTE_FLIGHT[key] = fut
    try:
        return await fut
    finally:
        async with _quote_lock:
            _QUOTE_FLIGHT.pop(key, None)


async def _jup_quote(input_mint: str, output_mint: str, amount_iu: int, *, slippage_bps: Optional[int] = None, swap_mode: str = "ExactIn", only_direct: bool = False) -> dict:
    if amount_iu <= 0:
        raise ValueError("amount_iu must be > 0")
    _assert_pubkey(input_mint, "inputMint")
    _assert_pubkey(output_mint, "outputMint")
    sbps = int(slippage_bps if slippage_bps is not None else _slippage_bps_default())
    legacy = bool(AS_LEGACY)
    key = _qkey(input_mint, output_mint, amount_iu, sbps, swap_mode, only_direct, legacy)

    cached = await _get_cached_quote(key)
    if cached is not None:
        return cached

    async def _fetch():
        if not budget.before_quote():
            raise RuntimeError("quote blocked by budget")
        try:
            await _jup_gate()
            params = {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(amount_iu),
                "slippageBps": sbps,
                "onlyDirectRoutes": "true" if only_direct else "false",
                "asLegacyTransaction": "true" if legacy else "false",
                # swapMode is ignored by modern /quote but fine to include
                "swapMode": swap_mode,
            }
            js = await _http_get("quote", params=params, timeout=HTTP_TIMEOUT_GET)
            await _set_cached_quote(key, js, QUOTE_TTL_SEC)
            return js
        finally:
            try:
                budget.after_quote()
            except Exception:
                pass

    return await _singleflight_quote(key, _fetch)


async def _jup_swap(user_pubkey: str, quote: dict, *, priority_lamports: Optional[int], compute_units: Optional[int]) -> str:
    await _jup_gate()
    body = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": False if (compute_units and compute_units > 0) else True,
        "quoteResponse": quote,
        "asLegacyTransaction": True if AS_LEGACY else False,
    }
    if priority_lamports is not None:
        body["prioritizationFeeLamports"] = int(priority_lamports)
    if compute_units and compute_units > 0:
        body["computeUnitLimit"] = int(compute_units)

    js = await _http_post_swap(json_body=body, params=None, timeout=HTTP_TIMEOUT_POST)
    tx_b64 = js.get("swapTransaction")
    if not tx_b64:
        raise RuntimeError(f"Jupiter returned no swapTransaction: {js}")
    return tx_b64

# ──────────────────────────────────────────────────────────────────────────────
# Public quote/price helpers
# ──────────────────────────────────────────────────────────────────────────────

def _clamp_slippage_bps(v: Optional[int]) -> int:
    try:
        sv = int(v if v is not None else _slippage_bps_default())
    except Exception:
        sv = _slippage_bps_default()
    if sv < 1: sv = 1
    if sv > 5000: sv = 5000
    return sv


async def _safe_decimals(mint: str, fallback: int = 6) -> int:
    try:
        dec = await _get_decimals(mint)
        if isinstance(dec, int) and 0 <= dec <= 18:
            return dec
    except Exception:
        pass
    return fallback


async def get_sell_quote(*, mint: str, amount_raw: int, slippage_bps: Optional[int] = None, only_direct: Optional[bool] = None) -> Dict[str, Any]:
    if not isinstance(amount_raw, int) or amount_raw <= 0:
        raise ValueError("amount_raw must be a positive integer")
    m = _sanitize_mint(mint)
    sbps = _clamp_slippage_bps(slippage_bps)
    direct = only_direct if isinstance(only_direct, bool) else ONLY_DIRECT_DEFAULT
    q = await _retry(_jup_quote, m, SOL_MINT, amount_raw, tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-sell", slippage_bps=sbps, only_direct=direct)
    lamports_out = int(q.get("outAmount") or 0)
    dec = await _safe_decimals(m)
    return {**q, "amount_in_raw": amount_raw, "out_amount_lamports": lamports_out, "out_amount_sol": lamports_out / LAMPORTS_PER_SOL, "decimals": dec}


async def get_buy_quote(*, mint: str, lamports_in: Optional[int] = None, amount_sol: Optional[float] = None, slippage_bps: Optional[int] = None, only_direct: Optional[bool] = None) -> Dict[str, Any]:
    m = _sanitize_mint(mint)
    if lamports_in is None and amount_sol is None:
        amount_sol = _envf("PF_TRADE_SOL", 0.0)
    if lamports_in is None:
        try:
            lamports_in = int(round(float(amount_sol or 0.0) * LAMPORTS_PER_SOL))
        except Exception:
            lamports_in = 0
    if lamports_in <= 0:
        raise ValueError("Provide a positive lamports_in or amount_sol > 0")
    sbps = _clamp_slippage_bps(slippage_bps)
    direct = only_direct if isinstance(only_direct, bool) else ONLY_DIRECT_DEFAULT
    q = await _retry(_jup_quote, SOL_MINT, m, lamports_in, tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-buy", slippage_bps=sbps, only_direct=direct)
    out_raw = int(q.get("outAmount") or 0)
    dec = await _safe_decimals(m)
    out_ui = out_raw / (10 ** dec) if out_raw > 0 else 0.0
    return {**q, "lamports_in": lamports_in, "out_amount_raw": out_raw, "out_amount_ui": out_ui, "decimals": dec}


def _price_probe_amounts(dec: int) -> List[int]:
    dec = max(0, min(int(dec or 0), 18))
    ui_steps = (1.0, 0.1, 0.01, 0.001)
    raw = sorted({int(round(ui * (10 ** dec))) for ui in ui_steps if (ui * (10 ** dec)) >= 1}, reverse=True)
    return raw or [1]


async def get_price_ui(mint: str) -> float:
    # Fast path
    if mint == SOL_MINT:
        return 1.0
    raw = (mint or "").strip()
    if not raw:
        return 0.0
    if _BAD_PRICE_BLACKLIST.get(raw, 0) >= time.time():
        return 0.0
    try:
        m = _sanitize_mint(raw)
    except Exception:
        _BAD_PRICE_BLACKLIST[raw] = time.time() + BAD_ID_TTL_SEC
        log.every("price.bad", n=20, msg="PRICE_BAD_ID", raw=raw)
        return 0.0

    if PRICE_TTL_SEC > 0:
        rec = _PRICE_CACHE.get(m)
        if rec and rec[0] >= time.time():
            try:
                return float(rec[1])
            except Exception:
                pass

    dec = await _safe_decimals(m)
    for amt_raw in _price_probe_amounts(dec):
        try:
            q = await _retry(_jup_quote, m, SOL_MINT, amt_raw, tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-price")
            lamports = int(q.get("outAmount") or 0)
            if lamports <= 0:
                continue
            sol_out = lamports / LAMPORTS_PER_SOL
            tokens_ui = amt_raw / (10 ** dec)
            px = sol_out / tokens_ui if tokens_ui > 0 else 0.0
            if px > 0:
                if PRICE_TTL_SEC > 0:
                    _PRICE_CACHE[m] = (time.time() + PRICE_TTL_SEC, px)
                return px
        except Exception:
            continue
    _BAD_PRICE_BLACKLIST[raw] = time.time() + BAD_ID_TTL_SEC
    return 0.0


# Aliases for backwards compatibility
price_of_ui = get_price_ui
price_of = get_price_ui


async def mint_decimals(mint: str) -> Optional[int]:
    return await get_decimals(mint)

# ──────────────────────────────────────────────────────────────────────────────
# Priority fee tuner + reserve guard + swaps
# ──────────────────────────────────────────────────────────────────────────────

def _to_sol_from_lamports_like(x: float) -> float:
    lamports = float(x)
    # Some providers return microlamports
    if lamports >= 1e9:
        lamports = lamports / 1e6
    return lamports / 1e9


QN_PRIORITY_HINT   = _envb("QN_PRIORITY_HINT", False)
QN_RPC_URL         = (os.getenv("SOL_RPC_URL") or "").strip()
QN_HINT_TIMEOUT_S  = max(0.8, _envf("QN_PRIORITY_TIMEOUT_SEC", 2.5))


def _qn_priority_seed_blocking() -> Optional[float]:
    if not (QN_PRIORITY_HINT and QN_RPC_URL):
        return None
    payloads = [
        {"jsonrpc": "2.0", "id": 1, "method": "qn_getPriorityFee", "params": []},
        {"jsonrpc": "2.0", "id": 1, "method": "getRecentPrioritizationFees", "params": []},
    ]
    for p in payloads:
        try:
            r = requests.post(QN_RPC_URL, json=p, timeout=QN_HINT_TIMEOUT_S, headers=_UA)
            js = r.json()
            res = js.get("result")
            if isinstance(res, dict):
                for k in ("median", "recentMedian", "lamports", "prioritizationFee", "perSignature"):
                    v = res.get(k)
                    if v is not None:
                        return _to_sol_from_lamports_like(float(v))
            elif isinstance(res, list) and res:
                vals = []
                for it in res:
                    if isinstance(it, dict):
                        v = it.get("prioritizationFee") or it.get("lamports") or it.get("median")
                        if v is not None:
                            vals.append(float(v))
                if vals:
                    vals.sort()
                    mid = vals[len(vals) // 2]
                    return _to_sol_from_lamports_like(mid)
        except Exception:
            continue
    return None


class DynPriority:
    def __init__(self) -> None:
        base = _envf("PF_PRIORITY_SOL", 0.0015)
        self.enabled     = _envb("PF_PRIORITY_DYNAMIC", True)
        self.target_sec  = _envf("PF_PRIORITY_TARGET_SEC", 0.6)
        self.step_up     = _envf("PF_PRIORITY_STEP_UP", 1.6)
        self.step_down   = _envf("PF_PRIORITY_STEP_DOWN", 0.8)
        self.min_sol     = _envf("PF_PRIORITY_MIN_SOL", max(0.0, base * 0.5))
        self.max_sol     = _envf("PF_PRIORITY_MAX_SOL", max(self.min_sol, 0.12))
        self.current_sol = max(self.min_sol, min(self.max_sol, base))
        self._pending: Dict[str, float] = {}
        self._seed_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        if not (self.enabled and QN_PRIORITY_HINT and QN_RPC_URL):
            return
        if self._seed_task and not self._seed_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._seed_task = loop.create_task(self._seed_from_qn())

    async def _seed_from_qn(self) -> None:
        try:
            seed = await asyncio.to_thread(_qn_priority_seed_blocking)
            if seed and seed > 0:
                old = self.current_sol
                self.current_sol = max(self.min_sol, min(self.max_sol, float(seed)))
                if abs(self.current_sol - old) / max(1e-9, old) >= 0.05:
                    log.event("PRIO_SEED_QN", old_sol=old, seed_sol=self.current_sol)
        except Exception:
            pass

    def choose_lamports(self, override_sol: Optional[float]) -> Optional[int]:
        if override_sol is not None:
            try:
                return int(max(0, override_sol) * LAMPORTS_PER_SOL)
            except Exception:
                return None
        if not self.enabled:
            try:
                base = float(os.getenv("PF_PRIORITY_SOL") or "0")
            except Exception:
                base = 0.0
            return int(base * LAMPORTS_PER_SOL) if base > 0 else None
        return int(self.current_sol * LAMPORTS_PER_SOL)

    def observe(self, sig: Optional[str], t0: float) -> None:
        if not (self.enabled and sig):
            return
        self._pending[sig] = t0
        asyncio.create_task(self._watch(sig))

    async def _watch(self, sig: str) -> None:
        deadline = time.time() + 45
        poll = max(0.2, _envf("PF_STATUS_POLL_SEC", 0.7))
        while time.time() < deadline:
            try:
                st = await _get_signature_status(sig)
                if st and (st.get("confirmationStatus") or st.get("slot") or st.get("confirmed")):
                    dur = max(0.0, time.time() - self._pending.pop(sig, time.time()))
                    err = st.get("err")
                    self._tune(dur, had_error=bool(err))
                    if err:
                        log.event("PRIO_OBS_ERROR", sig=sig, dur=dur, err=str(err))
                    else:
                        log.every("prio.obs", n=10, msg="PRIO_OBS", sig=sig, dur=dur, fee_sol=self.current_sol)
                    return
            except Exception:
                pass
            await asyncio.sleep(poll)
        self._pending.pop(sig, None)
        self._tune(self.target_sec * 2.0, had_error=False)

    def _tune(self, inclusion_sec: float, *, had_error: bool) -> None:
        old = self.current_sol
        if had_error:
            self.current_sol = min(self.max_sol, max(self.min_sol, self.current_sol * self.step_up))
        else:
            if inclusion_sec > self.target_sec * 1.4:
                self.current_sol = min(self.max_sol, max(self.min_sol, self.current_sol * self.step_up))
            elif inclusion_sec < self.target_sec * 0.6:
                self.current_sol = min(self.max_sol, max(self.min_sol, self.current_sol * self.step_down))
        if abs(self.current_sol - old) / max(1e-9, old) >= 0.10:
            log.event("PRIO_TUNE", old_sol=old, new_sol=self.current_sol, incl_sec=inclusion_sec)


_PRIO = DynPriority()


async def _get_signature_status(sig: str) -> Optional[dict]:
    def _call() -> Optional[dict]:
        try:
            js = _rpc_post("getSignatureStatuses", [[sig], {"searchTransactionHistory": False}])
            res = js.get("result", {}).get("value", [])
            if res and isinstance(res[0], dict):
                return res[0]
        except Exception:
            pass
        return None
    return await asyncio.to_thread(_call)


async def start() -> None:
    await _PRIO.start()


async def shutdown() -> None:
    return None


async def _enforce_min_sol(required_spend_lamports: int = 0) -> bool:
    try:
        min_sol_env = os.getenv("MIN_SOL_BALANCE", "").strip()
        min_sol = float(min_sol_env) if min_sol_env else float(MIN_SOL_BALANCE)
    except Exception:
        min_sol = float(MIN_SOL_BALANCE)

    if min_sol <= 0:
        return True

    try:
        bal_lamports = int(get_balance_lamports())
    except Exception:
        bal_lamports = 0

    reserve = int(min_sol * LAMPORTS_PER_SOL)
    spend = int(required_spend_lamports or 0)
    if bal_lamports - spend < reserve:
        try:
            need_ui = max(0.0, (reserve + spend - bal_lamports) / LAMPORTS_PER_SOL)
            await _send_safe(
                "TRADE BLOCKED (SOL reserve)\n"
                f"Balance: {bal_lamports/1e9:.4f} SOL\n"
                f"Reserve: {min_sol:.4f} SOL\n"
                f"Spend est: {spend/1e9:.4f} SOL\n"
                f"Top up needed: {need_ui:.4f} SOL",
                src="router.guard.sol",
            )
        except Exception:
            pass
        return False
    return True

# ──────────────────────────────────────────────────────────────────────────────
# Swaps (public) — return (ok, signature|None)
# ──────────────────────────────────────────────────────────────────────────────
async def buy_via_lightning(*, mint: str, amount_sol: Optional[float] = None, lamports_in: Optional[int] = None, slippage_bps: Optional[int] = None, priority_sol: Optional[float] = None, compute_units: Optional[int] = None) -> Tuple[bool, Optional[str]]:
    try:
        m = _sanitize_mint(mint)
    except Exception as e:
        await _send_safe(f"BUY ERROR (mint)\nMint: {mint}\nErr: {str(e)[:180]}", src="router.buy.error.mint")
        return False, None

    kp = load_keypair()
    owner = get_pubkey_str(kp)

    if lamports_in is None:
        if amount_sol is None:
            amount_sol = _envf("PF_TRADE_SOL", 0.0)
        lamports_in = int(max(0.0, float(amount_sol)) * LAMPORTS_PER_SOL)
    if lamports_in <= 0:
        await _send_safe(f"BUY ERROR (size)\nMint: {m}\nErr: zero-size trade", src="router.buy.error.size")
        return False, None

    prio_est = _PRIO.choose_lamports(priority_sol)
    required = lamports_in + (int(prio_est) if prio_est else 0)
    if not await _enforce_min_sol(required_spend_lamports=required):
        return False, None

    sbps = _clamp_slippage_bps(slippage_bps)
    try:
        quote = await _retry(_jup_quote, SOL_MINT, m, lamports_in, tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-buy", slippage_bps=sbps, only_direct=ONLY_DIRECT_DEFAULT)
    except Exception as e:
        log.event("ROUTER_BUY_QUOTE_FAIL", mint=m, err=str(e))
        await _send_safe(f"BUY ERROR (quote)\nMint: {m}\nErr: {str(e)[:180]}", src="router.buy.error.quote")
        return False, None

    out_raw = int(quote.get("outAmount") or 0)
    dec = await _safe_decimals(m)
    out_ui = out_raw / (10 ** dec) if out_raw > 0 else 0.0

    size_sol = lamports_in / LAMPORTS_PER_SOL
    slip_pct = float(sbps) / 100.0
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    if DRY_RUN:
        await _send_safe(
            "BUY SIGNAL (dry-run)\n" f"Mint: {m}\n" f"Size: {size_sol:.4f} SOL\nSlippage: {slip_pct:.2f}%\nUTC: {ts}",
            src="router.buy.signal.dry",
        )
        return True, None

    await _send_safe(
        "BUY SIGNAL\n" f"Mint: {m}\n" f"Size: {size_sol:.4f} SOL\nSlippage: {slip_pct:.2f}%\nUTC: {ts}",
        src="router.buy.signal.live",
    )

    t0 = time.time()
    for attempt in range(1, SWAP_RETRIES + 1):
        try:
            prio = _PRIO.choose_lamports(priority_sol)
            tx_b64 = await _jup_swap(owner, quote, priority_lamports=prio, compute_units=compute_units)
            try:
                sig = await _send_b64_with_diag(tx_b64, skip_preflight=True)
            except Exception as e_send:
                try:
                    sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
                except Exception as e_local:
                    raise RuntimeError(f"send failed: rpc_err={e_send!r}, local_err={e_local!r}") from e_local

            _PRIO.observe(sig, t0)
            await _send_safe(f"BUY FILLED\nMint: {m}\nQty: {out_ui:,.6f}\nTx: https://solscan.io/tx/{sig}", src="router.buy.filled.live")
            return True, sig
        except Exception as e:
            if attempt >= SWAP_RETRIES:
                log.event("ROUTER_BUY_SWAP_FAIL", mint=m, err=str(e))
                await _send_safe(f"BUY ERROR\nMint: {m}\nErr: {str(e)[:180]}", src="router.buy.error.live")
                return False, None
            _PRIO._tune(_PRIO.target_sec * 1.6, had_error=True)
            await asyncio.sleep(RETRY_BACKOFF * max(1, attempt))


async def sell_via_lightning(*, mint: str, token_program: Optional[str], amount_raw: int, slippage_bps: Optional[int] = None, priority_sol: Optional[float] = None, compute_units: Optional[int] = None) -> Tuple[bool, Optional[str]]:
    del token_program
    try:
        m = _sanitize_mint(mint)
    except Exception as e:
        await _send_safe(f"SELL ERROR (mint)\nMint: {mint}\nErr: {str(e)[:180]}", src="router.sell.error.mint")
        return False, None
    if amount_raw <= 0:
        await _send_safe(f"SELL ERROR (size)\nMint: {m}\nErr: zero-size trade", src="router.sell.error.size")
        return False, None

    kp = load_keypair()
    owner = get_pubkey_str(kp)

    prio_est = _PRIO.choose_lamports(priority_sol)
    required = int(prio_est) if prio_est else 0
    if not await _enforce_min_sol(required_spend_lamports=required):
        return False, None

    sbps = _clamp_slippage_bps(slippage_bps)
    try:
        quote = await _retry(_jup_quote, m, SOL_MINT, amount_raw, tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-sell", slippage_bps=sbps, only_direct=ONLY_DIRECT_DEFAULT)
    except Exception as e:
        log.event("ROUTER_SELL_QUOTE_FAIL", mint=m, err=str(e))
        await _send_safe(f"SELL ERROR (quote)\nMint: {m}\nErr: {str(e)[:180]}", src="router.sell.error.quote")
        return False, None

    lamports = int(quote.get("outAmount") or 0)
    sol_out = lamports / LAMPORTS_PER_SOL
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

    if DRY_RUN:
        await _send_safe(
            "SELL SIGNAL (dry-run)\n" f"Mint: {m}\nAmount raw: {amount_raw}\nProceeds: {sol_out:.6f} SOL\nUTC: {ts}",
            src="router.sell.signal.dry",
        )
        return True, None

    await _send_safe(
        "SELL SIGNAL\n" f"Mint: {m}\nAmount raw: {amount_raw}\nProceeds est: {sol_out:.6f} SOL\nUTC: {ts}",
        src="router.sell.signal.live",
    )

    t0 = time.time()
    for attempt in range(1, SWAP_RETRIES + 1):
        try:
            prio = _PRIO.choose_lamports(priority_sol)
            tx_b64 = await _jup_swap(owner, quote, priority_lamports=prio, compute_units=compute_units)
            try:
                sig = await _send_b64_with_diag(tx_b64, skip_preflight=True)
            except Exception as e_send:
                try:
                    sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
                except Exception as e_local:
                    raise RuntimeError(f"send failed: rpc_err={e_send!r}, local_err={e_local!r}") from e_local

            _PRIO.observe(sig, t0)
            await _send_safe(f"SELL FILLED\nMint: {m}\nRealized: {sol_out:.6f} SOL\nTx: https://solscan.io/tx/{sig}", src="router.sell.filled.live")
            return True, sig
        except Exception as e:
            if attempt >= SWAP_RETRIES:
                log.event("ROUTER_SELL_SWAP_FAIL", mint=m, err=str(e))
                await _send_safe(f"SELL ERROR\nMint: {m}\nErr: {str(e)[:180]}", src="router.sell.error.live")
                return False, None
            _PRIO._tune(_PRIO.target_sec * 1.6, had_error=True)
            await asyncio.sleep(RETRY_BACKOFF * max(1, attempt))

# ──────────────────────────────────────────────────────────────────────────────
# Token metadata helpers
# ──────────────────────────────────────────────────────────────────────────────
async def get_tokens() -> List[dict]:
    return await _tokens_catalog()


def get_token_symbol(mint: str) -> Optional[str]:
    try:
        m = _sanitize_mint(mint)
    except Exception:
        return None
    meta = _TOK_META.get(m)
    s = (meta or {}).get("symbol")
    return str(s).strip() if s else None

# ──────────────────────────────────────────────────────────────────────────────
# Exports
# ──────────────────────────────────────────────────────────────────────────────
__all__ = [
    # lifecycle
    "start", "shutdown",
    # price/quote
    "get_sell_quote", "get_buy_quote", "get_price_ui", "price_of_ui", "price_of",
    # tokens/meta
    "get_tokens", "get_token_symbol", "mint_decimals", "get_decimals",
    # swaps
    "buy_via_lightning", "sell_via_lightning",
    # utils
    "normalize_pump_id_to_mint", "_is_valid_pubkey_str", "is_valid_pubkey_str",
    # config
    "TX_RPC_URL", "TX_COMMITMENT",
]
