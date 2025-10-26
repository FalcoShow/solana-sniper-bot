# utils_pkg/router.py
from __future__ import annotations

import asyncio
import base64
import json
import os
import random
import re
import time
from typing import Any, Dict, Optional, Tuple, Callable, Sequence, List

import requests
from requests.exceptions import RequestException

from logging_setup import get_logger
from utils_pkg.mints import normalize_mint  # single source of truth
from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# Telegram helper (safe import)
try:
    from utils_pkg.core_utils import send_telegram as _core_send_telegram  # async
except Exception:  # pragma: no cover
    async def _core_send_telegram(_msg: str) -> None:  # type: ignore
        return

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# logging safety shim (so .every always exists)
# ──────────────────────────────────────────────────────────────────────────────
if not hasattr(log, "every"):
    def _every(_key: str, n: int = 10, msg: str | None = None, **kw):
        return log.event(msg or _key, **kw)
    setattr(log, "every", _every)  # type: ignore[attr-defined]

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
# Chain / RPC config (with safe fallbacks)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg.txlearn import RPC_URL as _TX_RPC_URL, TX_COMMITMENT as _TX_COMMITMENT  # type: ignore
except Exception:
    _TX_RPC_URL = os.getenv("SOL_RPC_URL") or "https://api.mainnet-beta.solana.com"
    _TX_COMMITMENT = (os.getenv("CONFIRM_COMMITMENT") or "confirmed").strip().lower()

TX_RPC_URL = _TX_RPC_URL
TX_COMMITMENT = _TX_COMMITMENT

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000

# ──────────────────────────────────────────────────────────────────────────────
# Feature toggles / HTTP knobs
# ──────────────────────────────────────────────────────────────────────────────
DRY_RUN = _envb("DRYRUN", False) or _envb("IS_DRYRUN", False) or _envb("DRY_RUN", False)

HTTP_TIMEOUT_GET  = max(5.0, _envf("ROUTER_HTTP_TIMEOUT_GET_SEC", 20.0))
HTTP_TIMEOUT_POST = max(5.0, _envf("ROUTER_HTTP_TIMEOUT_POST_SEC", 25.0))
RPC_TIMEOUT_SEC   = max(3.0, _envf("ROUTER_RPC_TIMEOUT_SEC", 10.0))

QUOTE_RETRIES = max(1, _envi("ROUTER_QUOTE_RETRIES", 3))
SWAP_RETRIES  = max(1, _envi("ROUTER_SWAP_RETRIES", 3))
RETRY_BACKOFF = max(0.05, _envf("ROUTER_RETRY_BACKOFF_SEC", 0.35))

JUP_MAX_QPS       = max(1, _envi("JUP_MAX_QPS", 3))
_JUP_MIN_INTERVAL = 1.0 / float(JUP_MAX_QPS)
_JUP_RATE_LOCK    = asyncio.Lock()
_JUP_LAST_TS: float = 0.0

QUOTE_TTL_SEC = max(0.0, _envf("ROUTER_QUOTE_TTL_SEC", 10.0))
PRICE_TTL_SEC = max(0.0, _envf("PF_PRICE_TTL_SEC", 6.0))
TOKENS_TTL_SEC = max(30.0, _envf("ROUTER_TOKENS_TTL_SEC", 900.0))

_QUOTE_CACHE: Dict[str, Tuple[float, dict]] = {}
_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}
_TOKENS_CACHE: Tuple[float, List[dict]] | None = None
_QUOTE_FLIGHT: Dict[str, asyncio.Future[Any]] = {}
_quote_lock = asyncio.Lock()

# Bad ID blacklist for price calls (avoid repeated 400/429 hammers)
BAD_ID_TTL_SEC = max(10, _envi("ROUTER_BAD_ID_TTL_SEC", 60))
_BAD_PRICE_BLACKLIST: Dict[str, float] = {}

def _bl_get(raw: str) -> bool:
    exp = _BAD_PRICE_BLACKLIST.get(raw)
    if exp and exp >= time.time():
        return True
    if exp:
        _BAD_PRICE_BLACKLIST.pop(raw, None)
    return False

def _bl_set(raw: str) -> None:
    _BAD_PRICE_BLACKLIST[raw] = time.time() + BAD_ID_TTL_SEC

# Budget hooks (soft gates). Fail open if missing.
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover
    class _BudgetNoop:
        def before_quote(self, units: int = 1) -> bool: return True
        def after_quote(self, units: int = 1) -> None:  return None
        def before_rpc(self, method: str, *, weight: Optional[int] = None) -> bool: return True
        def after_rpc(self, method: str, *, weight: Optional[int] = None) -> None:  return None
    budget = _BudgetNoop()  # type: ignore

# Default UA
_UA = {"user-agent": "pnlsniper/1.0 (+router)", "accept": "application/json", "content-type": "application/json"}

# ──────────────────────────────────────────────────────────────────────────────
# Telegram helpers (robust; always logs why it skipped)
# ──────────────────────────────────────────────────────────────────────────────
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

# Strip characters that explode under MarkdownV2; send plain text
_MARKDOWNV2_SPECIALS = r"_*[]()~`>#+-=|{}.!\\"
def _tg_plain(msg: str) -> str:
    # safest: remove specials (works regardless of parse_mode)
    return "".join(ch for ch in msg if ch not in _MARKDOWNV2_SPECIALS)

async def _tg_send(msg: str, *, src: str) -> None:
    ok, why = _tg_ready()
    if not ok:
        log.every("tg.skip", n=15, msg="TG_SKIP", why=why, src=src)
        return
    try:
        await _core_send_telegram(_tg_plain(msg))
        log.every("tg.sent", n=1, msg="TG_SENT_ONCE", src=src)  # n=1 so you see it at least once
    except Exception as e:
        log.event("TG_SEND_ERROR", src=src, err=str(e))

# ──────────────────────────────────────────────────────────────────────────────
# Base58 helpers and mint sanitation
# ──────────────────────────────────────────────────────────────────────────────
_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_IDX = {c: i for i, c in enumerate(_B58_ALPHABET)}

def _b58decode(s: str) -> bytes:
    s = (s or "").strip()
    if not s:
        return b""
    n = 0
    for ch in s:
        i = _B58_IDX.get(ch)
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

# Public alias
def is_valid_pubkey_str(s: str) -> bool:
    return _is_valid_pubkey_str(s)

def _assert_pubkey(s: str, name: str) -> None:
    if not _is_valid_pubkey_str(s):
        raise ValueError(f"Invalid {name} (must be 32-byte base58): {s}")

def _sanitize_mint(m: str) -> str:
    m = (m or "").strip()
    if not m:
        raise ValueError("empty mint")
    try:
        return normalize_mint(m)
    except Exception:
        if _is_valid_pubkey_str(m):
            return m
        raise

# Back-compat export used elsewhere
def normalize_pump_id_to_mint(raw: str) -> str:
    return normalize_mint(raw)

# ──────────────────────────────────────────────────────────────────────────────
# Jupiter base URL(s), headers, and multi-endpoint fallback
# ──────────────────────────────────────────────────────────────────────────────
def _alt_bases() -> Sequence[str]:
    bases: list[str] = []
    primary = (os.getenv("JUP_BASE_URL") or "https://quote-api.jup.ag").strip().rstrip("/")
    if primary:
        bases.append(primary)
    alts = (os.getenv("JUP_ALT_BASE_URLS") or "").strip()
    if alts:
        for a in alts.split(","):
            a = a.strip().rstrip("/")
            if a and a not in bases:
                bases.append(a)
    # dedupe
    deduped: list[str] = []
    for b in bases:
        if b and b not in deduped:
            deduped.append(b)
    return deduped

def _is_qn_call_url(u: str) -> bool:
    u = (u or "").rstrip("/")
    return "/functions/" in u and u.endswith("/call")

def _compose_endpoint(base: str, kind: str) -> tuple[str, bool]:
    b = base.rstrip("/")
    if _is_qn_call_url(b):
        sep = "&" if "?" in b else "?"
        return f"{b}{sep}path=/v6/{kind}&mode=RAW", True
    return f"{b}/v6/{kind}", False

def _jup_headers_for(base: str) -> Dict[str, str]:
    h: Dict[str, str] = {
        **_UA,
        "origin": "https://jup.ag",
        "referer": "https://jup.ag/",
    }
    x_api = (os.getenv("JUP_API_KEY") or os.getenv("QUICKNODE_FUNCTION_KEY") or "").strip()
    if x_api:
        h["x-api-key"] = x_api
        h.setdefault("x-quicknode-mode", "RAW")
    # Allow PRO (api.jup.ag) via x-api-key if provided
    pro_key = (os.getenv("JUP_PRO_KEY") or "").strip()
    if pro_key and ("api.jup.ag" in base):
        h["x-api-key"] = pro_key
    # Old bearer (still accepted by some relays)
    jup_bearer = (os.getenv("JUP_BEARER") or "").strip()
    if jup_bearer:
        h["x-jup-auth"] = f"Bearer {jup_bearer}"
    return h

async def _http_get_with_fallback(kind: str, *, params: dict, timeout: float) -> dict:
    last_err: Optional[Exception] = None
    for idx, base in enumerate(_alt_bases()):
        url, is_qn = _compose_endpoint(base, kind)
        try:
            if is_qn:
                r = requests.post(url, json=None, params=params, timeout=timeout, headers=_jup_headers_for(base))
            else:
                r = requests.get(url, params=params, timeout=timeout, headers=_jup_headers_for(base))
            try:
                js = r.json()
            except Exception:
                r.raise_for_status()
                raise
            js = _unwrap_qn_action(r, js)
            if r.status_code in (200, 201):
                return js
            last_err = RuntimeError(f"{url} -> HTTP {r.status_code} ({str(js)[:200]})")
        except RequestException as e:
            last_err = e
        except Exception as e:
            last_err = e
        await asyncio.sleep(0.05 if idx == 0 else 0.1)
    raise last_err or RuntimeError("all quote endpoints failed")

async def _http_post_with_fallback(kind: str, *, json_body: Optional[dict], params: Optional[dict], timeout: float) -> dict:
    last_err: Optional[Exception] = None
    for idx, base in enumerate(_alt_bases()):
        url, is_qn = _compose_endpoint(base, kind)
        try:
            if is_qn:
                r = requests.post(url, json=json_body, params=params, timeout=timeout, headers=_jup_headers_for(base))
            else:
                r = requests.post(url, json=json_body, params=params, timeout=timeout, headers=_jup_headers_for(base))
            try:
                js = r.json()
            except Exception:
                r.raise_for_status()
                raise
            js = _unwrap_qn_action(r, js)
            if r.status_code in (200, 201):
                return js
            last_err = RuntimeError(f"{url} -> HTTP {r.status_code} ({str(js)[:200]})")
        except RequestException as e:
            last_err = e
        except Exception as e:
            last_err = e
        await asyncio.sleep(0.05 if idx == 0 else 0.1)
    raise last_err or RuntimeError("all swap endpoints failed")

# Ordered bases for QUOTES: RELAY -> PRO -> LITE -> V6
def _quote_bases_order() -> List[tuple[str, str]]:
    out: List[tuple[str, str]] = []
    relay = (os.getenv("JUP_BASE_URL") or "").strip().rstrip("/")
    if relay:
        out.append((relay, "v1"))
    pro  = (os.getenv("JUP_UPSTREAM") or "https://api.jup.ag").strip().rstrip("/")
    lite = (os.getenv("JUP_LITE")     or "https://lite-api.jup.ag").strip().rstrip("/")
    v6   = (os.getenv("JUP_LEGACY")   or "https://quote-api.jup.ag").strip().rstrip("/")
    out += [(pro, "v1"), (lite, "v1"), (v6, "v6")]
    # de-dupe preserve order
    seen = set()
    final: List[tuple[str, str]] = []
    for b, k in out:
        if b and (b, k) not in seen:
            seen.add((b, k))
            final.append((b, k))
    return final

async def _http_get_quote_any(*, params_v1: dict, params_v6: dict) -> dict:
    last_err: Optional[Exception] = None
    for base, kind in _quote_bases_order():
        try:
            await _jup_gate()
            if kind == "v1":
                url = f"{base}/swap/v1/quote"
                r = requests.get(url, params=params_v1, timeout=HTTP_TIMEOUT_GET, headers=_jup_headers_for(base))
            else:
                url = f"{base}/v6/quote"
                r = requests.get(url, params=params_v6, timeout=HTTP_TIMEOUT_GET, headers=_jup_headers_for(base))
            if r.status_code >= 400:
                raise RuntimeError(f"{kind}@{base} -> {r.status_code} {r.text[:300]}")
            return r.json()
        except Exception as e:
            last_err = e
            log.every("quote.fallback", n=10, msg="QUOTE_FALLBACK", base=base, kind=kind, err=str(e))
            continue
    raise last_err or RuntimeError("all quote endpoints failed")

# ──────────────────────────────────────────────────────────────────────────────
# QN/Jupiter envelope unwrapping
# ──────────────────────────────────────────────────────────────────────────────
def _jtry(s: str):
    try:
        return json.loads(s)
    except Exception:
        return None

def _b64txt(s: str):
    try:
        raw = base64.b64decode(s, validate=True)
        return raw.decode("utf-8", "replace")
    except Exception:
        return None

_JUP_POS_KEYS = {"outAmount", "routePlan", "swapTransaction", "otherAmountThreshold", "priceImpactPct", "expiration"}
_JUP_ERR_KEYS = {"errorCode", "errorMessage", "error", "statusCode", "message"}

def _looks_jup(d: dict) -> bool:
    if any(k in d for k in _JUP_POS_KEYS): return True
    if any(k in d for k in _JUP_ERR_KEYS): return True
    sub = d.get("data") or d.get("result")
    return isinstance(sub, dict) and _looks_jup(sub)

_JSON_IN_LOGS = re.compile(r"\{.*\}")

def _from_result_container(obj: Any) -> Any:
    if not isinstance(obj, dict): return None
    res = obj.get("result")
    if res is None: return None
    if isinstance(res, dict):
        for k in ("json", "body", "text", "data", "result", "rawBody"):
            if k in res:
                v = res[k]
                if isinstance(v, dict): return v
                if isinstance(v, str):
                    j = _jtry(v) or _jtry(_b64txt(v) or "")
                    if j is not None: return j
    if isinstance(res, str):
        j = _jtry(res) or _jtry(_b64txt(res) or "")
        if j is not None: return j
    return None

def _from_action_envelope(obj: Any) -> Any:
    if not isinstance(obj, dict): return None
    ud = obj.get("user_data")
    if isinstance(ud, dict): return ud
    if isinstance(ud, str):
        j = _jtry(ud) or _jtry(_b64txt(ud) or "")
        if j is not None: return j
    resp = obj.get("execution", {}).get("response", {})
    body = resp.get("body")
    if isinstance(body, dict): return body
    if isinstance(body, str):
        j = _jtry(body) or _jtry(_b64txt(body) or "")
        if j is not None: return j
    return None

def _from_logs(obj: Any) -> Any:
    if not isinstance(obj, dict): return None
    logs = obj.get("logs")
    if not isinstance(logs, (list, tuple)): return None
    for line in logs:
        if not isinstance(line, str): continue
        m = _JSON_IN_LOGS.search(line)
        if not m: continue
        j = _jtry(m.group(0))
        if isinstance(j, dict) and _looks_jup(j):
            return j
    return None

def _dfs_extract(node: Any) -> Any:
    if isinstance(node, dict) and _looks_jup(node): return node
    got = _from_result_container(node) or _from_action_envelope(node) or _from_logs(node)
    if isinstance(got, dict) and _looks_jup(got): return got
    if isinstance(node, dict):
        for v in node.values():
            g = _dfs_extract(v)
            if isinstance(g, dict) and _looks_jup(g): return g
        return None
    if isinstance(node, list):
        for it in node:
            g = _dfs_extract(it)
            if isinstance(g, dict) and _looks_jup(g): return g
        return None
    if isinstance(node, str):
        j = _jtry(node) or _jtry(_b64txt(node) or "")
        if j is not None: return _dfs_extract(j)
    return None

def _tiny_shape(o: Any) -> str:
    if isinstance(o, dict): return "dict:" + ",".join(list(o.keys())[:6])
    if isinstance(o, list): return f"list:{len(o)}"
    return type(o).__name__

def _unwrap_qn_action(response: requests.Response, parsed: Any) -> Any:
    try:
        st = int(getattr(response, "status_code", 0) or 0)
    except Exception:
        st = 0
    if st not in (200, 201):
        return parsed
    try:
        inner = _dfs_extract(parsed)
        if isinstance(inner, dict) and _looks_jup(inner):
            return inner
        try:
            log.every("qn.unwrap.miss", n=30, msg="QN_UNWRAP_MISS",
                      status=st, top=_tiny_shape(parsed),
                      hint=("result" if isinstance(parsed, dict) and "result" in parsed else
                            "execution" if isinstance(parsed, dict) and "execution" in parsed else
                            "logs" if isinstance(parsed, dict) and "logs" in parsed else
                            "no-hint"))
        except Exception:
            pass
        return parsed
    except Exception:
        return parsed

# ──────────────────────────────────────────────────────────────────────────────
# Jupiter pacing + retry
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
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (400, 404):
                log.event("ROUTER_RETRY_FAIL", label=label, err=str(e))
                raise
            if i < tries:
                sleep_for = backoff * i * (0.75 + random.random() * 0.5)
                log.event("ROUTER_RETRY_WARN", label=label, err=str(e))
                await asyncio.sleep(sleep_for)
            else:
                log.event("ROUTER_RETRY_FAIL", label=label, err=str(e))
                raise
    raise last_err if last_err else RuntimeError(f"{label} failed")

# ──────────────────────────────────────────────────────────────────────────────
# Minimal RPC helpers (decimals & sig status)
# ──────────────────────────────────────────────────────────────────────────────
_DECIMALS: Dict[str, int] = {}
_TOK_META: Dict[str, Dict[str, Any]] = {}  # FIX: initialize cleanly

_KNOWN_DECIMALS: Dict[str, int] = {
    SOL_MINT: 9,
    "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v": 6,  # USDC classic
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263": 6,  # USDC (CCTP)
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": 6,  # USDT
}

def _rpc_post(method: str, params: list) -> dict:
    if not TX_RPC_URL:
        return {}
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    if not budget.before_rpc(method):
        return {}
    try:
        r = requests.post(TX_RPC_URL, json=payload, timeout=RPC_TIMEOUT_SEC, headers=_UA)
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

async def _tokens_catalog() -> List[dict]:
    global _TOKENS_CACHE
    now = time.time()
    if _TOKENS_CACHE and _TOKENS_CACHE[0] >= now:
        return _TOKENS_CACHE[1]
    try:
        js = await _http_get_with_fallback("tokens", params={}, timeout=HTTP_TIMEOUT_GET)
        arr: List[dict] = js if isinstance(js, list) else []
        if arr:
            for t in arr:
                try:
                    maddr = (t.get("address") or "").strip()
                    if maddr:
                        _TOK_META[maddr] = t
                except Exception:
                    continue
            _TOKENS_CACHE = (now + TOKENS_TTL_SEC, arr)
            return arr
    except Exception:
        pass
    return _TOKENS_CACHE[1] if _TOKENS_CACHE else []

def _tok_symbol(mint: str) -> Optional[str]:
    meta = _TOK_META.get(mint)
    s = (meta or {}).get("symbol")
    return str(s).strip() if s else None

async def _decimals_from_tokens_api(mint: str) -> Optional[int]:
    arr = await _tokens_catalog()
    if not arr:
        return None
    meta = _TOK_META.get(mint)
    if not meta:
        return None
    try:
        d = int(meta.get("decimals"))
        return d if 0 <= d <= 18 else None
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

    def _rpc_probe() -> Optional[int]:
        try:
            js = _rpc_post("getAccountInfo", [mint, {"encoding": "base64"}])
            val = js.get("result", {}).get("value", {})
            data = val.get("data")
            if isinstance(data, list) and data:
                raw_b64 = data[0]
                buf = base64.b64decode(raw_b64)
                dec = _mint_decimals_from_data(buf)
                if dec is not None:
                    return int(dec)
        except Exception:
            return None
        return None

    dec = await asyncio.to_thread(_rpc_probe)
    if dec is None or dec < 0 or dec > 18:
        try:
            dec = await _decimals_from_tokens_api(mint)
            if dec is not None:
                log.every("dec.fallback", n=20, msg="DEC_FALLBACK_TOKENS", mint=mint, decimals=dec)
        except Exception:
            dec = None
    if dec is not None and 0 <= dec <= 18:
        _DECIMALS[mint] = int(dec)
        return int(dec)
    return None

async def get_decimals(mint: str) -> Optional[int]:
    mint = _sanitize_mint(mint)
    _assert_pubkey(mint, "mint")
    return await _get_decimals(mint)

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

# ──────────────────────────────────────────────────────────────────────────────
# Priority fee controller
# ──────────────────────────────────────────────────────────────────────────────
def _to_sol_from_lamports_like(x: float) -> float:
    lamports = float(x)
    if lamports >= 1e9:
        lamports = lamports / 1e6  # micro-lamports → lamports
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
        self.enabled        = _envb("PF_PRIORITY_DYNAMIC", True)
        self.target_sec     = _envf("PF_PRIORITY_TARGET_SEC", 1.2)
        self.step_up        = _envf("PF_PRIORITY_STEP_UP", 1.50)
        self.step_down      = _envf("PF_PRIORITY_STEP_DOWN", 0.85)
        self.min_sol        = _envf("PF_PRIORITY_MIN_SOL", max(0.0, base * 0.5))
        self.max_sol        = _envf("PF_PRIORITY_MAX_SOL", max(self.min_sol, 0.04))
        self.current_sol    = max(self.min_sol, min(self.max_sol, base))
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
        poll = max(0.2, _envf("PF_STATUS_POLL_SEC", 0.6))
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

async def start() -> None:
    await _PRIO.start()

async def shutdown() -> None:
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Low-level Jupiter helpers (with fallback)
# ──────────────────────────────────────────────────────────────────────────────
def _slippage_bps() -> int:
    v = os.getenv("PF_SLIPPAGE_BPS")
    if v:
        try:
            return max(1, int(v))
        except Exception:
            pass
    s = _envf("JUPITER_MAX_SLIPPAGE", 0.006)  # fraction -> default 0.6%
    return max(1, int(round(s * 10_000)))

def _as_legacy() -> bool:
    return (os.getenv("JUP_AS_LEGACY", "0") or "0").strip().lower() in {"1", "true", "yes", "y"}

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

async def _jup_quote(
    input_mint: str,
    output_mint: str,
    amount_iu: int,
    *,
    slippage_bps: Optional[int] = None,
    swap_mode: str = "ExactIn",
    only_direct: bool = False,
) -> dict:
    if amount_iu <= 0:
        raise ValueError("amount_iu must be > 0")
    _assert_pubkey(input_mint, "inputMint")
    _assert_pubkey(output_mint, "outputMint")

    sbps = int(slippage_bps if slippage_bps is not None else _slippage_bps())
    legacy = _as_legacy()
    key = _qkey(input_mint, output_mint, amount_iu, sbps, swap_mode, only_direct, legacy)

    cached = await _get_cached_quote(key)
    if cached is not None:
        return cached

    async def _fetch():
        if not budget.before_quote():
            raise RuntimeError("quote blocked by budget")
        try:
            await _jup_gate()
            # v1 params use string booleans + swapMode=ExactIn; v6 similar minus swapMode
            params_v1 = {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(amount_iu),
                "slippageBps": sbps,
                "swapMode": swap_mode,
                "onlyDirectRoutes": "true" if only_direct else "false",
                "asLegacyTransaction": "true" if legacy else "false",
            }
            params_v6 = {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(amount_iu),
                "slippageBps": sbps,
                "onlyDirectRoutes": "true" if only_direct else "false",
                "asLegacyTransaction": "true" if legacy else "false",
            }
            js = await _http_get_quote_any(params_v1=params_v1, params_v6=params_v6)
            await _set_cached_quote(key, js, QUOTE_TTL_SEC)
            return js
        finally:
            try:
                budget.after_quote()
            except Exception:
                pass

    return await _singleflight_quote(key, _fetch)

async def _jup_swap(
    user_pubkey: str,
    quote: dict,
    *,
    priority_lamports: Optional[int],
    compute_units: Optional[int],
) -> str:
    await _jup_gate()
    body = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": False if (compute_units and compute_units > 0) else True,
        "quoteResponse": quote,
        "asLegacyTransaction": True if _as_legacy() else False,
    }
    if priority_lamports is not None:
        body["prioritizationFeeLamports"] = int(priority_lamports)
    if compute_units and compute_units > 0:
        body["computeUnitLimit"] = int(compute_units)

    js = await _http_post_with_fallback("swap", json_body=body, params=None, timeout=HTTP_TIMEOUT_POST)
    tx_b64 = js.get("swapTransaction")
    if not tx_b64:
        raise RuntimeError(f"Jupiter returned no swapTransaction: {js}")
    return tx_b64

# ──────────────────────────────────────────────────────────────────────────────
# Public quote & price helpers
# ──────────────────────────────────────────────────────────────────────────────
async def get_sell_quote(*, mint: str, amount_raw: int, slippage_bps: Optional[int] = None) -> Dict[str, Any]:
    if amount_raw <= 0:
        raise ValueError("amount_raw must be > 0")
    mint = _sanitize_mint(mint)
    _assert_pubkey(mint, "mint")

    quote = await _retry(
        _jup_quote, mint, SOL_MINT, amount_raw,
        tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-sell",
        slippage_bps=slippage_bps,
    )
    lamports_out = int(quote.get("outAmount", "0") or 0)
    return {
        **quote,
        "lamports_out": lamports_out,
        "out_amount_sol": lamports_out / LAMPORTS_PER_SOL,
    }

async def get_buy_quote(
    *,
    mint: str,
    lamports_in: Optional[int] = None,
    amount_sol: Optional[float] = None,
    slippage_bps: Optional[int] = None,
) -> Dict[str, Any]:
    mint = _sanitize_mint(mint)
    _assert_pubkey(mint, "mint")

    if lamports_in is None and amount_sol is None:
        pf = os.getenv("PF_TRADE_SOL")
        if pf:
            amount_sol = _envf("PF_TRADE_SOL", 0.0)
        else:
            raise ValueError("Provide lamports_in or amount_sol")

    if lamports_in is None:
        lamports_in = int((amount_sol or 0.0) * LAMPORTS_PER_SOL)

    q = await _retry(
        _jup_quote, SOL_MINT, mint, lamports_in,
        tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-buy",
        slippage_bps=slippage_bps,
    )
    out_raw = int(q.get("outAmount", "0") or 0)
    dec = await _get_decimals(mint)
    ui = out_raw / (10 ** dec) if (dec is not None and dec >= 0) else 0.0
    return {
        **q,
        "lamports_in": lamports_in,
        "out_amount_raw": out_raw,
        "out_amount_ui": ui,
        "decimals": dec,
        "symbol": _tok_symbol(mint),
    }

def _price_probe_amounts(dec: int) -> list[int]:
    steps_ui = [1.0, 0.1, 0.01, 0.001]
    vals = []
    for ui in steps_ui:
        raw = int(round(ui * (10 ** max(0, dec))))
        if raw >= 1:
            vals.append(raw)
    vals = sorted(set(vals), reverse=True)
    return vals or [1]

async def get_price_ui(mint: str) -> float:
    try:
        if normalize_mint(mint) == SOL_MINT:
            return 1.0
    except Exception:
        pass

    raw = (mint or "").strip()
    if not raw:
        return 0.0
    if _bl_get(raw):
        return 0.0

    try:
        norm = _sanitize_mint(raw)
    except Exception:
        _bl_set(raw)
        log.every("price.bad", n=20, msg="PRICE_BAD_ID", raw=raw)
        return 0.0

    dec = await _get_decimals(norm)
    if dec is None or dec < 0 or dec > 18:
        _bl_set(raw)
        log.every("price.nodec", n=20, msg="PRICE_NO_DECIMALS", raw=raw, norm=norm)
        return 0.0

    if PRICE_TTL_SEC > 0:
        rec = _PRICE_CACHE.get(norm)
        if rec and rec[0] >= time.time():
            return float(rec[1])

    for amt_raw in _price_probe_amounts(dec):
        try:
            q = await _retry(
                _jup_quote, norm, SOL_MINT, int(amt_raw),
                tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-price",
            )
            lamports = int(q.get("outAmount", "0") or 0)
            if lamports <= 0:
                continue
            sol_out = lamports / LAMPORTS_PER_SOL
            tokens_ui = amt_raw / (10 ** dec)
            px = sol_out / tokens_ui if tokens_ui > 0 else 0.0
            if px > 0 and PRICE_TTL_SEC > 0:
                _PRICE_CACHE[norm] = (time.time() + PRICE_TTL_SEC, px)
            return px
        except Exception:
            continue

    _bl_set(raw)
    return 0.0

# Compat aliases
async def price_of_ui(mint: str) -> float:
    return await get_price_ui(mint)

async def price_of(mint: str) -> float:
    return await get_price_ui(mint)

async def mint_decimals(mint: str) -> Optional[int]:
    mint = _sanitize_mint(mint)
    _assert_pubkey(mint, "mint")
    return await _get_decimals(mint)

# ──────────────────────────────────────────────────────────────────────────────
# Swaps (public) — return (ok, signature|None)
# ──────────────────────────────────────────────────────────────────────────────
async def buy_via_lightning(
    *,
    mint: str,
    amount_sol: Optional[float] = None,
    lamports_in: Optional[int] = None,
    slippage_bps: Optional[int] = None,
    priority_sol: Optional[float] = None,
    compute_units: Optional[int] = None,
) -> Tuple[bool, Optional[str]]:
    mint = _sanitize_mint(mint)
    _assert_pubkey(mint, "mint")

    kp = load_keypair()
    owner = get_pubkey_str(kp)

    if lamports_in is None:
        if amount_sol is None:
            amount_sol = _envf("PF_TRADE_SOL", 0.0)
        lamports_in = int(amount_sol * LAMPORTS_PER_SOL)

    if lamports_in <= 0:
        return False, None

    try:
        quote = await _retry(
            _jup_quote, SOL_MINT, mint, lamports_in,
            tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-buy",
            slippage_bps=slippage_bps,
        )
    except Exception as e:
        log.event("ROUTER_BUY_QUOTE_FAIL", mint=mint, err=str(e))
        return False, None

    if DRY_RUN:
        out_raw = int(quote.get("outAmount", "0") or 0)
        dec = await _get_decimals(mint) or 6
        out_ui = out_raw / (10 ** dec)
        size_sol = lamports_in / LAMPORTS_PER_SOL
        slip_pct = float(slippage_bps if slippage_bps is not None else _slippage_bps()) / 100.0
        sym = _tok_symbol(mint) or mint[:6]
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())

        # BUY (dry-run) Telegram pings
        await _tg_send(
            (
                "BUY SIGNAL (dry-run)\n"
                f"Token: {sym}\n"
                f"Mint: {mint}\n"
                f"Size: {size_sol:.4f} SOL\n"
                f"Slippage: {slip_pct:.2f}%\n"
                f"UTC: {ts}"
            ),
            src="router.buy.signal",
        )
        px = (size_sol / max(out_ui, 1e-9))
        await _tg_send(
            (
                "BUY FILLED (dry-run)\n"
                f"Token: {sym}\n"
                f"Qty: {out_ui:,.6f}\n"
                f"Price: {px:.9f} SOL\n"
                "Tx: https://solscan.io/tx/FAKE"
            ),
            src="router.buy.filled",
        )

        log.event("DRYRUN_BUY", mint=mint, lamports_in=lamports_in, out_ui=out_ui)
        return True, None

    t0 = time.time()
    for attempt in range(1, SWAP_RETRIES + 1):
        try:
            prio = _PRIO.choose_lamports(priority_sol)
            tx_b64 = await _jup_swap(owner, quote, priority_lamports=prio, compute_units=compute_units)
            sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            _PRIO.observe(sig, t0)
            return True, sig
        except Exception as e:
            if attempt >= SWAP_RETRIES:
                log.event("ROUTER_BUY_SWAP_FAIL", mint=mint, err=str(e))
                return False, None
            _PRIO._tune(_PRIO.target_sec * 1.6, had_error=True)
            await asyncio.sleep(RETRY_BACKOFF * max(1, attempt))

async def sell_via_lightning(
    *,
    mint: str,
    token_program: Optional[str],
    amount_raw: int,
    slippage_bps: Optional[int] = None,
    priority_sol: Optional[float] = None,
    compute_units: Optional[int] = None,
) -> Tuple[bool, Optional[str]]:
    del token_program  # Jupiter infers it

    mint = _sanitize_mint(mint)
    _assert_pubkey(mint, "mint")

    if amount_raw <= 0:
        return False, None

    kp = load_keypair()
    owner = get_pubkey_str(kp)

    try:
        quote = await _retry(
            _jup_quote, mint, SOL_MINT, amount_raw,
            tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-sell",
            slippage_bps=slippage_bps,
        )
    except Exception as e:
        log.event("ROUTER_SELL_QUOTE_FAIL", mint=mint, err=str(e))
        return False, None

    if DRY_RUN:
        lamports = int(quote.get("outAmount", "0") or 0)
        sol_out = lamports / LAMPORTS_PER_SOL
        sym = _tok_symbol(mint) or mint[:6]

        # SELL (dry-run) Telegram pings
        await _tg_send(
            (
                "SELL SIGNAL (dry-run)\n"
                f"Token: {sym}\n"
                f"Amount raw: {amount_raw}\n"
                f"Proceeds: {sol_out:.6f} SOL"
            ),
            src="router.sell.signal",
        )
        await _tg_send(
            (
                "SELL FILLED (dry-run)\n"
                f"Token: {sym}\n"
                f"Realized: {sol_out:.6f} SOL\n"
                "Tx: https://solscan.io/tx/FAKE"
            ),
            src="router.sell.filled",
        )

        log.event("DRYRUN_SELL", mint=mint, amount_raw=amount_raw, out_sol=sol_out)
        return True, None

    t0 = time.time()
    for attempt in range(1, SWAP_RETRIES + 1):
        try:
            prio = _PRIO.choose_lamports(priority_sol)
            tx_b64 = await _jup_swap(owner, quote, priority_lamports=prio, compute_units=compute_units)
            sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            _PRIO.observe(sig, t0)
            return True, sig
        except Exception as e:
            if attempt >= SWAP_RETRIES:
                log.event("ROUTER_SELL_SWAP_FAIL", mint=mint, err=str(e))
                return False, None
            _PRIO._tune(_PRIO.target_sec * 1.6, had_error=True)
            await asyncio.sleep(RETRY_BACKOFF * max(1, attempt))

# ──────────────────────────────────────────────────────────────────────────────
# Convenience: token metadata accessors
# ──────────────────────────────────────────────────────────────────────────────
async def get_tokens() -> List[dict]:
    return await _tokens_catalog()

def get_token_symbol(mint: str) -> Optional[str]:
    try:
        m = _sanitize_mint(mint)
    except Exception:
        return None
    return _tok_symbol(m)

__all__ = [
    # lifecycle
    "start", "shutdown",
    # quotes / price / tokens
    "get_sell_quote", "get_buy_quote", "get_price_ui", "price_of_ui", "price_of",
    "get_tokens", "get_token_symbol",
    # swaps
    "buy_via_lightning", "sell_via_lightning",
    # mint helpers
    "normalize_pump_id_to_mint", "_is_valid_pubkey_str", "is_valid_pubkey_str",
    "mint_decimals", "get_decimals",
    # exposed RPC config
    "TX_RPC_URL", "TX_COMMITMENT",
]
