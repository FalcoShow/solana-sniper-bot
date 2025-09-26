# utils_pkg/router.py
from __future__ import annotations

import asyncio
import base64
import os
import random
import time
from typing import Any, Dict, Optional, Tuple, Callable, List

import requests

from logging_setup import get_logger
from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# logging safety shim (so .every always exists)
# ──────────────────────────────────────────────────────────────────────────────
if not hasattr(log, "every"):
    def _every(_key: str, n: int = 10, msg: str | None = None, **kw):
        return log.event(msg or _key, **kw)
    setattr(log, "every", _every)  # type: ignore[attr-defined]

# ──────────────────────────────────────────────────────────────────────────────
# Chain / RPC config (with safe fallbacks)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg.txlearn import RPC_URL as _TX_RPC_URL, TX_COMMITMENT as _TX_COMMITMENT  # type: ignore
except Exception:
    _TX_RPC_URL = os.getenv("SOL_RPC_URL") or "https://api.mainnet-beta.solana.com"
    _TX_COMMITMENT = "confirmed"

TX_RPC_URL = _TX_RPC_URL
TX_COMMITMENT = _TX_COMMITMENT

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS_PER_SOL = 1_000_000_000

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
# Feature toggles / HTTP knobs
# ──────────────────────────────────────────────────────────────────────────────
DRY_RUN = _envb("DRY_RUN", False)

HTTP_TIMEOUT_GET  = max(5.0, _envf("ROUTER_HTTP_TIMEOUT_GET_SEC", 20.0))
HTTP_TIMEOUT_POST = max(5.0, _envf("ROUTER_HTTP_TIMEOUT_POST_SEC", 25.0))
RPC_TIMEOUT_SEC   = max(3.0, _envf("ROUTER_RPC_TIMEOUT_SEC", 10.0))

JUP_QUOTE_URL = os.getenv("JUP_QUOTE_URL", "https://quote-api.jup.ag/v6/quote")
JUP_SWAP_URL  = os.getenv("JUP_SWAP_URL",  "https://quote-api.jup.ag/v6/swap")

def _slippage_bps() -> int:
    """
    Slippage priority: PF_SLIPPAGE_BPS (int bps) >
                       JUPITER_MAX_SLIPPAGE (fraction) >
                       default 0.0125 (125 bps)
    """
    v = os.getenv("PF_SLIPPAGE_BPS")
    if v:
        try:
            return max(1, int(v))
        except Exception:
            pass
    s = _envf("JUPITER_MAX_SLIPPAGE", 0.0125)  # fraction -> bps
    return max(1, int(round(s * 10_000)))

def _as_legacy() -> bool:
    return (os.getenv("JUP_AS_LEGACY", "1") or "1").strip().lower() in {"1", "true", "yes", "y"}

# Retries / pacing
QUOTE_RETRIES = max(1, _envi("ROUTER_QUOTE_RETRIES", 1))
SWAP_RETRIES  = max(1, _envi("ROUTER_SWAP_RETRIES", 3))
RETRY_BACKOFF = max(0.05, _envf("ROUTER_RETRY_BACKOFF_SEC", 0.35))

JUP_MAX_QPS       = max(1, _envi("JUP_MAX_QPS", 2))
_JUP_MIN_INTERVAL = 1.0 / float(JUP_MAX_QPS)
_JUP_RATE_LOCK    = asyncio.Lock()
_JUP_LAST_TS: float = 0.0

# Cache / single-flight
QUOTE_TTL_SEC = max(0.0, _envf("ROUTER_QUOTE_TTL_SEC", 2.0))
PRICE_TTL_SEC = max(0.0, _envf("PF_PRICE_TTL_SEC", 5.0))
_QUOTE_CACHE: Dict[str, Tuple[float, dict]] = {}
_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}
_QUOTE_FLIGHT: Dict[str, asyncio.Future[Any]] = {}
_quote_lock = asyncio.Lock()

# Bad ID blacklist for price calls (avoid repeated 400s/429s)
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

# Common headers
_UA = {"user-agent": "pnlsniper/1.0 (+router)", "accept": "application/json"}

# ──────────────────────────────────────────────────────────────────────────────
# Base58 helpers (no external deps)
# ──────────────────────────────────────────────────────────────────────────────
_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_IDX = {c: i for i, c in enumerate(_B58_ALPHABET)}

def _b58decode(s: str) -> bytes:
    """Minimal base58 decode (no checksum)."""
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
    """True iff base58 string decodes to exactly 32 bytes."""
    try:
        return len(_b58decode(s)) == 32
    except Exception:
        return False

# Public-friendly alias
def is_valid_pubkey_str(s: str) -> bool:
    return _is_valid_pubkey_str(s)

def _scan_for_mints(s: str) -> List[str]:
    """Scan a string for any 43/44-char windows that decode to 32 bytes."""
    found: List[str] = []
    s = (s or "").strip()
    for i in range(0, max(0, len(s) - 43) + 1):
        for L in (44, 43):
            j = i + L
            if j <= len(s):
                sub = s[i:j]
                if _is_valid_pubkey_str(sub) and sub not in found:
                    found.append(sub)
    return found

def _strip_pseudo_suffix(raw: str) -> tuple[str, Optional[str]]:
    """
    Returns (base, suffix) where suffix ∈ {"pump","bonk",None}.
    """
    s = (raw or "").strip()
    for suf in ("pump", "bonk"):
        if s.endswith(suf):
            return s[:-len(suf)].strip(), suf
    return s, None

def normalize_pump_id_to_mint(raw: str) -> str:
    """
    Extract a clean 32-byte mint pubkey from:
      - a plain mint
      - a Pump Portal id that may end in 'pump'/'bonk'
      - strings that contain the mint (scan windows)

    Strategy:
      1) If it ends with 'pump'/'bonk', try dropping the suffix. If that
         decodes to 32 bytes, return it.
      2) If the cleaned string is already a 32-byte base58, return it.
      3) Scan the cleaned string for any 43/44-char window that decodes to 32.
      4) Scan the ORIGINAL raw string for any 43/44 window that decodes to 32.
         - If multiple distinct candidates are found, refuse (avoid false hits).
      5) LAST RESORT: if the raw itself decodes to 32, return raw.
      6) Else raise.
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("empty pump id")

    base, suf = _strip_pseudo_suffix(s)
    if suf:
        if _is_valid_pubkey_str(base):
            return base
        # base failed; be conservative and only accept an unambiguous single match
        cands = _scan_for_mints(s)
        if len(cands) == 1:
            log.every("mint.scan_suffix", n=20, msg="MINT_SCAN_SUFFIX", raw=s, cand=cands[0])
            return cands[0]
        raise ValueError(f"Cannot normalize pump id to a mint: '{raw}'")

    if _is_valid_pubkey_str(base):
        return base

    cands = _scan_for_mints(base)
    if len(cands) == 1:
        return cands[0]
    if len(cands) > 1:
        raise ValueError(f"ambiguous candidates in '{raw}'")

    if _is_valid_pubkey_str(s):
        return s

    raise ValueError(f"Cannot normalize pump id to a mint: '{raw}'")

def _sanitize_mint(m: str) -> str:
    """
    Always sanitize potential pump/bonk ids before sending to Jupiter/RPC.
    Never throws if the input at least decodes to 32 bytes; raises otherwise.
    """
    m = (m or "").strip()
    if not m:
        raise ValueError("empty mint")
    try:
        return normalize_pump_id_to_mint(m)
    except Exception:
        if _is_valid_pubkey_str(m):
            return m
        raise

def _assert_pubkey(s: str, name: str) -> None:
    if not _is_valid_pubkey_str(s):
        raise ValueError(f"Invalid {name} (must be 32-byte base58): {s}")

# ──────────────────────────────────────────────────────────────────────────────
# Non-blocking HTTP (requests in worker threads)
# ──────────────────────────────────────────────────────────────────────────────
async def _get_json(url: str, *, params: Optional[dict] = None, timeout: float | int = HTTP_TIMEOUT_GET) -> dict:
    def _req():
        r = requests.get(url, params=params, timeout=timeout, headers=_UA)
        r.raise_for_status()
        ctype = (r.headers.get("content-type") or "").lower()
        if "application/json" not in ctype:
            try:
                return r.json()
            except Exception:
                raise RuntimeError(f"Non-JSON response: {ctype} {r.text[:200]}")
        return r.json()
    return await asyncio.to_thread(_req)

async def _post_json(url: str, *, json_body: dict, timeout: float | int = HTTP_TIMEOUT_POST) -> dict:
    def _req():
        r = requests.post(url, json=json_body, timeout=timeout, headers=_UA)
        r.raise_for_status()
        ctype = (r.headers.get("content-type") or "").lower()
        if "application/json" not in ctype:
            try:
                return r.json()
            except Exception:
                raise RuntimeError(f"Non-JSON response: {ctype} {r.text[:200]}")
        return r.json()
    return await asyncio.to_thread(_req)

async def _retry(coro_fn: Callable, *args, tries: int, backoff: float, label: str, **kwargs) -> Any:
    last_err: Optional[Exception] = None
    for i in range(1, tries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            status = getattr(getattr(e, "response", None), "status_code", None)
            # 400/404 are almost always invalid inputs; don't spin
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
# Simple RPC helpers (for decimals & sig status)
# ──────────────────────────────────────────────────────────────────────────────
_DECIMALS: Dict[str, int] = {}

def _rpc_post(method: str, params: list) -> dict:
    if not TX_RPC_URL:
        return {}
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    if not budget.before_rpc(method):
        return {}
    r = requests.post(TX_RPC_URL, json=payload, timeout=RPC_TIMEOUT_SEC, headers=_UA)
    budget.after_rpc(method)
    r.raise_for_status()
    return r.json()

def _mint_decimals_from_data(b: bytes) -> Optional[int]:
    # SPL Mint layout: decimals at byte offset 44
    try:
        if len(b) >= 45:
            return int(b[44])
    except Exception:
        pass
    return None

async def _get_decimals(mint: str) -> Optional[int]:
    d = _DECIMALS.get(mint)
    if d is not None:
        return d

    def _call() -> Optional[int]:
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

    dec = await asyncio.to_thread(_call)
    if dec is not None:
        _DECIMALS[mint] = int(dec)
    return dec

# public export (optional helper used by main.py)
async def get_decimals(mint: str) -> Optional[int]:
    """Return SPL token decimals for a mint (cached)."""
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
# Dynamic priority fee controller
# ──────────────────────────────────────────────────────────────────────────────
def _to_sol_from_lamports_like(x: float) -> float:
    lamports = float(x)
    if lamports >= 1e9:
        lamports = lamports / 1e6  # micro-lamports -> lamports
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
        base = _envf("PF_PRIORITY_SOL", 0.002)
        self.enabled        = _envb("PF_PRIORITY_DYNAMIC", True)
        self.target_sec     = _envf("PF_PRIORITY_TARGET_SEC", 2.0)
        self.step_up        = _envf("PF_PRIORITY_STEP_UP", 1.50)
        self.step_down      = _envf("PF_PRIORITY_STEP_DOWN", 0.85)
        self.min_sol        = _envf("PF_PRIORITY_MIN_SOL", max(0.0, base * 0.5))
        self.max_sol        = _envf("PF_PRIORITY_MAX_SOL", max(self.min_sol, base * 5))
        self.current_sol    = max(self.min_sol, min(self.max_sol, base))
        self._pending: Dict[str, float] = {}  # sig -> t0
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
            await asyncio.sleep(max(0.2, _envf("PF_STATUS_POLL_SEC", 0.9)))
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
    """Optional background seeding (idempotent)."""
    await _PRIO.start()

async def shutdown() -> None:
    """No-op for compatibility (no aiohttp session to close)."""
    return None

# ──────────────────────────────────────────────────────────────────────────────
# Jupiter pacing
# ──────────────────────────────────────────────────────────────────────────────
async def _jup_gate() -> None:
    global _JUP_LAST_TS
    async with _JUP_RATE_LOCK:
        now = time.time()
        wait_for = _JUP_MIN_INTERVAL - (now - _JUP_LAST_TS)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _JUP_LAST_TS = time.time()

# ──────────────────────────────────────────────────────────────────────────────
# Quote cache / single-flight
# ──────────────────────────────────────────────────────────────────────────────
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
            fut = asyncio.ensure_future(maker())  # maker returns a coroutine
            _QUOTE_FLIGHT[key] = fut
    try:
        return await fut
    finally:
        async with _quote_lock:
            _QUOTE_FLIGHT.pop(key, None)

# ──────────────────────────────────────────────────────────────────────────────
# Low-level Jupiter helpers
# ──────────────────────────────────────────────────────────────────────────────
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
            params = {
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(amount_iu),
                "slippageBps": sbps,
                "swapMode": swap_mode,
                # Jupiter expects lowercase true/false strings in query params.
                "onlyDirectRoutes": "true" if only_direct else "false",
            }
            js = await _get_json(JUP_QUOTE_URL, params=params, timeout=HTTP_TIMEOUT_GET)
            await _set_cached_quote(key, js, QUOTE_TTL_SEC)
            return js
        finally:
            try:
                budget.after_quote()
            except Exception:
                pass

    return await _singleflight_quote(key, _fetch)

async def _jup_swap(user_pubkey: str, quote: dict, *, priority_lamports: Optional[int]) -> str:
    await _jup_gate()

    body = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "quoteResponse": quote,
        "asLegacyTransaction": True if _as_legacy() else False,
    }
    if priority_lamports is not None:
        body["prioritizationFeeLamports"] = int(priority_lamports)

    js = await _post_json(JUP_SWAP_URL, json_body=body, timeout=HTTP_TIMEOUT_POST)
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
    lamports_out = int(quote.get("outAmount", "0"))
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
    out_raw = int(q.get("outAmount", "0"))
    dec = await _get_decimals(mint)
    ui = out_raw / (10 ** dec) if (dec is not None and dec >= 0) else 0.0
    return {**q, "lamports_in": lamports_in, "out_amount_raw": out_raw, "out_amount_ui": ui, "decimals": dec}

async def get_price_ui(mint: str) -> float:
    """
    Returns the SOL price per 1 token (UI units).
    Accepts raw Pump/BONK ids or real mints. Never raises; returns 0.0 on failure.
    """
    raw = (mint or "").strip()
    if not raw:
        return 0.0
    if _bl_get(raw):
        return 0.0

    # Normalize to a real mint, or bail quietly
    try:
        norm = _sanitize_mint(raw)
    except Exception:
        _bl_set(raw)
        log.every("price.bad", n=20, msg="PRICE_BAD_ID", raw=raw)
        return 0.0

    # decimals
    dec = await _get_decimals(norm)
    if dec is None or dec < 0 or dec > 18:
        _bl_set(raw)
        log.every("price.nodec", n=20, msg="PRICE_NO_DECIMALS", raw=raw, norm=norm)
        return 0.0

    # Serve from cache if fresh
    if PRICE_TTL_SEC > 0:
        rec = _PRICE_CACHE.get(norm)
        if rec and rec[0] >= time.time():
            return float(rec[1])

    # Tiny exact-in quote for pricing
    tiny_raw = max(1, 10 ** max(0, dec - 3))
    try:
        q = await _retry(
            _jup_quote, norm, SOL_MINT, tiny_raw,
            tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-price",
        )
        lamports = int(q.get("outAmount", "0"))
        if lamports <= 0:
            _bl_set(raw)
            return 0.0
        sol_out = lamports / LAMPORTS_PER_SOL
        tokens_ui = tiny_raw / (10 ** dec)
        px = sol_out / tokens_ui if tokens_ui > 0 else 0.0
        if PRICE_TTL_SEC > 0:
            _PRICE_CACHE[norm] = (time.time() + PRICE_TTL_SEC, px)
        return px
    except Exception:
        _bl_set(raw)  # e.g., Jupiter 400 due to non-mint address
        return 0.0

# Compat aliases
async def price_of_ui(mint: str) -> float:
    return await get_price_ui(mint)

async def price_of(mint: str) -> float:
    return await get_price_ui(mint)

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
        out_raw = int(quote.get("outAmount", "0"))
        dec = await _get_decimals(mint)
        out_ui = out_raw / (10 ** dec) if (dec is not None and dec >= 0) else 0.0
        log.event("DRYRUN_BUY", mint=mint, lamports_in=lamports_in, out_ui=out_ui)
        return True, None

    t0 = time.time()
    for attempt in range(1, SWAP_RETRIES + 1):
        try:
            prio = _PRIO.choose_lamports(priority_sol)
            tx_b64 = await _jup_swap(owner, quote, priority_lamports=prio)
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
) -> Tuple[bool, Optional[str]]:
    del token_program  # Jupiter doesn't need this here

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
        lamports = int(quote.get("outAmount", "0"))
        log.event("DRYRUN_SELL", mint=mint, amount_raw=amount_raw, out_sol=lamports / LAMPORTS_PER_SOL)
        return True, None

    t0 = time.time()
    for attempt in range(1, SWAP_RETRIES + 1):
        try:
            prio = _PRIO.choose_lamports(priority_sol)
            tx_b64 = await _jup_swap(owner, quote, priority_lamports=prio)
            sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
            _PRIO.observe(sig, t0)
            return True, sig
        except Exception as e:
            if attempt >= SWAP_RETRIES:
                log.event("ROUTER_SELL_SWAP_FAIL", mint=mint, err=str(e))
                return False, None
            _PRIO._tune(_PRIO.target_sec * 1.6, had_error=True)
            await asyncio.sleep(RETRY_BACKOFF * max(1, attempt))

__all__ = [
    # lifecycle
    "start", "shutdown",
    # quotes / price
    "get_sell_quote", "get_buy_quote", "get_price_ui", "price_of_ui", "price_of",
    # swaps
    "buy_via_lightning", "sell_via_lightning",
    # mint helpers
    "normalize_pump_id_to_mint", "_is_valid_pubkey_str", "is_valid_pubkey_str",
    # extras exposed for callers
    "get_decimals",
    # exposed RPC config for legacy callers
    "TX_RPC_URL", "TX_COMMITMENT",
]
