# utils_pkg/router.py
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, Optional, Tuple, Callable

import random
import requests

from logging_setup import get_logger
from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)
from utils_pkg.solana_token import fetch_mint_info
# Only take constants; DO NOT pull in _Rpc (it uses aiohttp and leaks sessions)
from utils_pkg.txlearn import TX_RPC_URL, TX_COMMITMENT  # keep compatibility

log = get_logger()

# =============================================================================
# Config / ENV
# =============================================================================
SOL_MINT = "So11111111111111111111111111111111111111112"

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

DRY_RUN = _envb("DRY_RUN", False)

# Jupiter endpoints
JUP_QUOTE_URL = os.getenv("JUP_QUOTE_URL", "https://quote-api.jup.ag/v6/quote")
JUP_SWAP_URL  = os.getenv("JUP_SWAP_URL",  "https://quote-api.jup.ag/v6/swap")

# Slippage: env is fraction (e.g. 0.0125 => 1.25%)
def _slippage_bps() -> int:
    s = _envf("JUPITER_MAX_SLIPPAGE", 0.0125)
    return max(1, int(round(s * 10_000)))

def _as_legacy() -> bool:
    v = os.getenv("JUP_AS_LEGACY", "1")
    return str(v).strip().lower() in {"1", "true", "yes", "y"}

# Router retry knobs (make quote retries 1 by default — 400s shouldn’t retry)
QUOTE_RETRIES = max(1, _envi("ROUTER_QUOTE_RETRIES", 1))
SWAP_RETRIES  = max(1, _envi("ROUTER_SWAP_RETRIES", 2))
RETRY_BACKOFF = max(0.05, _envf("ROUTER_RETRY_BACKOFF_SEC", 0.35))

# Global Jupiter rate gate (QPS). Default 3 QPS is gentle enough for v6.
JUP_MAX_QPS   = max(1, _envi("JUP_MAX_QPS", 3))
_JUP_MIN_INTERVAL = 1.0 / float(JUP_MAX_QPS)
_JUP_RATE_LOCK = asyncio.Lock()
_JUP_LAST_TS: float = 0.0

# Caching / single-flight to cut quote storms
QUOTE_TTL_SEC = max(0.0, _envf("ROUTER_QUOTE_TTL_SEC", 2.0))
PRICE_TTL_SEC = max(0.0, _envf("PF_PRICE_TTL_SEC", 5.0))  # shared with other modules
_QUOTE_CACHE: Dict[str, Tuple[float, dict]] = {}          # key -> (expiry_ts, payload)
_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}         # mint -> (expiry_ts, price_ui)
_QUOTE_FLIGHT: Dict[str, asyncio.Future[Any]] = {}        # key -> future
_quote_lock = asyncio.Lock()

# Optional budget hooks (soft gates). Fail open if module is missing.
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # graceful no-ops
    class _BudgetNoop:
        def before_quote(self, units: int = 1) -> bool: return True
        def after_quote(self, units: int = 1) -> None:  return None
        def before_rpc(self, method: str, *, weight: Optional[int] = None) -> bool: return True
        def after_rpc(self, method: str, *, weight: Optional[int] = None) -> None:  return None
    budget = _BudgetNoop()  # type: ignore

# Common HTTP headers
_UA = {"user-agent": "pnlsniper/1.0 (+router)"}

# =============================================================================
# Non-blocking HTTP helpers (requests in worker thread)
# =============================================================================
async def _get_json(url: str, *, params: Optional[dict] = None, timeout: int = 20) -> dict:
    def _req():
        r = requests.get(url, params=params, timeout=timeout, headers=_UA)
        r.raise_for_status()
        return r.json()
    return await asyncio.to_thread(_req)

async def _post_json(url: str, *, json_body: dict, timeout: int = 25) -> dict:
    def _req():
        r = requests.post(url, json=json_body, timeout=timeout, headers=_UA)
        r.raise_for_status()
        return r.json()
    return await asyncio.to_thread(_req)

async def _retry(coro_fn: Callable, *args, tries: int, backoff: float, label: str, **kwargs) -> Any:
    last_err: Optional[Exception] = None
    for i in range(1, tries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            # If it’s an HTTPError and status is 400/404, don’t spam retries
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (400, 404):
                log.event("ROUTER_RETRY_FAIL", label=label, err=str(e))
                raise
            if i < tries:
                # jitter to play nice with 429s
                sleep_for = backoff * i * (0.75 + random.random() * 0.5)
                log.event("ROUTER_RETRY_WARN", label=label, err=str(e))
                await asyncio.sleep(sleep_for)
            else:
                log.event("ROUTER_RETRY_FAIL", label=label, err=str(e))
                raise
    raise last_err if last_err else RuntimeError(f"{label} failed")

# =============================================================================
# Async decimals cache
# =============================================================================
_DECIMALS: Dict[str, int] = {}

async def _get_decimals(mint: str) -> Optional[int]:
    d = _DECIMALS.get(mint)
    if d is not None:
        return d
    try:
        info = await fetch_mint_info(mint)
        if not info:
            return None
        dec, _owner = info
        _DECIMALS[mint] = int(dec)
        return int(dec)
    except Exception:
        return None

# =============================================================================
# Minimal RPC helper (requests) — replaces txlearn._Rpc to avoid aiohttp leaks
# =============================================================================
def _rpc_post(method: str, params: list) -> dict:
    if not TX_RPC_URL:
        return {}
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(TX_RPC_URL, json=payload, timeout=10, headers=_UA)
    r.raise_for_status()
    return r.json()

async def _get_signature_status(sig: str) -> Optional[dict]:
    def _call() -> Optional[dict]:
        try:
            # getSignatureStatuses returns array under "result.value"
            js = _rpc_post("getSignatureStatuses", [[sig], {"searchTransactionHistory": False}])
            res = js.get("result", {}).get("value", [])
            if res and isinstance(res[0], dict):
                return res[0]
        except Exception:
            pass
        return None
    return await asyncio.to_thread(_call)

# =============================================================================
# Dynamic priority fee controller (no aiohttp, uses _get_signature_status)
# =============================================================================
def _to_sol_from_lamports_like(x: float) -> float:
    lamports = float(x)
    if lamports >= 1e9:
        lamports = lamports / 1e6  # micro-lamports -> lamports
    return lamports / 1e9

# Optional QuickNode seed (best-effort)
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
                return int(max(0, override_sol) * 1_000_000_000)
            except Exception:
                return None
        if not self.enabled:
            try:
                base = float(os.getenv("PF_PRIORITY_SOL") or "0")
            except Exception:
                base = 0.0
            return int(base * 1e9) if base > 0 else None
        return int(self.current_sol * 1_000_000_000)

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

# module-level instance
_PRIO = DynPriority()

# Public hook to start optional background seeding (safe to call multiple times)
async def start() -> None:
    await _PRIO.start()

# =============================================================================
# Jupiter rate gate
# =============================================================================
async def _jup_gate() -> None:
    global _JUP_LAST_TS
    async with _JUP_RATE_LOCK:
        now = time.time()
        wait_for = _JUP_MIN_INTERVAL - (now - _JUP_LAST_TS)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _JUP_LAST_TS = time.time()

# =============================================================================
# Quote cache/single-flight helpers
# =============================================================================
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

async def _singleflight_quote(key: str, maker: Callable[[], "asyncio.Future[Any]"]) -> dict:
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

# =============================================================================
# Low-level Jupiter helpers (async) with budget hooks + cache + rate gate
# =============================================================================
async def _jup_quote(
    input_mint: str,
    output_mint: str,
    amount_iu: int,
    *,
    slippage_bps: Optional[int] = None,
    swap_mode: str = "ExactIn",
    only_direct: bool = False,
    as_legacy: Optional[bool] = None,  # kept for signature parity; NOT sent to /quote
) -> dict:
    if amount_iu <= 0:
        raise ValueError("amount_iu must be > 0")

    sbps = int(slippage_bps if slippage_bps is not None else _slippage_bps())
    legacy = True if (as_legacy if as_legacy is not None else _as_legacy()) else False
    key = _qkey(input_mint, output_mint, amount_iu, sbps, swap_mode, only_direct, legacy)

    # cache hit?
    cached = await _get_cached_quote(key)
    if cached is not None:
        return cached

    async def _fetch():
        if not budget.before_quote():
            raise RuntimeError("quote blocked by budget")

        # Respect global rate gate
        await _jup_gate()

        # DO NOT send asLegacyTransaction to /quote
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_iu),
            "slippageBps": sbps,
            "swapMode": swap_mode,
            "onlyDirectRoutes": "true" if only_direct else "false",
        }
        js = await _get_json(JUP_QUOTE_URL, params=params, timeout=20)
        budget.after_quote()
        await _set_cached_quote(key, js, QUOTE_TTL_SEC)
        return js

    return await _singleflight_quote(key, _fetch)

async def _jup_swap(user_pubkey: str, quote: dict, *, priority_lamports: Optional[int]) -> str:
    # Respect global rate gate for swap build, too
    await _jup_gate()

    body = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "quoteResponse": quote,
        # asLegacy belongs here, not on /quote
        "asLegacyTransaction": True if _as_legacy() else False,
    }
    if priority_lamports is not None:
        body["prioritizationFeeLamports"] = int(priority_lamports)

    js = await _post_json(JUP_SWAP_URL, json_body=body, timeout=25)
    tx_b64 = js.get("swapTransaction")
    if not tx_b64:
        raise RuntimeError(f"Jupiter returned no swapTransaction: {js}")
    return tx_b64

# =============================================================================
# Quotes & price helpers (public)
# =============================================================================
async def get_sell_quote(*, mint: str, amount_raw: int, slippage_bps: Optional[int] = None) -> Dict[str, Any]:
    if amount_raw <= 0:
        raise ValueError("amount_raw must be > 0")
    quote = await _retry(
        _jup_quote, mint, SOL_MINT, amount_raw,
        tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-sell",
        slippage_bps=slippage_bps,
    )
    lamports_out = int(quote.get("outAmount", "0"))
    return {
        **quote,
        "lamports_out": lamports_out,
        "out_amount_sol": lamports_out / 1_000_000_000.0,
    }

async def get_buy_quote(
    *,
    mint: str,
    lamports_in: Optional[int] = None,
    amount_sol: Optional[float] = None,
    slippage_bps: Optional[int] = None,
) -> Dict[str, Any]:
    if lamports_in is None and amount_sol is None:
        pf = os.getenv("PF_TRADE_SOL")
        if pf:
            amount_sol = _envf("PF_TRADE_SOL", 0.0)
        else:
            raise ValueError("Provide lamports_in or amount_sol")

    if lamports_in is None:
        lamports_in = int((amount_sol or 0.0) * 1_000_000_000)

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
    # price cache
    if PRICE_TTL_SEC > 0:
        rec = _PRICE_CACHE.get(mint)
        if rec and rec[0] >= time.time():
            return float(rec[1])

    # Only compute price if we know decimals; otherwise return 0.0 (prevents 1 SOL fallback storms)
    dec = await _get_decimals(mint)
    if dec is None or dec < 0:
        return 0.0

    tiny_raw = max(1, 10 ** max(0, dec - 3))
    try:
        q = await _retry(
            _jup_quote, mint, SOL_MINT, tiny_raw,
            tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-price",
        )
        lamports = int(q.get("outAmount", "0"))
        if lamports <= 0:
            return 0.0
        sol_out = lamports / 1_000_000_000.0
        tokens_ui = tiny_raw / (10 ** dec)
        px = sol_out / tokens_ui if tokens_ui > 0 else 0.0
        if PRICE_TTL_SEC > 0:
            _PRICE_CACHE[mint] = (time.time() + PRICE_TTL_SEC, px)
        return px
    except Exception:
        return 0.0

# Backward-compat aliases
async def price_of_ui(mint: str) -> float:  # alias
    return await get_price_ui(mint)

async def price_of(mint: str) -> float:  # alias
    return await get_price_ui(mint)

# =============================================================================
# Swaps (public) — return (ok, signature | None)
# =============================================================================
async def buy_via_lightning(
    *,
    mint: str,
    amount_sol: Optional[float] = None,
    lamports_in: Optional[int] = None,
    slippage_bps: Optional[int] = None,
    priority_sol: Optional[float] = None,
) -> Tuple[bool, Optional[str]]:
    kp = load_keypair()
    owner = get_pubkey_str(kp)

    if lamports_in is None:
        if amount_sol is None:
            amount_sol = _envf("PF_TRADE_SOL", 0.0)
        lamports_in = int(amount_sol * 1_000_000_000)

    if lamports_in <= 0:
        return False, None

    # Quote with retries (budget-aware via _jup_quote)
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

    # Swap with retries; bump priority between attempts
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
            _PRIO._tune(_PRIO.target_sec * 1.6, had_error=True)  # nudge up
            await asyncio.sleep(RETRY_BACKOFF * attempt)

async def sell_via_lightning(
    *,
    mint: str,
    token_program: Optional[str],
    amount_raw: int,
    slippage_bps: Optional[int] = None,
    priority_sol: Optional[float] = None,
) -> Tuple[bool, Optional[str]]:
    del token_program  # not needed by Jupiter

    if amount_raw <= 0:
        return False, None

    kp = load_keypair()
    owner = get_pubkey_str(kp)

    # Quote with retries (budget-aware via _jup_quote)
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
        log.event("DRYRUN_SELL", mint=mint, amount_raw=amount_raw, out_sol=lamports / 1e9)
        return True, None

    # Swap with retries; bump priority between attempts
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
            await asyncio.sleep(RETRY_BACKOFF * attempt)

__all__ = [
    "start",
    "get_sell_quote",
    "get_buy_quote",
    "get_price_ui",
    "price_of_ui",
    "price_of",
    "buy_via_lightning",
    "sell_via_lightning",
]
