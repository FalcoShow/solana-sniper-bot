# utils_pkg/router.py
from __future__ import annotations

import asyncio
import os
import time
import random
import contextlib
from typing import Any, Dict, Optional, Tuple, Callable

import requests

from logging_setup import get_logger
from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)
from utils_pkg.solana_token import fetch_mint_info
from utils_pkg.txlearn import _Rpc, TX_RPC_URL, TX_COMMITMENT  # keep import for compatibility

# ---------------------------------------------------------------------------
# Optional budget hooks (soft gates). Fail open if module is missing.
# ---------------------------------------------------------------------------
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # graceful no-ops
    class _BudgetNoop:
        def before_quote(self, units: int = 1) -> bool: return True
        def after_quote(self, units: int = 1) -> None:  return None
        def before_rpc(self, method: str, *, weight: Optional[int] = None) -> bool: return True
        def after_rpc(self, method: str, *, weight: Optional[int] = None) -> None:  return None
    budget = _BudgetNoop()  # type: ignore

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

# Slippage: support BOTH env styles → prefer explicit BPS if present
def _slippage_bps() -> int:
    try:
        bps_env = os.getenv("PF_SLIPPAGE_BPS")
        if bps_env is not None:
            bps = int(bps_env)
            return max(1, bps)
    except Exception:
        pass
    # Fallback: JUPITER_MAX_SLIPPAGE as fraction (e.g., 0.0125 => 125 bps)
    s = _envf("JUPITER_MAX_SLIPPAGE", 0.0125)
    return max(1, int(round(s * 10_000)))

def _as_legacy() -> bool:
    v = os.getenv("JUP_AS_LEGACY", "1")
    return str(v).strip().lower() in {"1", "true", "yes", "y"}

# Router retry knobs
QUOTE_RETRIES = max(1, _envi("ROUTER_QUOTE_RETRIES", 2))
SWAP_RETRIES  = max(1, _envi("ROUTER_SWAP_RETRIES", 2))
RETRY_BACKOFF = max(0.05, _envf("ROUTER_RETRY_BACKOFF_SEC", 0.35))

# Global HTTP throttling (helps avoid Jupiter 429)
ROUTER_HTTP_MAX_CONCURRENCY = max(1, _envi("ROUTER_HTTP_MAX_CONCURRENCY", 4))
JUP_MIN_INTERVAL_MS = max(0, _envi("JUP_MIN_INTERVAL_MS", 120))   # ~8 QPS by default
JUP_JITTER_MS       = max(0, _envi("JUP_JITTER_MS", 40))          # add small jitter

_http_sema = asyncio.Semaphore(ROUTER_HTTP_MAX_CONCURRENCY)
_rate_lock = asyncio.Lock()
_LAST_HTTP_AT = 0.0

# Caching / single-flight to cut quote storms
QUOTE_TTL_SEC = max(0.0, _envf("ROUTER_QUOTE_TTL_SEC", 2.0))
PRICE_TTL_SEC = max(0.0, _envf("PF_PRICE_TTL_SEC", 5.0))  # shared with other modules
_QUOTE_CACHE: Dict[str, Tuple[float, dict]] = {}          # key -> (expiry_ts, payload)
_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}         # mint -> (expiry_ts, price_ui)
_QUOTE_FLIGHT: Dict[str, asyncio.Future[Any]] = {}        # key -> future
_quote_lock = asyncio.Lock()

# QuickNode priority hint (optional)
QN_PRIORITY_HINT   = _envb("QN_PRIORITY_HINT", False)
QN_RPC_URL         = (os.getenv("SOL_RPC_URL") or "").strip()
QN_HINT_TIMEOUT_S  = max(0.8, _envf("QN_PRIORITY_TIMEOUT_SEC", 2.5))

# Signature status poll cadence
STATUS_POLL_SEC    = max(0.2, _envf("PF_STATUS_POLL_SEC", 0.9))

# Common HTTP headers
_UA = {"user-agent": "pnlsniper/1.0 (+router)"}

# =============================================================================
# Throttled, non-blocking HTTP helpers (requests in worker thread)
# =============================================================================
async def _throttle_http() -> None:
    """Simple per-process min-interval gate + jitter to reduce 429s."""
    global _LAST_HTTP_AT
    min_interval = JUP_MIN_INTERVAL_MS / 1000.0
    jitter = (random.uniform(0, JUP_JITTER_MS) / 1000.0) if JUP_JITTER_MS > 0 else 0.0
    async with _rate_lock:
        now = time.time()
        wait = (min_interval + jitter) - (now - _LAST_HTTP_AT)
        if wait > 0:
            await asyncio.sleep(wait)
        _LAST_HTTP_AT = time.time()

async def _get_json(url: str, *, params: Optional[dict] = None, timeout: int = 20) -> dict:
    await _throttle_http()
    async with _http_sema:
        def _req():
            r = requests.get(url, params=params, timeout=timeout, headers=_UA)
            r.raise_for_status()
            return r.json()
        return await asyncio.to_thread(_req)

async def _post_json(url: str, *, json_body: dict, timeout: int = 25) -> dict:
    await _throttle_http()
    async with _http_sema:
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
            if i < tries:
                # log first failure at info level to reduce noise
                if i == 1:
                    log.event("ROUTER_RETRY_WARN", label=label, err=str(e))
                await asyncio.sleep(backoff * i)
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
# Dynamic priority fee controller (with optional QuickNode seed)
# =============================================================================
def _to_sol_from_lamports_like(x: float) -> float:
    """
    Accept lamports OR micro-lamports (some RPCs return micro-lamports for priority).
    Heuristic:
      - If >= 1e9, assume micro-lamports and divide by 1e6 first.
      - Then convert lamports -> SOL.
    """
    lamports = float(x)
    if lamports >= 1e9:
        lamports = lamports / 1e6  # micro-lamports -> lamports
    return lamports / 1e9

async def _seed_priority_from_quicknode() -> Optional[float]:
    """Best-effort fetch of a sane network priority baseline from QuickNode."""
    if not (QN_PRIORITY_HINT and QN_RPC_URL):
        return None

    def _call_qn() -> Optional[float]:
        payloads = [
            {"jsonrpc": "2.0", "id": 1, "method": "qn_getPriorityFee", "params": []},
            {"jsonrpc": "2.0", "id": 1, "method": "getRecentPrioritizationFees", "params": []},
        ]
        for p in payloads:
            try:
                mname = p["method"]
                if not budget.before_rpc(mname, weight=1):
                    continue
                r = requests.post(QN_RPC_URL, json=p, timeout=QN_HINT_TIMEOUT_S, headers=_UA)
                js = r.json()
                budget.after_rpc(mname, weight=1)
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

    return await asyncio.to_thread(_call_qn)

class DynPriority:
    """
    Adaptive controller:
      - Aim for confirmations around target_sec.
      - If recent inclusion >> target → step up fee
      - If recent inclusion << target → step down fee
      - Clamp between [min_sol, max_sol]
      - Optionally seed from QuickNode priority hint on start().
    """
    def __init__(self) -> None:
        base = _envf("PF_PRIORITY_SOL", 0.002)
        self.enabled        = _envb("PF_PRIORITY_DYNAMIC", True)
        self.target_sec     = _envf("PF_PRIORITY_TARGET_SEC", 2.0)
        self.step_up        = _envf("PF_PRIORITY_STEP_UP", 1.50)   # multiply
        self.step_down      = _envf("PF_PRIORITY_STEP_DOWN", 0.85) # multiply
        self.min_sol        = _envf("PF_PRIORITY_MIN_SOL", max(0.0, base * 0.5))
        self.max_sol        = _envf("PF_PRIORITY_MAX_SOL", max(self.min_sol, base * 5))
        self.current_sol    = max(self.min_sol, min(self.max_sol, base))
        self._pending: Dict[str, float] = {}  # sig -> t0
        self._rpc = _Rpc(TX_RPC_URL or "")
        self._seed_task: Optional[asyncio.Task[None]] = None  # created on start()

    async def start(self) -> None:
        """Call once after the event loop is running to optionally seed from QuickNode."""
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
            seed = await _seed_priority_from_quicknode()
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
        # poll for confirmation/error up to 45s
        deadline = time.time() + 45
        while time.time() < deadline:
            try:
                st = await self._rpc.get_signature_status(sig)
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
            await asyncio.sleep(STATUS_POLL_SEC)
        # timeout — nudge up a bit
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

# module-level instance (safe: no tasks scheduled here)
_PRIO = DynPriority()

# Public hooks to start/stop optional background seeding
async def start() -> None:
    await _PRIO.start()

async def shutdown() -> None:
    # No open aiohttp sessions here; clear caches and stop seed task if running
    with contextlib.suppress(Exception):
        _QUOTE_CACHE.clear()
        _PRICE_CACHE.clear()
    t = getattr(_PRIO, "_seed_task", None)
    if t and not t.done():
        t.cancel()
        with contextlib.suppress(Exception):
            await t

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
# Low-level Jupiter helpers (async) with budget hooks + cache
# =============================================================================
async def _jup_quote(
    input_mint: str,
    output_mint: str,
    amount_iu: int,
    *,
    slippage_bps: Optional[int] = None,
    swap_mode: str = "ExactIn",
    only_direct: bool = False,
    as_legacy: Optional[bool] = None,
) -> dict:
    if amount_iu <= 0:
        raise ValueError("amount_iu must be > 0")

    sbps = int(slippage_bps if slippage_bps is not None else _slippage_bps())
    legacy = True if (as_legacy if as_legacy is not None else _as_legacy()) else False
    key = _qkey(input_mint, output_mint, amount_iu, sbps, swap_mode, only_direct, legacy)

    # 1) cache hit?
    cached = await _get_cached_quote(key)
    if cached is not None:
        return cached

    # 2) single-flight + budget gate
    async def _fetch():
        if not budget.before_quote():
            raise RuntimeError("quote blocked by budget")
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount_iu),
            "slippageBps": sbps,
            "swapMode": swap_mode,
            "onlyDirectRoutes": "true" if only_direct else "false",
            "asLegacyTransaction": "true" if legacy else "false",
        }
        js = await _get_json(JUP_QUOTE_URL, params=params, timeout=20)
        budget.after_quote()
        await _set_cached_quote(key, js, QUOTE_TTL_SEC)
        return js

    return await _singleflight_quote(key, _fetch)

async def _jup_swap(user_pubkey: str, quote: dict, *, priority_lamports: Optional[int]) -> str:
    body = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "quoteResponse": quote,
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

    dec = await _get_decimals(mint)
    if dec is None:
        # try a 1 SOL buy-quote to infer decimals if missing
        try:
            q = await get_buy_quote(mint=mint, amount_sol=1.0)
            out_raw = int(q.get("outAmount", "0"))
            if out_raw > 0 and q.get("decimals") is not None:
                dec = int(q["decimals"])
                tokens_ui = out_raw / (10 ** dec)
                px = 1.0 / tokens_ui if tokens_ui > 0 else 0.0
                if PRICE_TTL_SEC > 0:
                    _PRICE_CACHE[mint] = (time.time() + PRICE_TTL_SEC, px)
                return px
        except Exception:
            return 0.0

    tiny_raw = max(1, 10 ** max(0, (dec or 0) - 3))
    try:
        q = await _retry(
            _jup_quote, mint, SOL_MINT, tiny_raw,
            tries=QUOTE_RETRIES, backoff=RETRY_BACKOFF, label="quote-price",
        )
        lamports = int(q.get("outAmount", "0"))
        if lamports <= 0:
            return 0.0
        sol_out = lamports / 1_000_000_000.0
        tokens_ui = tiny_raw / (10 ** (dec or 0))
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
    "shutdown",
    "get_sell_quote",
    "get_buy_quote",
    "get_price_ui",
    "price_of_ui",
    "price_of",
    "buy_via_lightning",
    "sell_via_lightning",
]
