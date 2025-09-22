from __future__ import annotations

import asyncio
import contextlib
import os
import re
import traceback
from typing import Any, Dict, Optional

from logging_setup import get_logger, bump
from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.pump_trader import maybe_buy
from utils_pkg.trader import sell_on_jupiter, exit_engine, IS_DRYRUN

# ──────────────────────────────────────────────────────────────────────────────
# Optional helpers (graceful fallbacks)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg.dedupe import claim_once  # (key:str, ttl:int) -> bool
except Exception:
    def claim_once(key: str, ttl: int = 3600) -> bool:  # type: ignore
        return True

try:
    from utils_pkg.guardrails import buys_allowed, record_buy_result  # type: ignore
except Exception:
    def buys_allowed() -> bool:  # type: ignore
        return True
    def record_buy_result(*_args, **_kwargs):  # type: ignore
        return None

try:
    from utils_pkg.positions import all as all_positions
except Exception:
    def all_positions():  # type: ignore
        return []

try:
    from scripts.sell_engine import run_sell_loop
except Exception:
    async def run_sell_loop(stop_evt: asyncio.Event):  # type: ignore
        while not stop_evt.is_set():
            await asyncio.sleep(5)

# Router pieces (price helper)
try:
    from utils_pkg import router
    from utils_pkg.router import get_price_ui as _route_price_ui
except Exception:
    router = None  # type: ignore
    async def _route_price_ui(_mint: str) -> float:  # type: ignore
        return 0.0

# Liquidity screen (fallback provided)
try:
    from utils_pkg.liquidity import looks_like_mint as _looks_like_mint, screen_token
except Exception:
    _looks_like_mint = None  # type: ignore
    async def screen_token(_mint: str):  # type: ignore
        return True, {"ok": True}

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Mint normalization / validation
# ──────────────────────────────────────────────────────────────────────────────
# Base58 alphabet (no 0,O,I,l) – 32–44 chars typical for Solana mints
_BASE58_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")
_ENDS_WITH_PUMP_OR_BONK = re.compile(r"(pump|bonk)$", re.IGNORECASE)

def normalize_mint(maybe: str) -> str:
    """Extract the first token-like base58 substring (handles '...pump'/'...bonk' tails)."""
    if not maybe:
        return ""
    m = _BASE58_RE.search(str(maybe))
    return m.group(0) if m else str(maybe).strip()

def is_probable_mint(s: str) -> bool:
    """True if it looks like a Solana mint; uses utils if present + regex fallback."""
    try:
        if _looks_like_mint is not None and _looks_like_mint(s):  # type: ignore
            return True
    except Exception:
        pass
    return bool(_BASE58_RE.fullmatch(s or ""))

# ──────────────────────────────────────────────────────────────────────────────
# env helpers
# ──────────────────────────────────────────────────────────────────────────────
def _as_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "y", "on"}

def _as_float(val: Any, default: float) -> float:
    try:
        return float(val)
    except Exception:
        return default

def _as_int(val: Any, default: int) -> int:
    try:
        return int(val)
    except Exception:
        return default

# ──────────────────────────────────────────────────────────────────────────────
# config
# ──────────────────────────────────────────────────────────────────────────────
EXIT_ENGINE_ENABLED      = _as_bool(os.getenv("EXIT_ENGINE_ENABLED", "1"), True)
DEDUPE_ENABLED           = _as_bool(os.getenv("PF_DEDUPE_ENABLED", "1"), True)
DEDUPE_TTL_SEC           = _as_int(os.getenv("PF_DEDUPE_TTL_SEC", "3600"), 3600)
LIQ_FILTER_ENABLED       = _as_bool(os.getenv("ENTRY_LIQ_FILTER", "1"), True)

# Momentum confirmation gate (prevents DOA entries)
ENTRY_MOMENTUM_ENABLED   = _as_bool(os.getenv("ENTRY_MOMENTUM_ENABLED", "1"), True)
ENTRY_MOMO_WINDOW_SEC    = _as_int(os.getenv("ENTRY_MOMO_WINDOW_SEC", "10"), 10)
ENTRY_MOMO_MIN_TRADES    = _as_int(os.getenv("ENTRY_MOMO_MIN_TRADES", "6"), 6)
ENTRY_MOMO_MIN_UP_PCT    = _as_float(os.getenv("ENTRY_MOMO_MIN_UP_PCT", "5"), 5.0)
ENTRY_MOMO_PRICE_TRIES   = _as_int(os.getenv("ENTRY_MOMO_PRICE_TRIES", "4"), 4)
ENTRY_MOMO_PRICE_DELAY_S = _as_float(os.getenv("ENTRY_MOMO_PRICE_DELAY_S", "0.7"), 0.7)
ENTRY_MOMO_MAX_PARALLEL  = max(1, _as_int(os.getenv("ENTRY_MOMO_MAX_PARALLEL", "6"), 6))

# Entry filters
MIN_INITIAL_BUY_SOL      = _as_float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"), 0.0)
MAX_NAME_LEN             = _as_int(os.getenv("ENTRY_MAX_NAME_LEN", "40"), 40)
ENTRY_MAX_AGE_MIN        = _as_int(os.getenv("ENTRY_MAX_AGE_MIN", "20"), 20)   # skip if token older than this (if age available)
MIN_POOL_AGE_MIN         = _as_int(os.getenv("MIN_POOL_AGE_MIN", "3"), 3)      # require pool >= this age (if provided)

SYMBOL_BLACKLIST         = {
    s.strip().upper()
    for s in os.getenv("ENTRY_SYMBOL_BLACKLIST", "RUG,SCAM").split(",")
    if s.strip()
}

# Concurrency / pacing
MAX_CONCURRENCY          = max(1, _as_int(os.getenv("PF_MAX_CONCURRENT", "2"), 2))
LOOP_COOLDOWN_SEC        = _as_float(os.getenv("PF_COOLDOWN_SEC", "15"), 15.0)

# Portfolio caps (avoid over-fanout)
MAX_TRADEABLE            = _as_int(os.getenv("MAX_TRADEABLE", "0"), 0)  # 0 = unlimited

# Pause toggle (quick kill switch)
PAUSE_TRADING            = _as_bool(os.getenv("PAUSE_TRADING", "0"), False)

# Debug
DEBUG_TX                 = _as_bool(os.getenv("PF_DEBUG_TX", "0"), False)

# ──────────────────────────────────────────────────────────────────────────────
# small utils
# ──────────────────────────────────────────────────────────────────────────────
def _get_first(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def _extract_raw_mint(msg: Dict[str, Any]) -> str:
    raw = _get_first(
        msg,
        "mint", "mintAddress", "mint_address", "bondingCurve", "bonding_curve",
        "address", "id",
        default=""
    )
    raw = ("" if raw is None else str(raw)).strip()
    print(f"[main] raw pump id = {raw}", flush=True)
    return raw

def _extract_symbol(msg: Dict[str, Any]) -> str:
    return str(_get_first(msg, "symbol", "ticker", default="")).strip()

def _extract_name(msg: Dict[str, Any]) -> str:
    return str(_get_first(msg, "name", default="")).strip()

def _approx_minutes_since(ts_any: Any) -> Optional[float]:
    """best-effort: accept seconds epoch, ms epoch, or ISO-like. return minutes or None."""
    try:
        import datetime as _dt
        now = _dt.datetime.utcnow().timestamp()
        if ts_any is None:
            return None
        # int/float epoch?
        if isinstance(ts_any, (int, float)):
            t = float(ts_any)
            if t > 10_000_000_000:  # ms epoch
                t /= 1000.0
            return max(0.0, (now - t) / 60.0)
        # string?
        if isinstance(ts_any, str):
            s = ts_any.strip()
            if not s:
                return None
            try:
                f = float(s)
                if f > 10_000_000_000:
                    f /= 1000.0
                return max(0.0, (_dt.datetime.utcnow().timestamp() - f) / 60.0)
            except Exception:
                pass
            try:
                dt = _dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
                return max(0.0, (_dt.datetime.utcnow().timestamp() - dt.timestamp()) / 60.0)
            except Exception:
                return None
    except Exception:
        return None
    return None

def _passes_basic_filters(msg: Dict[str, Any]) -> bool:
    typ = str(_get_first(msg, "txType", "type", default="")).strip().lower()
    if typ != "create":
        return False

    # initial buy gate (fresh pump size)
    init_buy = _as_float(_get_first(msg, "initialBuy", "initial_buy", default=0.0), 0.0)
    if init_buy < MIN_INITIAL_BUY_SOL:
        if DEBUG_TX:
            print(f"[filter] initialBuy {init_buy} < min {MIN_INITIAL_BUY_SOL} — skip", flush=True)
        return False

    # symbol blacklist
    sym = _extract_symbol(msg).upper()
    if any(bad and bad in sym for bad in SYMBOL_BLACKLIST):
        if DEBUG_TX:
            print(f"[filter] symbol '{sym}' hits blacklist {SYMBOL_BLACKLIST} — skip", flush=True)
        return False

    # name length
    nm = _extract_name(msg)
    if MAX_NAME_LEN > 0 and len(nm) > MAX_NAME_LEN:
        if DEBUG_TX:
            print(f"[filter] name too long ({len(nm)} > {MAX_NAME_LEN}) — skip", flush=True)
        return False

    # age gating (best-effort; only applied if event exposes timestamps)
    mins_since_token = _approx_minutes_since(_get_first(msg, "created_at", "createdAt", "timestamp", "ts", default=None))
    if mins_since_token is not None and ENTRY_MAX_AGE_MIN > 0 and mins_since_token > ENTRY_MAX_AGE_MIN:
        if DEBUG_TX:
            print(f"[filter] token age {mins_since_token:.1f}m > max {ENTRY_MAX_AGE_MIN}m — skip", flush=True)
        return False

    mins_since_pool = _approx_minutes_since(_get_first(msg, "poolCreatedAt", "pool_created_at", default=None))
    if mins_since_pool is not None and MIN_POOL_AGE_MIN > 0 and mins_since_pool < MIN_POOL_AGE_MIN:
        if DEBUG_TX:
            print(f"[filter] pool age {mins_since_pool:.1f}m < min {MIN_POOL_AGE_MIN}m — skip", flush=True)
        return False

    return True

# ──────────────────────────────────────────────────────────────────────────────
# optional post-buy immediate test
# ──────────────────────────────────────────────────────────────────────────────
async def _post_buy_exit_test(mint_norm: str, tx_sig: str) -> None:
    if str(os.getenv("POSTBUY_MODE", "")).strip().lower() != "immediate_sell_test":
        return
    await asyncio.sleep(5)
    try:
        await sell_on_jupiter(mint_norm, pct_of_wallet=1.0)
    except Exception:
        if DEBUG_TX:
            traceback.print_exc()

# ──────────────────────────────────────────────────────────────────────────────
# strategy with momentum gate
# ──────────────────────────────────────────────────────────────────────────────
class Strategy:
    def __init__(self) -> None:
        self._sema = asyncio.Semaphore(MAX_CONCURRENCY)
        self._gate_sema = asyncio.Semaphore(ENTRY_MOMO_MAX_PARALLEL)
        # gate_state: **key by raw id** to match incoming trade ticks exactly
        self._gate_state: Dict[str, Dict[str, Any]] = {}

    # --- trade tick feed for momentum counters
    async def on_trade(self, raw_event: Dict[str, Any]) -> None:
        if EXIT_ENGINE_ENABLED:
            try:
                await exit_engine.on_trade_tick(raw_event)
            except Exception:
                if DEBUG_TX:
                    traceback.print_exc()

        m = str(_get_first(raw_event, "mint", "mintAddress", "mint_address", "id", default="")).strip()
        if not m:
            return
        st = self._gate_state.get(m)
        if st is not None:
            st["trades"] = int(st.get("trades", 0)) + 1

    # --- momentum gate worker
    async def _gate_candidate(self, ui: Dict[str, Any], mint_raw: str, mint_norm: str) -> None:
        async with self._gate_sema:
            # Take a local claim so only one dyno runs the gate
            if DEDUPE_ENABLED:
                if not claim_once(f"gate:{mint_raw}", ttl=max(ENTRY_MOMO_WINDOW_SEC * 2, 60)):
                    if DEBUG_TX:
                        print(f"[dedupe] gate already running for {mint_raw} — skip", flush=True)
                    return

            # init state keyed by **raw**
            self._gate_state[mint_raw] = {"trades": 0, "price0": 0.0}

            # Try a few times to get an initial price (mint may be too fresh)
            price0 = 0.0
            if is_probable_mint(mint_norm):
                for _ in range(max(1, ENTRY_MOMO_PRICE_TRIES)):
                    try:
                        price0 = await _route_price_ui(mint_norm)
                    except Exception:
                        price0 = 0.0
                    if price0 > 0:
                        break
                    await asyncio.sleep(ENTRY_MOMO_PRICE_DELAY_S)
            self._gate_state[mint_raw]["price0"] = price0

            # wait the momentum window
            await asyncio.sleep(max(1, ENTRY_MOMO_WINDOW_SEC))

            # snapshot counters and reprice
            st = self._gate_state.get(mint_raw, {})
            trades = int(st.get("trades", 0))
            price1 = 0.0
            if is_probable_mint(mint_norm):
                try:
                    price1 = await _route_price_ui(mint_norm)
                except Exception:
                    price1 = 0.0

            up_ok = True
            if price0 > 0 and price1 > 0:
                up_pct = ((price1 / price0) - 1.0) * 100.0
                up_ok = up_pct >= ENTRY_MOMO_MIN_UP_PCT
            trades_ok = trades >= ENTRY_MOMO_MIN_TRADES

            log.event(
                "MOMO_EVAL",
                mint=mint_raw,
                trades=trades,
                price0=price0,
                price1=price1,
                min_trades=ENTRY_MOMO_MIN_TRADES,
                min_up_pct=ENTRY_MOMO_MIN_UP_PCT,
                up_ok=int(up_ok),
                trades_ok=int(trades_ok),
            )

            # cleanup state
            self._gate_state.pop(mint_raw, None)

            if not buys_allowed() or PAUSE_TRADING:
                if DEBUG_TX:
                    print("[guardrails] buys not allowed at gate decision — skip", flush=True)
                record_buy_result(ok=False, reason="guardrails_halt")
                return

            # portfolio cap check here (fast-fail before we spend credits downstream)
            if MAX_TRADEABLE > 0:
                try:
                    if len(list(all_positions())) >= MAX_TRADEABLE:
                        log.event("BUY_BLOCK_MAX_TRADEABLE", cap=MAX_TRADEABLE)
                        record_buy_result(ok=False, reason="max_tradeable")
                        return
                except Exception:
                    pass

            if trades_ok and up_ok:
                # final buy claim (so only one dyno executes)
                if DEDUPE_ENABLED and not claim_once(f"buy:{mint_raw}", ttl=600):
                    if DEBUG_TX:
                        print(f"[dedupe] buy already claimed for {mint_raw} — skip", flush=True)
                    record_buy_result(ok=False, reason="buy_claimed_elsewhere")
                    return

                # Execute buy using **normalized** mint behind the concurrency semaphore
                try:
                    async with self._sema:
                        tx_sig = await maybe_buy(mint=mint_norm, ui=ui)
                    if tx_sig:
                        record_buy_result(ok=True, reason="executed", sig=tx_sig)
                        if EXIT_ENGINE_ENABLED and not IS_DRYRUN:
                            asyncio.create_task(
                                exit_engine.on_buy_filled(raw_id_or_mint=mint_norm, tx_sig=tx_sig)
                            )
                        asyncio.create_task(_post_buy_exit_test(mint_norm, tx_sig))
                    else:
                        record_buy_result(ok=False, reason="no_tx_sig")
                except Exception:
                    traceback.print_exc()
                    record_buy_result(ok=False, reason="exception")
            else:
                record_buy_result(ok=False, reason="momo_reject")

    # --- main new-token handler
    async def on_new_token(self, raw_event: Dict[str, Any]) -> None:
        # Global halt?
        if PAUSE_TRADING or not buys_allowed():
            if DEBUG_TX:
                print("[guardrails] buys not allowed now — skip", flush=True)
            return

        # portfolio cap check (cheap)
        if MAX_TRADEABLE > 0:
            try:
                if len(list(all_positions())) >= MAX_TRADEABLE:
                    log.event("BUY_BLOCK_MAX_TRADEABLE", cap=MAX_TRADEABLE)
                    return
            except Exception:
                pass

        mint_raw = _extract_raw_mint(raw_event)
        if not mint_raw:
            if DEBUG_TX:
                print("[strategy] missing mint — skip", flush=True)
            return

        # Require a true 'mint' from the event when raw ends with pump/bonk to avoid bogus curve ids
        real_mint_from_event = str(_get_first(raw_event, "mint", "mintAddress", "mint_address", default="")).strip()
        if not real_mint_from_event and _ENDS_WITH_PUMP_OR_BONK.search(mint_raw or ""):
            if DEBUG_TX:
                print(f"[strategy] bonding-curve id with no mint => skip ({mint_raw})", flush=True)
            return

        # Normalize for price/liquidity/routing
        mint_norm = normalize_mint(real_mint_from_event or mint_raw)
        if mint_norm != mint_raw:
            print(f"[main] normalized mint = {mint_norm}", flush=True)

        if not _passes_basic_filters(raw_event):
            return

        # Liquidity + safety screen – use normalized mint
        if LIQ_FILTER_ENABLED and is_probable_mint(mint_norm):
            ok, rep = await screen_token(mint_norm)
            if not ok:
                if DEBUG_TX:
                    print(f"[liquidity] screen failed for {mint_norm}: {rep}", flush=True)
                return
            log.event(
                "ENTRY_LIQ_OK",
                mint=mint_norm,
                priceImpactPct=rep.get("priceImpactPct"),
                dec=rep.get("decimals"),
            )

        # Build UI context for trader; include both forms
        ui: Dict[str, Any] = dict(raw_event)
        ui.setdefault("pool", "pump")
        ui.setdefault("symbol", _extract_symbol(raw_event))
        ui.setdefault("name", _extract_name(raw_event))
        ui["rawMint"] = mint_raw
        ui["mint"] = mint_norm
        ui["normalized"] = (mint_norm != mint_raw)

        # Legacy-style noisy logs (kept) – show **raw** as it appears on the wire
        symbol = ui.get("symbol", "") or "?"
        print(f"[trade] pool=pump symbol={symbol} mint={mint_raw} curve=-", flush=True)
        print(f"[trade] pool=pump symbol={symbol} mint={mint_raw} curve=- for {symbol} ({mint_raw})", flush=True)

        # Dedupe gating/buying across dynos keyed by **raw** id
        if DEDUPE_ENABLED and ENTRY_MOMENTUM_ENABLED:
            if not claim_once(f"gate-propose:{mint_raw}", ttl=max(ENTRY_MOMO_WINDOW_SEC * 2, 90)):
                if DEBUG_TX:
                    print(f"[dedupe] gate-propose taken for {mint_raw} — skip", flush=True)
                return

        # Momentum gate or immediate buy
        if ENTRY_MOMENTUM_ENABLED:
            asyncio.create_task(self._gate_candidate(ui, mint_raw, mint_norm))
        else:
            # Immediate path (legacy behavior + dedupe)
            if DEDUPE_ENABLED and not claim_once(f"buy:{mint_raw}", ttl=600):
                if DEBUG_TX:
                    print(f"[dedupe] buy already claimed for {mint_raw} — skip", flush=True)
                return
            async with self._sema:
                try:
                    if PAUSE_TRADING or not buys_allowed():  # re-check at commit
                        if DEBUG_TX:
                            print("[guardrails] buys not allowed at execution — skip", flush=True)
                        return
                    # cap check again right before spending credits
                    if MAX_TRADEABLE > 0:
                        try:
                            if len(list(all_positions())) >= MAX_TRADEABLE:
                                log.event("BUY_BLOCK_MAX_TRADEABLE", cap=MAX_TRADEABLE)
                                return
                        except Exception:
                            pass

                    # BUY with **normalized** mint
                    tx_sig = await maybe_buy(mint=mint_norm, ui=ui)
                    if not tx_sig:
                        record_buy_result(ok=False, reason="no_tx_sig")
                        return
                    record_buy_result(ok=True, reason="executed", sig=tx_sig)

                    if EXIT_ENGINE_ENABLED and not IS_DRYRUN:
                        asyncio.create_task(
                            exit_engine.on_buy_filled(raw_id_or_mint=mint_norm, tx_sig=tx_sig)
                        )
                    asyncio.create_task(_post_buy_exit_test(mint_norm, tx_sig))
                except Exception:
                    print("[strategy] maybe_buy exception:", flush=True)
                    traceback.print_exc()
                    record_buy_result(ok=False, reason="exception")

        await asyncio.sleep(LOOP_COOLDOWN_SEC)

# ──────────────────────────────────────────────────────────────────────────────
# stream loop & main
# ──────────────────────────────────────────────────────────────────────────────
async def stream_loop(stop_evt: Optional[asyncio.Event] = None) -> None:
    # Prefer async context manager if the client supports it (prevents aiohttp leaks)
    if hasattr(PumpPortalClient, "__aenter__"):
        async with PumpPortalClient() as client:  # type: ignore[func-returns-value]
            strat = Strategy()
            print("[main] Connecting:", client.masked_uri(), flush=True)
            async for ev in client.events():
                if stop_evt and stop_evt.is_set():
                    break
                try:
                    kind = ev.get("kind")
                    raw = ev.get("raw", {}) or {}
                    if kind == "new_token":
                        await strat.on_new_token(raw)
                    elif kind == "trade":
                        await strat.on_trade(raw)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    print("[main] event handling error:", flush=True)
                    traceback.print_exc()
    else:
        client = PumpPortalClient()
        strat = Strategy()
        print("[main] Connecting:", client.masked_uri(), flush=True)
        try:
            async for ev in client.events():
                if stop_evt and stop_evt.is_set():
                    break
                try:
                    kind = ev.get("kind")
                    raw = ev.get("raw", {}) or {}
                    if kind == "new_token":
                        await strat.on_new_token(raw)
                    elif kind == "trade":
                        await strat.on_trade(raw)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    print("[main] event handling error:", flush=True)
                    traceback.print_exc()
        finally:
            if hasattr(client, "close"):
                with contextlib.suppress(Exception):
                    maybe_awaitable = client.close()
                    if asyncio.iscoroutine(maybe_awaitable):
                        await maybe_awaitable

async def main(stop_evt: Optional[asyncio.Event] = None) -> None:
    # Touch a startup heartbeat (optional; keeps your logger pattern intact)
    with contextlib.suppress(Exception):
        bump("boot")

    # Initialize dynamic priority (safe if router is missing; idempotent)
    try:
        if router and hasattr(router, "start"):
            await router.start()
            log.event("ROUTER_PRIO_INIT_STARTED", ok=1)
    except Exception as e:
        log.event("ROUTER_PRIO_INIT_FAIL", error=str(e))

    # Start sell loop on boot if there are existing positions (important on restarts)
    _sell_stop: Optional[asyncio.Event] = None
    _sell_task: Optional[asyncio.Task] = None
    try:
        if list(all_positions()):
            log.event("BOOT_START_SELL_LOOP", reason="existing_positions")
            _sell_stop = asyncio.Event()
            _sell_task = asyncio.create_task(run_sell_loop(_sell_stop))
    except Exception as e:
        log.event("BOOT_SELL_LOOP_START_FAIL", error=str(e))

    try:
        backoff = 3
        while True:
            if stop_evt and stop_evt.is_set():
                break
            try:
                await stream_loop(stop_evt)
                print("[main] stream ended (remote closed). Reconnecting...", flush=True)
                backoff = 3
            except asyncio.CancelledError:
                raise
            except Exception:
                print("[main] stream_loop crashed:", flush=True)
                traceback.print_exc()
                backoff = min(backoff * 2, 30)
            await asyncio.sleep(backoff)
    finally:
        # Graceful shutdown: stop sell loop
        try:
            if _sell_stop is not None:
                _sell_stop.set()
            if _sell_task is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(_sell_task, timeout=2.0)
        except Exception:
            pass

if __name__ == "__main__":
    asyncio.run(main())
