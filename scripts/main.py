# scripts/main.py
from __future__ import annotations

import asyncio
import os
import traceback
from typing import Any, Dict, Optional

from utils_pkg.pumpportal_client import PumpPortalClient
from utils_pkg.pump_trader import maybe_buy, resolve_true_mint, resolve_true_mint_from_pump_id
from utils_pkg.trader import sell_on_jupiter, exit_engine, IS_DRYRUN


# ------------------------ env helpers ------------------------

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


# ------------------------ config ------------------------

EXIT_ENGINE_ENABLED = _as_bool(os.getenv("EXIT_ENGINE_ENABLED", "1"), True)

# Entry filters (optional; tweak via env)
MIN_INITIAL_BUY_SOL = _as_float(os.getenv("ENTRY_MIN_BUY_SOL", "0.0"), 0.0)
MAX_NAME_LEN        = _as_int(os.getenv("ENTRY_MAX_NAME_LEN", "40"), 40)
SYMBOL_BLACKLIST    = {
    s.strip().upper()
    for s in os.getenv("ENTRY_SYMBOL_BLACKLIST", "RUG,SCAM").split(",")
    if s.strip()
}

# Concurrency / pacing
MAX_CONCURRENCY   = max(1, _as_int(os.getenv("PF_MAX_CONCURRENT", "2"), 2))
LOOP_COOLDOWN_SEC = _as_float(os.getenv("PF_COOLDOWN_SEC", "15"), 15.0)

# Debug
DEBUG_TX = _as_bool(os.getenv("PF_DEBUG_TX", "0"), False)


# ------------------------ small utils ------------------------

def _get_first(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default

def _extract_raw_mint(msg: Dict[str, Any]) -> str:
    """
    DO NOT normalize or strip '.pump'.
    We pass the raw id to the trader/exit-engine; they handle resolution.
    """
    raw = _get_first(
        msg,
        "mint",
        "mintAddress",
        "mint_address",
        "bondingCurve",
        "bonding_curve",
        default="",
    )
    raw = ("" if raw is None else str(raw)).strip()
    print(f"[main] raw pump id = {raw}", flush=True)  # keep for log correlation
    return raw

def _extract_symbol(msg: Dict[str, Any]) -> str:
    return str(_get_first(msg, "symbol", "ticker", default="")).strip()

def _extract_name(msg: Dict[str, Any]) -> str:
    return str(_get_first(msg, "name", default="")).strip()

def _passes_filters(msg: Dict[str, Any]) -> bool:
    # Only act on new token creates
    typ = str(_get_first(msg, "txType", "type", default="")).strip().lower()
    if typ != "create":
        return False

    # Initial buy threshold
    init_buy = _as_float(_get_first(msg, "initialBuy", "initial_buy", default=0.0), 0.0)
    if init_buy < MIN_INITIAL_BUY_SOL:
        if DEBUG_TX:
            print(f"[filter] initialBuy {init_buy} < min {MIN_INITIAL_BUY_SOL} — skip", flush=True)
        return False

    # Symbol blacklist
    sym = _extract_symbol(msg).upper()
    if any(bad and bad in sym for bad in SYMBOL_BLACKLIST):
        if DEBUG_TX:
            print(f"[filter] symbol '{sym}' hits blacklist {SYMBOL_BLACKLIST} — skip", flush=True)
        return False

    # Name length
    nm = _extract_name(msg)
    if MAX_NAME_LEN > 0 and len(nm) > MAX_NAME_LEN:
        if DEBUG_TX:
            print(f"[filter] name too long ({len(nm)} > {MAX_NAME_LEN}) — skip", flush=True)
        return False

    return True


# ------------------------ optional post-buy test ------------------------

async def _post_buy_exit_test(raw_id_or_mint: str, tx_sig: str) -> None:
    """
    If POSTBUY_MODE=immediate_sell_test, attempt to resolve to a mint and market-sell 100%.
    Note: resolution is best-effort; if unresolved we skip the test safely.
    """
    if str(os.getenv("POSTBUY_MODE", "")).strip().lower() != "immediate_sell_test":
        return

    # Let the tx land so owner balance becomes visible
    await asyncio.sleep(5)

    # Try to resolve a real mint for the test
    resolved = None
    try:
        resolved = resolve_true_mint(raw_id_or_mint, signature=tx_sig)
    except Exception:
        resolved = None
    if not resolved:
        try:
            resolved = resolve_true_mint_from_pump_id(raw_id_or_mint)
        except Exception:
            resolved = None

    if not resolved:
        print(f"[post-buy test] could not resolve '{raw_id_or_mint}' — skip sell test", flush=True)
        return

    try:
        await sell_on_jupiter(resolved, pct_of_wallet=1.0)
        print(f"[post-buy test] sold 100% via Jupiter for {resolved}", flush=True)
    except Exception as e:
        print(f"[post-buy test] sell failed for {resolved}: {e}", flush=True)


# ------------------------ strategy ------------------------

class Strategy:
    def __init__(self) -> None:
        self._seen_mints: set[str] = set()
        self._sema = asyncio.Semaphore(MAX_CONCURRENCY)

    async def on_new_token(self, raw_event: Dict[str, Any]) -> None:
        mint_raw = _extract_raw_mint(raw_event)
        if not mint_raw:
            if DEBUG_TX:
                print("[strategy] missing mint — skip", flush=True)
            return

        if mint_raw in self._seen_mints:
            if DEBUG_TX:
                print(f"[strategy] already seen {mint_raw} — skip", flush=True)
            return
        self._seen_mints.add(mint_raw)

        if not _passes_filters(raw_event):
            return

        # Build UI context for trader; keep raw id intact.
        ui: Dict[str, Any] = dict(raw_event)  # shallow copy
        ui.setdefault("pool", "pump")
        ui.setdefault("symbol", _extract_symbol(raw_event))
        ui.setdefault("name", _extract_name(raw_event))
        ui["rawMint"] = mint_raw
        ui.setdefault("normalized", False)

        # Two log lines to mirror previous output style
        symbol = ui.get("symbol", "") or "?"
        print(f"[trade] pool=pump symbol={symbol} mint={mint_raw} curve=-", flush=True)
        print(f"[trade] pool=pump symbol={symbol} mint={mint_raw} curve=- for {symbol} ({mint_raw})", flush=True)

        async with self._sema:
            try:
                # Submit BUY (may be raw '...pump'); trading stack handles it.
                tx_sig = await maybe_buy(mint=mint_raw, ui=ui)
                if not tx_sig:
                    return  # buy didn’t execute (dry-run/route failure/etc.)

                # 👉 Wire the exit engine with *raw id*; it resolves internally.
                if not IS_DRYRUN and EXIT_ENGINE_ENABLED:
                    asyncio.create_task(
                        exit_engine.on_buy_filled(raw_id_or_mint=mint_raw, tx_sig=tx_sig)
                    )

                # Optional immediate sell test
                asyncio.create_task(_post_buy_exit_test(mint_raw, tx_sig))

            except Exception:
                print("[strategy] maybe_buy exception:", flush=True)
                traceback.print_exc()

        # Light pacing so we don't hammer the stream/trader
        await asyncio.sleep(LOOP_COOLDOWN_SEC)

    async def on_trade(self, raw_event: Dict[str, Any]) -> None:
        # Hook for extra strategy reactions to trades if you add any later.
        return


# ------------------------ stream loop & main ------------------------

async def stream_loop() -> None:
    client = PumpPortalClient()
    strat = Strategy()

    print("[main] Connecting:", client.masked_uri(), flush=True)

    async for ev in client.events():
        try:
            kind = ev.get("kind")
            raw = ev.get("raw", {}) or {}
            if kind == "new_token":
                await strat.on_new_token(raw)
            elif kind == "trade" and EXIT_ENGINE_ENABLED:
                try:
                    await exit_engine.on_trade_tick(raw)
                except Exception:
                    if DEBUG_TX:
                        print("[exit] on_trade_tick error:", flush=True)
                        traceback.print_exc()
                await strat.on_trade(raw)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Never let a single bad event kill the stream
            print("[main] event handling error:", flush=True)
            traceback.print_exc()

async def main() -> None:
    backoff = 3
    while True:
        try:
            await stream_loop()
            print("[main] stream ended (remote closed). Reconnecting...", flush=True)
            backoff = 3  # reset on clean end
        except asyncio.CancelledError:
            raise
        except Exception:
            print("[main] stream_loop crashed:", flush=True)
            traceback.print_exc()
            backoff = min(backoff * 2, 30)
        await asyncio.sleep(backoff)

if __name__ == "__main__":
    asyncio.run(main())
