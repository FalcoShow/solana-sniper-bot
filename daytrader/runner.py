from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from .features import compute_indicators
from .alpha import generate_signal
from .position import load_position, save_position, new_position, close_position
from .risk import atr_to_price, size_from_risk, stops_from_atr

# Trade SOL <-> token by default
SOL_MINT = "So11111111111111111111111111111111111111112"

# Optional bridge into your existing core utils
try:
    # swap_token_jupiter(input_mint, output_mint, amount_float, buy=True, slippage=...)
    from utils_pkg.core_utils import swap_token_jupiter, get_wallet_balance  # type: ignore
except Exception:
    swap_token_jupiter = None  # type: ignore
    get_wallet_balance = None  # type: ignore


# Optional data source shim (your daytrader/data_feed.py should define this)
try:
    from daytrader.data_feed import get_recent_bars  # type: ignore
except Exception:
    async def get_recent_bars(mint: str, interval_s: int, lookback: int) -> List[Dict[str, Any]]:
        raise RuntimeError("daytrader.data_feed.get_recent_bars missing; please add data_feed.py")


def _get_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


# Tunables (via .env)
SLIPPAGE = _get_float("DT_SLIPPAGE", 0.005)     # 0.5% default
POLL_SEC = int(_get_float("DT_POLL_SEC", 30))   # loop cadence
LOOKBACK = int(_get_float("DT_LOOKBACK", 200))  # bars to fetch for features


@dataclass
class RunnerConfig:
    mint: str
    symbol: str
    interval_s: int = 60  # bar interval (seconds)


async def _get_sol_balance_fallback() -> float:
    """Try wallet RPC; fall back to DT_SOL_BAL_FALLBACK or 0."""
    if get_wallet_balance:
        try:
            bal = await get_wallet_balance()
            return float(bal)
        except Exception:
            pass
    try:
        return float(os.getenv("DT_SOL_BAL_FALLBACK", "0"))
    except Exception:
        return 0.0


async def _buy(cfg: RunnerConfig, bars: List[Dict[str, Any]]) -> Optional[str]:
    """Size position by risk (ATR), swap SOL -> token, persist position."""
    feats = compute_indicators(bars)
    latest = feats["latest"]              # dict with 'c' last price in SOL terms
    atr_pct = feats["atr_pct"]            # ATR as % of price
    atr_price = atr_to_price(atr_pct, latest["c"])

    bal_sol = await _get_sol_balance_fallback()
    trade_sol = size_from_risk(
        balance_sol=bal_sol,
        entry_price_sol=latest["c"],
        atr_price=atr_price
    )

    if trade_sol <= 0:
        return None

    if swap_token_jupiter is None:
        raise RuntimeError("utils_pkg.core_utils.swap_token_jupiter not available")

    ok, sig = await swap_token_jupiter(
        SOL_MINT, cfg.mint, trade_sol, buy=True, slippage=SLIPPAGE
    )
    if not ok:
        return None

    # Approximate token qty by SOL spent / entry price (fine for risk book-keeping)
    qty_token = trade_sol / max(1e-12, latest["c"])
    stop, tp = stops_from_atr(entry_price_sol=latest["c"], atr_price=atr_price)
    pos = new_position(cfg.mint, cfg.symbol, qty_token, latest["c"], stop, tp)
    save_position(pos)
    return sig


async def _sell(cfg: RunnerConfig, bars: List[Dict[str, Any]], qty_token: float) -> Optional[str]:
    """Swap token -> SOL and close the position."""
    if qty_token <= 0:
        return None
    if swap_token_jupiter is None:
        raise RuntimeError("utils_pkg.core_utils.swap_token_jupiter not available")

    ok, sig = await swap_token_jupiter(
        cfg.mint, SOL_MINT, qty_token, buy=False, slippage=SLIPPAGE
    )
    if not ok:
        return None

    close_position(cfg.mint)
    return sig


async def run_once(cfg: RunnerConfig) -> Optional[str]:
    """
    One evaluation/decision cycle:
      1) fetch bars
      2) compute features + signal
      3) manage stops/targets on any open position
      4) act on signal (enter/exit) when confidence >= 0.6
    """
    bars = await get_recent_bars(cfg.mint, cfg.interval_s, LOOKBACK)
    if len(bars) < 50:  # need enough bars for indicators
        return None

    signal = generate_signal(bars)
    latest = signal.latest  # last bar dict {'c': ...}

    # Refresh / check open position
    pos = load_position(cfg.mint)
    if pos:
        pos.update_last(latest["c"])
        # Hard exits
        if latest["c"] <= pos.stop_sol or latest["c"] >= pos.tp_sol:
            return await _sell(cfg, bars, pos.qty_token)

    # Entry / Early exit on signal
    if (not pos or pos.is_flat) and signal.action == "BUY" and signal.confidence >= 0.6:
        return await _buy(cfg, bars)

    if pos and not pos.is_flat and signal.action == "SELL" and signal.confidence >= 0.6:
        return await _sell(cfg, bars, pos.qty_token)

    return None


async def run_loop(cfg: RunnerConfig) -> None:
    """Continuous loop. Keeps going even if something hiccups once."""
    while True:
        try:
            tx = await run_once(cfg)
            if tx:
                print(f"[{cfg.symbol}] trade sent: {tx}")
        except Exception as e:
            print(f"[runner] error: {e}")
        await asyncio.sleep(POLL_SEC)


# -------- CLI --------

def _parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--mint", required=True, help="Token mint to trade vs SOL")
    p.add_argument("--symbol", required=True, help="Symbol/label for logs")
    p.add_argument("--interval", type=int, default=60, help="Bar interval seconds")
    p.add_argument("--once", action="store_true", help="Run a single decision only")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    cfg = RunnerConfig(mint=args.mint, symbol=args.symbol, interval_s=args.interval)
    if args.once:
        asyncio.run(run_once(cfg))
    else:
        asyncio.run(run_loop(cfg))
