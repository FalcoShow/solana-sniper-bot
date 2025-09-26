# sniper/auto_sell.py
from __future__ import annotations

import os
import asyncio
import logging
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta

from utils_pkg.core_utils import (
    init_resources,
    close_resources,
    session,
    redis,
    get_wallet_tokens,
    convert_sol_to_usd,
    send_telegram,
    log_to_notion,
    save_entry_price,
    get_entry_price,
)

# Router: single source of truth for mint handling, price & swaps
from utils_pkg.router import (
    normalize_pump_id_to_mint,
    get_decimals,
    get_price_ui,
    sell_via_lightning,
)

# circuit-breakers (best-effort)
try:
    from utils_pkg.circuit_breakers import (
        trading_halted,
        record_error_and_maybe_halt,
        record_pnl_and_maybe_halt,
    )
except Exception:  # pragma: no cover
    async def trading_halted(*_a, **_k) -> bool:  # type: ignore
        return False
    async def record_error_and_maybe_halt(*_a, **_k):  # type: ignore
        return None
    async def record_pnl_and_maybe_halt(*_a, **_k):  # type: ignore
        return None

from utils_pkg.ai_sell_logic import should_hold_token

# positions: prefer services.*, fallback to project root
try:  # pragma: no cover
    from services.positions import load_positions, save_position, remove_position
except Exception:  # pragma: no cover
    from positions import load_positions, save_position, remove_position  # type: ignore

# optional: simple arbitrage heads-up (best-effort)
try:  # pragma: no cover
    from sniper.arbitrage import check_arbitrage_opportunity
except Exception:  # pragma: no cover
    async def check_arbitrage_opportunity(mint: str):
        return None

# optional: AlertManager (Telegram / Discord multiplexing)
try:  # pragma: no cover
    from services.alert_manager import AlertManager
except Exception:  # pragma: no cover
    AlertManager = None  # type: ignore

# self-learning hooks (daily/rolling aggregation lives here)
try:
    from self_learning_ai_strategy import (
        get_strategy_params,
        process_trade_closure,
    )
except Exception:  # pragma: no cover
    async def get_strategy_params(_redis=None) -> Dict[str, Any]:
        return {
            "STOP_LOSS_PERCENT": float(os.getenv("STOP_LOSS_PERCENT", "20")),
            "TARGET_PROFIT_PERCENT": float(os.getenv("TARGET_PROFIT_PERCENT", "200")),
            "HOLD_IF_SCORE_ABOVE": float(os.getenv("HOLD_IF_SCORE_ABOVE", "0.85")),
            "SLIPPAGE": float(os.getenv("SLIPPAGE", "0.5")),  # %
            "MAX_HOLD_TIME_MINUTES": int(os.getenv("MAX_HOLD_TIME_MINUTES", "360")),
        }

    async def process_trade_closure(**kwargs):
        return None

logger = logging.getLogger("auto_sell")
logging.basicConfig(level=logging.INFO)

# Wrapped SOL mint used by Jupiter
SOL_MINT = "So11111111111111111111111111111111111111112"

# ENV
WALLET_PUBKEY = os.getenv("WALLET_PUBKEY") or os.getenv("WALLET_PUBLIC_KEY")
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))
POSITION_MONITOR_INTERVAL = int(os.getenv("POSITION_MONITOR_INTERVAL", "20"))
PARTIAL_SELL_CHUNKS = int(os.getenv("PARTIAL_SELL_CHUNKS", "4"))  # laddered TP count
DRY_RUN = (os.getenv("DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y"})

# Router knobs
DEFAULT_PRIORITY_SOL = float(os.getenv("PF_PRIORITY_SOL", "0.002") or 0.0)
DEFAULT_SLIPPAGE_BPS_ENV = os.getenv("PF_SLIPPAGE_BPS")  # if set, overrides % slippage input


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _slippage_to_bps(slippage_percent: float) -> int:
    # 0.5 (%) -> 50 bps
    try:
        return max(1, int(round(float(slippage_percent) * 100)))
    except Exception:
        return 50


async def _get_buytime(mint: str, pos: Optional[Dict[str, Any]]) -> Optional[datetime]:
    ts = await redis.get(f"buytime:{mint}") if redis else None
    if ts:
        try:
            dt = datetime.fromisoformat(ts)
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    if pos and pos.get("timestamp"):
        try:
            dt = datetime.fromisoformat(pos["timestamp"])
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


async def _get_entry(mint: str, pos: Optional[Dict[str, Any]]) -> Optional[float]:
    e = await get_entry_price(redis, mint) if redis else None
    if e is not None:
        return e
    if pos and isinstance(pos.get("buy_price"), (int, float)):
        return float(pos["buy_price"])
    return None


async def _update_trailing_stop(mint: str, pos: Dict[str, Any], stop_loss_percent: float, current_price: float) -> None:
    if current_price <= 0:
        return
    new_trailing = current_price * (1 - stop_loss_percent / 100.0)
    trailing = pos.get("trailing_stop")
    if trailing is None or new_trailing > trailing:
        pos["trailing_stop"] = new_trailing
        await save_position(mint, pos)


def _should_partial_take_profit(pct_change: float, target_profit: float, chunks_sold: int) -> bool:
    if PARTIAL_SELL_CHUNKS <= 0:
        return False
    step = target_profit / PARTIAL_SELL_CHUNKS
    needed = step * (chunks_sold + 1)
    return pct_change >= needed


async def _router_price_ui(mint: str) -> float:
    try:
        return float(await get_price_ui(mint) or 0.0)
    except Exception:
        return 0.0


async def _sell_token(mint_raw: str, amount_ui: float, slippage_percent: float) -> Optional[str]:
    """
    Execute sell using router (Jupiter v6 under the hood).
    Handles Pump IDs → real mint, fetches decimals, scales UI → raw, and submits.
    """
    try:
        norm = normalize_pump_id_to_mint(mint_raw)
    except Exception:
        await record_error_and_maybe_halt(reason="autosell_normalize_fail")
        return None

    try:
        dec = await get_decimals(norm)
    except Exception:
        dec = None
    if dec is None:
        # conservative default
        dec = 9

    try:
        amount_raw = int(round(float(amount_ui) * (10 ** int(dec))))
    except Exception:
        await record_error_and_maybe_halt(reason="autosell_amount_scale_fail")
        return None
    if amount_raw <= 0:
        return None

    # honor global BPS override if set; otherwise derive from provided percentage
    if DEFAULT_SLIPPAGE_BPS_ENV is not None:
        try:
            slippage_bps = max(1, int(DEFAULT_SLIPPAGE_BPS_ENV))
        except Exception:
            slippage_bps = _slippage_to_bps(slippage_percent)
    else:
        slippage_bps = _slippage_to_bps(slippage_percent)

    if DRY_RUN:
        return f"DRYRUN-{os.urandom(4).hex()}"

    ok, sig = await sell_via_lightning(
        mint=norm,
        token_program=None,
        amount_raw=amount_raw,
        slippage_bps=slippage_bps,
        priority_sol=DEFAULT_PRIORITY_SOL,
    )
    if not ok or not sig:
        await record_error_and_maybe_halt(reason="autosell_swap_failed")
        return None
    return sig


async def _alert_once(redis_key: str, cooldown_sec: int, fn):
    try:
        if not redis:
            await fn()
            return
        last = await redis.get(redis_key)
        now = _now_utc().timestamp()
        if last and (now - float(last)) < cooldown_sec:
            return
        await fn()
        await redis.set(redis_key, str(now), ex=cooldown_sec)
    except Exception:
        try:
            await fn()
        except Exception:
            pass


async def _handle_partial_sell(
    alert_mgr,
    mint: str,
    symbol: str,
    pos: Dict[str, Any],
    sell_amount_ui: float,
    entry_price: float,
    current_price: float,
    slippage_percent: float,
):
    sig = await _sell_token(mint, sell_amount_ui, slippage_percent)
    if not sig:
        logger.warning(f"[{symbol}] partial sell failed")
        return

    pos["amount_invested"] = max(0.0, float(pos.get("amount_invested", 0.0)) - sell_amount_ui)
    pos["chunks_sold"] = int(pos.get("chunks_sold", 0)) + 1
    await save_position(mint, pos)

    profit_sol = max(0.0, (current_price - entry_price) * sell_amount_ui)
    usd_proceeds = await convert_sol_to_usd(current_price * sell_amount_ui)
    profit_usd = await convert_sol_to_usd(profit_sol)

    # Record realized PnL for daily stop logic
    try:
        await record_pnl_and_maybe_halt(pnl_usd=float(profit_usd))
    except TypeError:
        await record_pnl_and_maybe_halt(redis, pnl_usd=float(profit_usd))  # type: ignore

    # Notion + Telegram + AlertManager
    await log_to_notion(
        token=mint,
        symbol=symbol,
        balance=sell_amount_ui,
        entry=entry_price,
        current=current_price,
        profit=profit_sol,
        reason="Partial TP",
    )
    msg = (
        f"🟡 Partial TP {symbol}\n"
        f"∆ {((current_price-entry_price)/entry_price)*100:.2f}% | Sold {sell_amount_ui:.4f}\n"
        f"≈ ${usd_proceeds:.2f}\nTx: {sig}"
    )
    await send_telegram(msg)

    if AlertManager and alert_mgr:
        try:
            await alert_mgr.send_sell_alert(
                symbol, current_price, profit_sol, f"https://dexscreener.com/solana/{mint}", sol_value=sell_amount_ui
            )
        except Exception:
            pass

    # Learning hook on partial close
    await process_trade_closure(
        token_address=mint,
        buy_price=entry_price,
        sell_price=current_price,
        amount=sell_amount_ui,
        partial=True,
        timestamp=_now_utc().isoformat(),
    )


async def _handle_full_exit(
    alert_mgr,
    mint: str,
    symbol: str,
    pos: Dict[str, Any],
    entry_price: float,
    current_price: float,
    slippage_percent: float,
    reason: str,
):
    amt_ui = float(pos.get("amount_invested", 0.0))
    if amt_ui <= 0:
        return

    sig = await _sell_token(mint, amt_ui, slippage_percent)
    if not sig:
        logger.warning(f"[{symbol}] full exit failed")
        return

    profit_sol = (current_price - entry_price) * amt_ui
    usd_proceeds = await convert_sol_to_usd(current_price * amt_ui)
    profit_usd = await convert_sol_to_usd(profit_sol)

    # Record realized PnL for daily stop logic
    try:
        await record_pnl_and_maybe_halt(pnl_usd=float(profit_usd))
    except TypeError:
        await record_pnl_and_maybe_halt(redis, pnl_usd=float(profit_usd))  # type: ignore

    await log_to_notion(
        token=mint,
        symbol=symbol,
        balance=amt_ui,
        entry=entry_price,
        current=current_price,
        profit=profit_sol,
        reason=reason,
    )
    msg = (
        f"🔴 Exit {symbol} — {reason}\n"
        f"∆ {((current_price-entry_price)/entry_price)*100:.2f}% | Sold {amt_ui:.4f}\n"
        f"≈ ${usd_proceeds:.2f}\nTx: {sig}"
    )
    await send_telegram(msg)

    if AlertManager and alert_mgr:
        try:
            await alert_mgr.send_sell_alert(
                symbol, current_price, profit_sol, f"https://dexscreener.com/solana/{mint}", sol_value=amt_ui
            )
        except Exception:
            pass

    await process_trade_closure(
        token_address=mint,
        buy_price=entry_price,
        sell_price=current_price,
        amount=amt_ui,
        partial=False,
        timestamp=_now_utc().isoformat(),
    )

    try:
        await remove_position(mint)
    except Exception:
        pass
    try:
        if redis:
            await redis.delete(f"entry_price:{mint}")
            await redis.delete(f"buytime:{mint}")
    except Exception:
        pass


async def sweep_loop():
    """
    Sweep positions to enforce SL/TP/hold logic. Sells are allowed even if trading is halted.
    """
    alert_mgr = AlertManager() if AlertManager else None

    while True:
        try:
            params = await get_strategy_params(redis)
            stop_loss = float(params["STOP_LOSS_PERCENT"])
            target_profit = float(params["TARGET_PROFIT_PERCENT"])
            hold_score = float(params["HOLD_IF_SCORE_ABOVE"])
            slippage_percent = float(params["SLIPPAGE"])
            max_hold_min = int(params["MAX_HOLD_TIME_MINUTES"])

            positions = await load_positions() or {}
            tokens = await get_wallet_tokens(WALLET_PUBKEY) if WALLET_PUBKEY else []
            wallet_map = {t["mint"]: float(t.get("balance", 0.0)) for t in (tokens or [])}

            # We still proceed even if halted; exits reduce risk.
            try:
                _ = await trading_halted(redis)  # noqa: F841
            except Exception:
                pass

            for mint, pos in list(positions.items()):
                symbol = pos.get("symbol") or mint[:6]
                balance_ui = wallet_map.get(mint, 0.0)
                if balance_ui <= 0:
                    await remove_position(mint)
                    continue

                # Normalize for router calls
                try:
                    norm = normalize_pump_id_to_mint(mint)
                except Exception:
                    # keep original for storage; but we can't price/sell without a mint
                    continue

                entry_price = await _get_entry(mint, pos)
                if not entry_price or entry_price <= 0:
                    cp = await _router_price_ui(norm)
                    if cp <= 0:
                        continue
                    entry_price = cp
                    await save_entry_price(redis, mint, entry_price)

                current_price = await _router_price_ui(norm)
                if current_price <= 0:
                    continue

                pct_change = ((current_price - entry_price) / entry_price) * 100.0
                buy_time = await _get_buytime(mint, pos) or (_now_utc() - timedelta(minutes=5))

                # Update trailing stop upward only
                await _update_trailing_stop(mint, pos, stop_loss, current_price)
                trailing = float(pos.get("trailing_stop", entry_price * (1 - stop_loss / 100.0)))

                # LADDERED TPs
                chunks_sold = int(pos.get("chunks_sold", 0))
                if chunks_sold < PARTIAL_SELL_CHUNKS and _should_partial_take_profit(pct_change, target_profit, chunks_sold):
                    sell_amount_ui = max(0.0, balance_ui / float(PARTIAL_SELL_CHUNKS - chunks_sold))
                    if sell_amount_ui > 0:
                        await _handle_partial_sell(
                            alert_mgr=alert_mgr,
                            mint=mint,
                            symbol=symbol,
                            pos=pos,
                            sell_amount_ui=sell_amount_ui,
                            entry_price=entry_price,
                            current_price=current_price,
                            slippage_percent=slippage_percent,
                        )
                        continue

                # HARD SL (trailing)
                if current_price <= trailing:
                    await _handle_full_exit(
                        alert_mgr=alert_mgr,
                        mint=mint,
                        symbol=symbol,
                        pos=pos,
                        entry_price=entry_price,
                        current_price=current_price,
                        slippage_percent=slippage_percent,
                        reason="Trailing Stop",
                    )
                    continue

                # MAX HOLD TIME
                if (_now_utc() - buy_time) > timedelta(minutes=max_hold_min):
                    await _handle_full_exit(
                        alert_mgr=alert_mgr,
                        mint=mint,
                        symbol=symbol,
                        pos=pos,
                        entry_price=entry_price,
                        current_price=current_price,
                        slippage_percent=slippage_percent,
                        reason="Max Hold",
                    )
                    continue

                # AI HOLD/SELL
                hold = await should_hold_token(
                    threshold_score=hold_score,
                    price_change_pct=pct_change,
                    token_address=mint,
                    buy_time=buy_time,
                    redis=redis,
                )
                if not hold:
                    await _handle_full_exit(
                        alert_mgr=alert_mgr,
                        mint=mint,
                        symbol=symbol,
                        pos=pos,
                        entry_price=entry_price,
                        current_price=current_price,
                        slippage_percent=slippage_percent,
                        reason="AI Signal",
                    )
                    continue

                # Optional arbitrage FYI (rate-limited)
                async def _arb_fyi():
                    opp = await check_arbitrage_opportunity(mint)
                    if opp:
                        edge = opp.get("edge_bps", 0)
                        await send_telegram(
                            f"⚡ Potential Arb {symbol}: +{edge} bps\n"
                            f"Buy {opp['buy']['dex']} → Sell {opp['sell']['dex']}"
                        )

                await _alert_once(f"arb:fyi:{mint}", cooldown_sec=ALERT_COOLDOWN_SECONDS, fn=_arb_fyi)

        except Exception as e:
            logger.error(f"[auto_sell] loop error: {e}", exc_info=True)

        await asyncio.sleep(POSITION_MONITOR_INTERVAL)


async def main():
    await init_resources()
    try:
        await sweep_loop()
    finally:
        await close_resources()


if __name__ == "__main__":
    asyncio.run(main())
