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
    get_token_price,
    swap_token_jupiter,
    convert_sol_to_usd,
    send_telegram,
    log_to_notion,
    save_entry_price,
    get_entry_price,
)

# circuit-breakers (best-effort)
try:
    from utils_pkg.circuit_breakers import (
        trading_halted,
        record_error_and_maybe_halt,
        record_pnl_and_maybe_halt,
    )
except Exception:
    async def trading_halted(*args, **kwargs) -> bool:  # type: ignore
        return False
    async def record_error_and_maybe_halt(*args, **kwargs):  # type: ignore
        return None
    async def record_pnl_and_maybe_halt(*args, **kwargs):  # type: ignore
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
except Exception:
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


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


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


async def _sell_token(mint: str, amount: float, slippage: float) -> Optional[str]:
    ok, sig = await swap_token_jupiter(
        input_mint=mint,
        output_mint=SOL_MINT,
        amount_float=amount,
        buy=False,
        slippage=slippage,
    )
    if not ok:
        await record_error_and_maybe_halt(reason="autosell_swap_failed")
        return None
    return sig


async def _alert_once(redis_key: str, cooldown_sec: int, fn):
    try:
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
    sell_amount: float,
    entry_price: float,
    current_price: float,
    slippage: float,
):
    sig = await _sell_token(mint, sell_amount, slippage)
    if not sig:
        logger.warning(f"[{symbol}] partial sell failed")
        return

    pos["amount_invested"] = max(0.0, float(pos.get("amount_invested", 0.0)) - sell_amount)
    pos["chunks_sold"] = int(pos.get("chunks_sold", 0)) + 1
    await save_position(mint, pos)

    profit_sol = max(0.0, (current_price - entry_price) * sell_amount)
    usd_proceeds = await convert_sol_to_usd(current_price * sell_amount)
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
        balance=sell_amount,
        entry=entry_price,
        current=current_price,
        profit=profit_sol,
        reason="Partial TP",
    )
    msg = (
        f"🟡 Partial TP {symbol}\n"
        f"∆ {((current_price-entry_price)/entry_price)*100:.2f}% | Sold {sell_amount:.4f}\n"
        f"≈ ${usd_proceeds:.2f}\nTx: {sig}"
    )
    await send_telegram(msg)

    if AlertManager and alert_mgr:
        try:
            await alert_mgr.send_sell_alert(symbol, current_price, profit_sol, f"https://dexscreener.com/solana/{mint}", sol_value=sell_amount)
        except Exception:
            pass

    # Learning hook on partial close
    await process_trade_closure(
        token_address=mint,
        buy_price=entry_price,
        sell_price=current_price,
        amount=sell_amount,
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
    slippage: float,
    reason: str,
):
    amt = float(pos.get("amount_invested", 0.0))
    if amt <= 0:
        return

    sig = await _sell_token(mint, amt, slippage)
    if not sig:
        logger.warning(f"[{symbol}] full exit failed")
        return

    profit_sol = (current_price - entry_price) * amt
    usd_proceeds = await convert_sol_to_usd(current_price * amt)
    profit_usd = await convert_sol_to_usd(profit_sol)

    # Record realized PnL for daily stop logic
    try:
        await record_pnl_and_maybe_halt(pnl_usd=float(profit_usd))
    except TypeError:
        await record_pnl_and_maybe_halt(redis, pnl_usd=float(profit_usd))  # type: ignore

    await log_to_notion(
        token=mint,
        symbol=symbol,
        balance=amt,
        entry=entry_price,
        current=current_price,
        profit=profit_sol,
        reason=reason,
    )
    msg = (
        f"🔴 Exit {symbol} — {reason}\n"
        f"∆ {((current_price-entry_price)/entry_price)*100:.2f}% | Sold {amt:.4f}\n"
        f"≈ ${usd_proceeds:.2f}\nTx: {sig}"
    )
    await send_telegram(msg)

    if AlertManager and alert_mgr:
        try:
            await alert_mgr.send_sell_alert(symbol, current_price, profit_sol, f"https://dexscreener.com/solana/{mint}", sol_value=amt)
        except Exception:
            pass

    await process_trade_closure(
        token_address=mint,
        buy_price=entry_price,
        sell_price=current_price,
        amount=amt,
        partial=False,
        timestamp=_now_utc().isoformat(),
    )

    try:
        await remove_position(mint)
    except Exception:
        pass
    try:
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
            slippage = float(params["SLIPPAGE"])
            max_hold_min = int(params["MAX_HOLD_TIME_MINUTES"])

            positions = await load_positions() or {}
            tokens = await get_wallet_tokens(WALLET_PUBKEY) if WALLET_PUBKEY else []
            wallet_map = {t["mint"]: float(t.get("balance", 0.0)) for t in (tokens or [])}

            # We still proceed even if halted; exits reduce risk.
            halted = False
            try:
                halted = await trading_halted(redis)
            except Exception:
                halted = False

            for mint, pos in list(positions.items()):
                symbol = pos.get("symbol") or mint[:6]
                balance = wallet_map.get(mint, 0.0)
                if balance <= 0:
                    await remove_position(mint)
                    continue

                entry_price = await _get_entry(mint, pos)
                if not entry_price or entry_price <= 0:
                    cp = await get_token_price(mint)
                    if not cp:
                        continue
                    entry_price = cp
                    await save_entry_price(redis, mint, entry_price)

                current_price = await get_token_price(mint)
                if not current_price or current_price <= 0:
                    continue

                pct_change = ((current_price - entry_price) / entry_price) * 100.0
                buy_time = await _get_buytime(mint, pos) or (_now_utc() - timedelta(minutes=5))

                # Update trailing stop upward only
                await _update_trailing_stop(mint, pos, stop_loss, current_price)
                trailing = float(pos.get("trailing_stop", entry_price * (1 - stop_loss / 100.0)))

                # LADDERED TPs
                chunks_sold = int(pos.get("chunks_sold", 0))
                if chunks_sold < PARTIAL_SELL_CHUNKS and _should_partial_take_profit(pct_change, target_profit, chunks_sold):
                    sell_amount = max(0.0, balance / float(PARTIAL_SELL_CHUNKS - chunks_sold))
                    if sell_amount > 0:
                        await _handle_partial_sell(
                            alert_mgr=alert_mgr,
                            mint=mint,
                            symbol=symbol,
                            pos=pos,
                            sell_amount=sell_amount,
                            entry_price=entry_price,
                            current_price=current_price,
                            slippage=slippage,
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
                        slippage=slippage,
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
                        slippage=slippage,
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
                        slippage=slippage,
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
