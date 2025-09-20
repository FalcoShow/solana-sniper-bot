# utils_pkg/trader.py
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, Optional, Tuple

from logging_setup import get_logger, bump
from utils_pkg.positions import (
    get as pos_get,
    record_buy,
    record_sell,
)
from utils_pkg.txlearn import (
    resolve_then_learn_position as tx_resolve_then_learn,
    TX_RPC_URL, RPC_URL, TX_COMMITMENT, _Rpc,  # lightweight async RPC
)
from utils_pkg.solana_token import fetch_mint_info, classify_token_program
from utils_pkg.price_feed import price_in_sol
from utils_pkg.router import sell_via_lightning  # Lightning-style Jupiter router

# Optional resolvers if present (best-effort only)
try:
    from utils_pkg.pump_trader import resolve_true_mint, resolve_true_mint_from_pump_id
except Exception:
    resolve_true_mint = None  # type: ignore
    resolve_true_mint_from_pump_id = None  # type: ignore

# Optional guardrails + ledger (best-effort imports)
try:
    from utils_pkg.guardrails import record_trade_open as gr_record_trade_open
except Exception:
    gr_record_trade_open = None  # type: ignore

try:
    from utils_pkg.ledger import record_fill as ledger_record_fill
except Exception:
    ledger_record_fill = None  # type: ignore

log = get_logger()

# ──────────────────────────────────────────────────────────────────────────────
# Env & constants
# ──────────────────────────────────────────────────────────────────────────────
IS_DRYRUN = (os.getenv("DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "y", "on"})
OWNER_PUBKEY = (os.getenv("PF_PP_ACCOUNT") or "").strip()

SLIPPAGE_BPS = int(os.getenv("PF_SLIPPAGE_BPS", "600") or "600")
PRIORITY_SOL = float(os.getenv("PF_PRIORITY_SOL", "0.002") or "0.002")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
async def _resolve_mint_from_tx(sig: str, owner: str) -> Optional[str]:
    """
    Fallback: parse getTransaction(meta.postTokenBalances) to find the mint we received.
    """
    tx_rpc = _Rpc(TX_RPC_URL or RPC_URL)
    try:
        tx = await tx_rpc.get_transaction(sig, TX_COMMITMENT)
    except Exception:
        tx = None

    if not tx:
        return None

    meta = (tx or {}).get("meta") or {}
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    def _raw_amount(entry: dict) -> int:
        ui = entry.get("uiTokenAmount") or {}
        try:
            return int(str(ui.get("amount", "0")))
        except Exception:
            return 0

    # Prefer positive delta for our owner
    best_mint: Optional[str] = None
    best_delta: int = 0

    pre_map: Dict[Tuple[str, str], int] = {}
    for b in pre:
        if b.get("owner") == owner:
            pre_map[(b.get("mint", ""), owner)] = _raw_amount(b)

    for b in post:
        if b.get("owner") != owner:
            continue
        mint = b.get("mint") or ""
        if not mint:
            continue
        after_amt = _raw_amount(b)
        before_amt = pre_map.get((mint, owner), 0)
        delta = after_amt - before_amt
        if delta > best_delta:
            best_delta, best_mint = delta, mint

    if best_mint:
        return best_mint

    # Fallback: if single owned mint exists in post, accept it
    owned_post = [b.get("mint") for b in post if b.get("owner") == owner and b.get("mint")]
    uniq = list({m for m in owned_post if m})
    return uniq[0] if len(uniq) == 1 else None


async def _resolve_true_mint_best_effort(raw_id_or_mint: str, tx_sig: str, owner: str) -> Optional[str]:
    """
    Try multiple ways to turn a raw pump id or candidate into a real mint:
      1) pump_trader.resolve_true_mint(..., signature=tx_sig) (best)
      2) pump_trader.resolve_true_mint_from_pump_id(...)
      3) infer from getTransaction (owner delta)
      4) accept the input if it already behaves like a mint (mint info exists)
    """
    # 1) signature-aided resolver
    if resolve_true_mint is not None:
        try:
            m = resolve_true_mint(raw_id_or_mint, signature=tx_sig)
            if asyncio.iscoroutine(m):
                m = await m
            if m:
                return m
        except Exception:
            pass

    # 2) pump-id-only resolver
    if resolve_true_mint_from_pump_id is not None:
        try:
            m = resolve_true_mint_from_pump_id(raw_id_or_mint)
            if asyncio.iscoroutine(m):
                m = await m
            if m:
                return m
        except Exception:
            pass

    # 3) infer from the tx itself
    try:
        m = await _resolve_mint_from_tx(tx_sig, owner)
        if m:
            return m
    except Exception:
        pass

    # 4) last resort: if this is already a mint (has mint info), accept it
    try:
        info = await fetch_mint_info(raw_id_or_mint)
        if info:
            return raw_id_or_mint
    except Exception:
        pass

    return None


# ──────────────────────────────────────────────────────────────────────────────
# Public SELL API (kept name for scripts/main.py compatibility)
# ──────────────────────────────────────────────────────────────────────────────
async def sell_on_jupiter(mint: str, pct_of_wallet: float = 1.0) -> Optional[str]:
    """
    Sells via your Lightning router (keeps naming for scripts/main.py).
    Looks up current position to get qty_raw + token_program; exits by %.
    """
    if IS_DRYRUN:
        log.event("DRYRUN_SELL", mint=mint, pct=pct_of_wallet)
        return None

    p = pos_get(mint)
    if not p:
        log.event("SELL_NO_POSITION", mint=mint)
        return None

    pct = max(0.0, min(1.0, float(pct_of_wallet)))
    qty_raw = int(max(1, round(int(p["qty_raw"]) * pct)))

    ok, sig = await sell_via_lightning(
        mint=mint,
        token_program=p["token_program"],
        amount_raw=qty_raw,
        slippage_bps=SLIPPAGE_BPS,
        priority_sol=PRIORITY_SOL,
    )
    log.event("SELL_SUBMIT_DIRECT", mint=mint, qty_raw=qty_raw, ok=ok, sig=sig)

    if ok:
        record_sell(mint=mint, qty_raw=qty_raw, sig=sig)
        bump("sells_ok")
        # (ledger for sells is recorded by scripts/sell_engine; we keep this path minimal)
        return sig
    else:
        bump("sells_fail")
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Exit Engine: after BUY, learn + persist, and (optionally) react to ticks
# ──────────────────────────────────────────────────────────────────────────────
class _ExitEngine:
    def __init__(self) -> None:
        self._sell_task: Optional[asyncio.Task] = None
        self._sell_stop_evt: Optional[asyncio.Event] = None

    async def _ensure_sell_loop(self) -> None:
        """
        Start the continuous sell loop the first time we have a live position.
        """
        if self._sell_task is None or self._sell_task.done():
            from scripts.sell_engine import run_sell_loop  # lazy import to avoid cycles
            self._sell_stop_evt = asyncio.Event()
            self._sell_task = asyncio.create_task(run_sell_loop(self._sell_stop_evt))
            log.event("EXIT_ENGINE_START")

    async def on_buy_filled(self, raw_id_or_mint: str, tx_sig: str) -> None:
        """
        After each BUY fill:
          - Resolve the true mint robustly
          - Use tx-learner to wait for wallet balance visibility
          - Fetch decimals + token program
          - Approximate avg entry cost (SOL per token) via quote API
          - Record the position to SQLite (utils_pkg/positions.py)
          - Emit optional guardrails/ledger events
          - Ensure sell loop is running
        """
        if IS_DRYRUN:
            return

        owner = OWNER_PUBKEY or None

        # 1) tx-learner for balance visibility (and tx status)
        res = await tx_resolve_then_learn(
            raw_id_or_mint=raw_id_or_mint,
            tx_sig=tx_sig,
            owner=owner,
            rpc_url_tx=TX_RPC_URL,
            rpc_url_general=RPC_URL or TX_RPC_URL,
        )

        # Early exit on explicit tx error
        status = (res or {}).get("status") or {}
        if status.get("err") is not None:
            log.event("EXIT_TX_FAILED", raw=raw_id_or_mint, sig=tx_sig, err=str(status.get("err")))
            return

        # 2) resolve final mint key for DB / pricing
        mint = await _resolve_mint_from_tx(tx_sig, OWNER_PUBKEY or "")
        if not mint:
            mint = await _resolve_true_mint_best_effort(raw_id_or_mint, tx_sig, OWNER_PUBKEY or "")
        if not mint:
            log.event("EXIT_RESOLVE_FAIL", raw=raw_id_or_mint, sig=tx_sig)
            return

        # 3) mint info → decimals + token program
        info = await fetch_mint_info(mint)
        if not info:
            log.event("EXIT_MINTINFO_FAIL", mint=mint)
            return
        decimals, owner_pid = info
        token_program = classify_token_program(owner_pid)

        # 4) balance in tokens (ui) from learner → qty_raw
        bal_ui = float(res.get("owner_balance_ui") or 0.0)
        qty_raw = int(max(0, round(bal_ui * (10 ** int(decimals)))))

        # 5) best-effort avg entry SOL/token
        avg_cost_sol = await price_in_sol(mint, decimals)
        if avg_cost_sol <= 0:
            avg_cost_sol = 0.0

        # 6) persist position
        record_buy(
            mint=mint,
            qty_raw=qty_raw,
            price_per_token_sol=avg_cost_sol,
            decimals=decimals,
            token_program=token_program,
            owner_ata="",           # optional; router derives ATA internally
            symbol=None,            # can be filled by caller if you track it
            pool="pump",
            sig=tx_sig,
        )
        bump("positions_opened")
        log.event(
            "EXIT_BUY_LEARNED",
            mint=mint,
            qty_raw=qty_raw,
            decimals=decimals,
            token_program=token_program,
            tx_found=bool(res.get("tx_found")),
        )

        # 7) optional guardrails + ledger
        try:
            if gr_record_trade_open:
                # Notional in SOL (best-effort) for risk ledgers
                notional_sol = bal_ui * avg_cost_sol
                asyncio.create_task(gr_record_trade_open(notional_sol=float(notional_sol)))
        except Exception:
            pass

        try:
            if ledger_record_fill:
                ledger_record_fill(
                    side="buy",
                    mint=mint,
                    qty_raw=int(qty_raw),
                    decimals=int(decimals),
                    price_sol=float(avg_cost_sol),
                    sig=tx_sig,
                )
        except Exception:
            pass

        # 8) ensure sell loop is alive
        await self._ensure_sell_loop()

    async def on_trade_tick(self, raw_event: Dict[str, Any]) -> None:
        # Optional: hook for streaming ticks (we currently just count activity)
        bump("trade_ticks")


# exported singleton used by scripts/main.py
exit_engine = _ExitEngine()

__all__ = [
    "IS_DRYRUN",
    "sell_on_jupiter",
    "exit_engine",
]
