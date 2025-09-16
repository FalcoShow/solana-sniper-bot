# utils_pkg/trader.py
from __future__ import annotations

import os
import math
import time
import asyncio
from typing import Tuple, Optional, Dict, Any

import requests
import base58

from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# =============================================================================
# Env & config helpers
# =============================================================================

def _envf(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default

def _envi(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

IS_DRYRUN = str(os.getenv("DRY_RUN", "0")).strip() in {"1", "true", "yes", "y", "on"}
RPC_URL   = os.getenv("SOL_RPC_URL")

# Canonical mints
SOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# =============================================================================
# Mint validation & Jupiter legacy toggle
# =============================================================================

def _assert_mint(m: str) -> None:
    try:
        if len(base58.b58decode(m)) != 32:
            raise ValueError("decoded length is not 32 bytes")
    except Exception as e:
        raise ValueError(f"Invalid mint '{m}': {e}")

def _jup_as_legacy() -> bool:
    """
    Default to legacy Jupiter transactions so our signer path (legacy-only)
    works consistently. Set JUP_AS_LEGACY=0 if you later add v0 support.
    """
    v = os.getenv("JUP_AS_LEGACY", "1")
    return str(v).lower() in {"1", "true", "yes", "y"}

# =============================================================================
# Exit / risk tuning (override with env to adjust aggressiveness)
# =============================================================================

EXIT_TP1_PCT         = _envf("EXIT_TP1_PCT", 40.0)       # +40% → take partial
EXIT_TP1_SELL_PCT    = _envf("EXIT_TP1_SELL_PCT", 0.40)  # sell 40% at TP1
EXIT_TP2_PCT         = _envf("EXIT_TP2_PCT", 100.0)      # +100% → take more
EXIT_TP2_SELL_PCT    = _envf("EXIT_TP2_SELL_PCT", 0.30)  # sell 30% at TP2
EXIT_TSL_ARM_PCT     = _envf("EXIT_TSL_ARM_PCT", 60.0)   # arm trailing stop when +60%
EXIT_TSL_TRAIL_PCT   = _envf("EXIT_TSL_TRAIL_PCT", 20.0) # trail gap 20% off the peak
EXIT_HARD_STOP_PCT   = _envf("HARD_STOP_PCT", -32.0)     # kill if drawdown <= -32%

# =============================================================================
# Sell path helpers
# =============================================================================

def _slippage_bps() -> int:
    # JUPITER_MAX_SLIPPAGE as fraction (e.g. 0.0125 = 1.25%)
    s = _envf("JUPITER_MAX_SLIPPAGE", 0.0125)
    return max(1, int(round(s * 10_000)))

def _priority_lamports() -> Optional[int]:
    p = os.getenv("PF_PRIORITY_SOL")
    if not p:
        return None
    try:
        return int(float(p) * 1_000_000_000)
    except Exception:
        return None

# =============================================================================
# RPC helpers
# =============================================================================

def _rpc(method: str, params: list) -> Any:
    if not RPC_URL:
        raise RuntimeError("SOL_RPC_URL is not set")
    r = requests.post(
        RPC_URL,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=20,
    )
    r.raise_for_status()
    js = r.json()
    if "error" in js:
        raise RuntimeError(js["error"])
    return js["result"]

def _sum_token_balance_iu(owner: str, mint: str) -> Tuple[int, int]:
    """
    Return (amount_in_base_units, decimals) across all token accounts for `mint`.
    """
    _assert_mint(mint)
    res = _rpc("getTokenAccountsByOwner", [
        owner, {"mint": mint}, {"encoding": "jsonParsed"}
    ])
    total = 0
    decimals = 0
    for it in res.get("value", []):
        info = it["account"]["data"]["parsed"]["info"]
        ta = info["tokenAmount"]
        decimals = int(ta.get("decimals", 0))
        amt = int(ta.get("amount", "0"))
        total += amt
    return total, decimals

# =============================================================================
# Jupiter quote / swap (LEGACY tx)
# =============================================================================

def _jup_quote(input_mint: str, output_mint: str, amount_iu: int) -> dict:
    _assert_mint(input_mint); _assert_mint(output_mint)

    url = "https://quote-api.jup.ag/v6/quote"
    # Some Jupiter edge deployments are picky about types on these two flags,
    # so we send them as strings here (works reliably in practice).
    q = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount_iu),
        "slippageBps": _slippage_bps(),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "true" if _jup_as_legacy() else "false",
    }
    r = requests.get(url, params=q, timeout=20)
    if not r.ok:
        raise RuntimeError(f"Jupiter quote {r.status_code}: {r.text[:400]}")
    return r.json()

def _jup_swap(user_pubkey: str, quote: dict, priority_lamports: Optional[int]) -> str:
    url = "https://quote-api.jup.ag/v6/swap"
    body = {
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "dynamicComputeUnitLimit": True,
        "quoteResponse": quote,
        # swap endpoint accepts real booleans
        "asLegacyTransaction": True if _jup_as_legacy() else False,
    }
    if priority_lamports is not None:
        body["prioritizationFeeLamports"] = priority_lamports

    r = requests.post(url, json=body, timeout=20)
    if not r.ok:
        try:
            detail = r.json()
        except Exception:
            detail = r.text
        raise RuntimeError(f"Jupiter /swap error {r.status_code}: {detail}")

    js = r.json()
    tx_b64 = js.get("swapTransaction")
    if not tx_b64:
        raise RuntimeError(f"Jupiter swap returned no transaction: {js}")
    return tx_b64

# =============================================================================
# Telegram (optional)
# =============================================================================

def _tg_send(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat  = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception:
        pass

# =============================================================================
# Public: SELL
# =============================================================================

async def sell_on_jupiter(mint: str, *, pct_of_wallet: float = 1.0) -> Optional[str]:
    """
    Sell `pct_of_wallet` of the SPL token `mint` into SOL via Jupiter.
    Returns the signature if broadcasted, or None if nothing to sell.
    Respects DRY_RUN=1.
    """
    kp = load_keypair()
    owner = get_pubkey_str(kp)

    try:
        bal_iu, _decimals = _sum_token_balance_iu(owner, mint)
    except Exception as e:
        print(f"[SELL] balance query failed for {mint}: {e}")
        return None

    if bal_iu <= 0:
        print(f"[SELL] no balance for {mint}")
        return None

    pct = max(0.0, min(1.0, pct_of_wallet))
    sell_iu = int(math.floor(bal_iu * pct))
    if sell_iu <= 0:
        print(f"[SELL] computed 0 amount for {mint}")
        return None

    quote = _jup_quote(mint, SOL_MINT, sell_iu)

    if IS_DRYRUN:
        est_out = quote.get("outAmount", "0")
        print(f"[DRYRUN:SELL] {mint} size_iu={sell_iu} est_out_lamports={est_out}")
        _tg_send(f"🧪 DRY SELL {mint}: {sell_iu} iu")
        return None

    tx_b64 = _jup_swap(owner, quote, _priority_lamports())
    sig = await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=True)
    print(f"[SELL] {mint} -> SOL sent: https://solscan.io/tx/{sig}")
    _tg_send(f"✅ SOLD {mint} • pct={pct:.0%} • tx: https://solscan.io/tx/{sig}")
    return sig

# =============================================================================
# Exit Engine (positions + rules)
# =============================================================================

def _extract_mint(msg: Dict[str, Any]) -> str:
    return (msg.get("mint") or msg.get("mintAddress") or msg.get("mint_address") or "").strip()

class PositionsExitEngine:
    """
    Minimal in-memory positions + exits.
    - Learns size after buy (`on_buy_filled`).
    - Uses Jupiter quotes for price in SOL.
    - Does TP1 / TP2 partials, TSL, and hard stop.
    """

    def __init__(self) -> None:
        self.owner = get_pubkey_str(load_keypair())
        self.pos: Dict[str, Dict[str, Any]] = {}  # mint -> state

    async def on_buy_filled(self, mint: str, *, poll_sec: float = 6.0) -> None:
        """
        Called after you send a buy. Poll wallet to learn size.
        Safe to call even if the buy failed; it’ll just see 0 and return.
        """
        await asyncio.sleep(poll_sec)  # let the fill settle

        try:
            amt_iu, dec = _sum_token_balance_iu(self.owner, mint)
        except Exception as e:
            # e.g., RPC: "could not find mint" or transient index lag — just skip quietly
            print(f"[pos] skip learn {mint}: {e}")
            return

        if amt_iu <= 0:
            return

        # estimate entry price via a tiny quote (≈1% of size, clamped)
        sample = max(1, min(amt_iu // 100, amt_iu))
        try:
            q = _jup_quote(mint, SOL_MINT, sample)
            out_lamports = float(q.get("outAmount", 0))
            price_sol = (out_lamports / sample) / 1_000_000_000.0
        except Exception:
            price_sol = None

        self.pos[mint] = {
            "size_iu": amt_iu,
            "decimals": dec,
            "tp1_done": False,
            "tp2_done": False,
            "peak_sol": price_sol,   # track peak for TSL
            "entry_sol": price_sol,  # None if quote failed, we’ll handle it
            "last_ts": time.time(),
        }
        print(f"[pos] learned entry for {mint}: size_iu={amt_iu} entry≈{price_sol} SOL", flush=True)
        _tg_send(f"📥 ENTRY {mint} size_iu={amt_iu} entry≈{(price_sol or 0):.6f} SOL")

    async def on_trade_tick(self, msg: Dict[str, Any]) -> None:
        """Call on every PumpPortal trade tick."""
        mint = _extract_mint(msg)
        if not mint or mint not in self.pos:
            return

        st = self.pos[mint]
        if st.get("size_iu", 0) <= 0:
            return

        # price via small quote (~0.5–1% of position)
        sample = max(1, min(st["size_iu"] // 200, st["size_iu"]))
        try:
            q = _jup_quote(mint, SOL_MINT, sample)
            out_lamports = float(q.get("outAmount", 0))
            px_sol = (out_lamports / sample) / 1_000_000_000.0
        except Exception:
            return  # skip if quote fails briefly

        # If we never got entry price (quote failed earlier), set it now
        if not st.get("entry_sol"):
            st["entry_sol"] = px_sol
            st["peak_sol"]  = px_sol

        # update peak (for TSL)
        st["peak_sol"] = max(st.get("peak_sol") or px_sol, px_sol)
        st["last_ts"]  = time.time()

        entry = st["entry_sol"]
        if not entry or entry <= 0:
            return

        pnl_pct = (px_sol / entry - 1.0) * 100.0
        drawdown_from_peak_pct = (px_sol / (st["peak_sol"] or px_sol) - 1.0) * 100.0

        # ----- Hard stop -----
        if pnl_pct <= EXIT_HARD_STOP_PCT:
            print(f"[exit] HARD STOP {mint} pnl={pnl_pct:.1f}%")
            _tg_send(f"⛔ HARD STOP {mint} pnl={pnl_pct:.1f}%")
            await sell_on_jupiter(mint, pct_of_wallet=1.0)
            st["size_iu"] = 0
            return

        # ----- TP1 -----
        if (not st["tp1_done"]) and (pnl_pct >= EXIT_TP1_PCT):
            print(f"[exit] TP1 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP1_SELL_PCT:.0%}")
            _tg_send(f"🎯 TP1 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP1_SELL_PCT:.0%}")
            await sell_on_jupiter(mint, pct_of_wallet=EXIT_TP1_SELL_PCT)
            st["tp1_done"] = True
            return

        # ----- TP2 -----
        if (not st["tp2_done"]) and (pnl_pct >= EXIT_TP2_PCT):
            print(f"[exit] TP2 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP2_SELL_PCT:.0%}")
            _tg_send(f"🎯 TP2 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP2_SELL_PCT:.0%}")
            await sell_on_jupiter(mint, pct_of_wallet=EXIT_TP2_SELL_PCT)
            st["tp2_done"] = True
            return

        # ----- Trailing stop (armed after EXIT_TSL_ARM_PCT) -----
        if pnl_pct >= EXIT_TSL_ARM_PCT:
            if drawdown_from_peak_pct <= -EXIT_TSL_TRAIL_PCT:
                print(f"[exit] TSL {mint} drop_from_peak={drawdown_from_peak_pct:.1f}% -> exit")
                _tg_send(f"🪢 TSL {mint} drop={drawdown_from_peak_pct:.1f}% -> exit")
                await sell_on_jupiter(mint, pct_of_wallet=1.0)  # close remainder
                st["size_iu"] = 0
                return

# exported singleton
exit_engine = PositionsExitEngine()
