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

def _istrue(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "y", "on"}

IS_DRYRUN = _istrue("DRY_RUN", "0")
RPC_URL   = os.getenv("SOL_RPC_URL")
TX_RPC_URL = os.getenv("SOL_RPC_TX_URL") or os.getenv("PF_TX_RPC_URL") or RPC_URL
PF_PP_ACCOUNT = (os.getenv("PF_PP_ACCOUNT") or "").strip() or None
PF_DEBUG_TX = str(os.getenv("PF_DEBUG_TX", "0")).lower() in {"1","true","yes","y","on"}

# Canonical mints
SOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
TOKEN_PROGRAM_ID      = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

# Position learning tunables
POS_LEARN_MAX_ATTEMPTS   = _envi("PF_POS_LEARN_MAX_ATTEMPTS", 10)
POS_LEARN_BASE_DELAY_SEC = _envf("PF_POS_LEARN_BASE_DELAY_SEC", 6.0)
POS_LEARN_BACKOFF        = _envf("PF_POS_LEARN_BACKOFF", 1.5)
POS_LEARN_LOG_EVERY      = _envi("PF_POS_LEARN_LOG_EVERY", 3)

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
    v = os.getenv("JUP_AS_LEGACY", "1")
    return str(v).lower() in {"1", "true", "yes", "y"}

# =============================================================================
# Exit / risk tuning
# =============================================================================

EXIT_TP1_PCT         = _envf("EXIT_TP1_PCT", 40.0)
EXIT_TP1_SELL_PCT    = _envf("EXIT_TP1_SELL_PCT", 0.40)
EXIT_TP2_PCT         = _envf("EXIT_TP2_PCT", 100.0)
EXIT_TP2_SELL_PCT    = _envf("EXIT_TP2_SELL_PCT", 0.30)
EXIT_TSL_ARM_PCT     = _envf("EXIT_TSL_ARM_PCT", 60.0)
EXIT_TSL_TRAIL_PCT   = _envf("EXIT_TSL_TRAIL_PCT", 20.0)
EXIT_HARD_STOP_PCT   = _envf("HARD_STOP_PCT", -32.0)

# =============================================================================
# Sell path helpers
# =============================================================================

def _slippage_bps() -> int:
    s = _envf("JUPITER_MAX_SLIPPAGE", 0.0125)  # 1.25%
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

def _rpc_node(url: str, method: str, params: list) -> Any:
    if not url:
        raise RuntimeError("RPC URL is not set")
    r = requests.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=20,
    )
    r.raise_for_status()
    js = r.json()
    if "error" in js:
        raise RuntimeError(js["error"])
    return js["result"]

def _rpc(method: str, params: list) -> Any:
    return _rpc_node(RPC_URL, method, params)

def _rpc_tx(method: str, params: list) -> Any:
    return _rpc_node(TX_RPC_URL, method, params)

def _sum_token_balance_iu_via_program_scan(owner: str, mint: str) -> Tuple[int, int]:
    total = 0
    decimals: Optional[int] = None
    for prog in (TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID):
        res = _rpc("getTokenAccountsByOwner", [
            owner, {"programId": prog}, {"encoding": "jsonParsed"}
        ]) or {}
        for it in res.get("value", []):
            info = it["account"]["data"]["parsed"]["info"]
            if info.get("mint") != mint:
                continue
            ta = info.get("tokenAmount", {})
            total += int(ta.get("amount", "0"))
            if decimals is None:
                decimals = int(ta.get("decimals", 0))
    return total, (decimals or 0)

def _sum_token_balance_iu(owner: str, mint: str) -> Tuple[int, int]:
    try:
        res = _rpc("getTokenAccountsByOwner", [
            owner, {"mint": mint}, {"encoding": "jsonParsed"}
        ]) or {}
        value = res.get("value", [])
        if not value:
            return _sum_token_balance_iu_via_program_scan(owner, mint)

        total = 0
        decimals = 0
        for it in value:
            info = it["account"]["data"]["parsed"]["info"]
            ta = info["tokenAmount"]
            decimals = int(ta.get("decimals", 0)) or decimals
            total += int(ta.get("amount", "0"))
        return total, decimals

    except Exception as e:
        msg = str(e)
        if "could not find mint" in msg or "Invalid param" in msg:
            return _sum_token_balance_iu_via_program_scan(owner, mint)
        total, dec = _sum_token_balance_iu_via_program_scan(owner, mint)
        if total > 0:
            return total, dec
        raise

async def _wait_tx_confirmed(sig: str, *, max_secs: float = 30.0) -> bool:
    t0 = time.time()
    while time.time() - t0 < max_secs:
        res = _rpc("getSignatureStatuses", [[sig], {"searchTransactionHistory": True}])
        v = (res or {}).get("value", [None])[0]
        if v:
            if v.get("err"):
                return False
            st = v.get("confirmationStatus") or v.get("confirmation_status")
            if st in ("confirmed", "finalized"):
                return True
        await asyncio.sleep(0.8)
    return False

# ---- Learn from buy transaction (fast path) ----------------------------------

def _parse_tx_token_delta_for_owner(tx: dict, owner: str, mint: str) -> Tuple[int, int]:
    """
    Return (delta_amount_iu, decimals) for `owner` & `mint` from a parsed getTransaction.
    If delta can't be computed, fall back to post balance (useful on first buy).
    """
    meta = (tx or {}).get("meta") or {}
    pre  = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    # Map pre by (accountIndex) for quick lookups
    pre_by_idx: Dict[int, Tuple[int, int]] = {}
    for b in pre:
        if (b.get("owner") == owner) and (b.get("mint") == mint):
            ui = b.get("uiTokenAmount") or {}
            amt = int(ui.get("amount", "0"))
            dec = int(ui.get("decimals", 0))
            pre_by_idx[int(b.get("accountIndex", -1))] = (amt, dec)

    total_delta = 0
    decimals = 0
    any_match = False
    post_sum = 0

    for b in post:
        if (b.get("owner") == owner) and (b.get("mint") == mint):
            any_match = True
            ui = b.get("uiTokenAmount") or {}
            post_amt = int(ui.get("amount", "0"))
            decimals = int(ui.get("decimals", 0)) or decimals
            post_sum += post_amt

            idx = int(b.get("accountIndex", -1))
            if idx in pre_by_idx:
                pre_amt, _ = pre_by_idx[idx]
                total_delta += max(0, post_amt - pre_amt)
            else:
                total_delta += post_amt  # assume new ATA

    if any_match:
        if total_delta > 0:
            return total_delta, decimals
        return post_sum, decimals  # fallback
 if PF_DEBUG_TX and (post):
        owners = {(b.get("owner"), b.get("mint")) for b in post}
        print(f"[pos:debug] postTokenBalances owners for tx include: {owners}", flush=True)
    return 0, 0

async def _learn_from_tx_sig(owner: str, mint: str, sig: str, *, max_wait: float = 45.0) -> Tuple[int, int]:
    """
    Poll getTransaction(jsonParsed) for the buy signature and compute how much
    of `mint` landed in `owner`'s wallet. Returns (amount_iu, decimals).
    """
    t0 = time.time()
    while time.time() - t0 < max_wait:
        try:
            tx = _rpc_tx("getTransaction", [
                sig,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0
                }
            ])
        except Exception:
            tx = None

        if tx:
            amt_iu, dec = _parse_tx_token_delta_for_owner(tx, owner, mint)
            if amt_iu > 0:
                return amt_iu, dec
        await asyncio.sleep(1.2)

if PF_DEBUG_TX:
        print(f"[pos] tx not visible on TX RPC for {sig}. "
              f"Set SOL_RPC_TX_URL to a node that supports getTransaction.", flush=True)
    return 0, 0

# =============================================================================
# Jupiter quote / swap (LEGACY tx)
# =============================================================================

def _jup_quote(input_mint: str, output_mint: str, amount_iu: int) -> dict:
    _assert_mint(input_mint); _assert_mint(output_mint)

    url = "https://quote-api.jup.ag/v6/quote"
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
    def __init__(self) -> None:
        # prefer explicit PF_PP_ACCOUNT, else local keypair pubkey
        self.owner = PF_PP_ACCOUNT or get_pubkey_str(load_keypair())
        self.pos: Dict[str, Dict[str, Any]] = {}

    async def on_buy_filled(self, mint: str, *, tx_sig: Optional[str] = None,
                            poll_sec: float = POS_LEARN_BASE_DELAY_SEC) -> None:
        """
        Learn position size. Prefer parsing the buy transaction (tx_sig) for the
        exact credited amount; fall back to RPC wallet scans if needed.
        """
        # 1) Fast path: learn from the transaction itself
        amt_iu, dec = 0, 0
        if tx_sig:
            amt_iu, dec = await _learn_from_tx_sig(self.owner, mint, tx_sig, max_wait=45.0)

        # 2) If tx parsing didn’t yield, fall back to scanning the wallet with backoff
        if amt_iu <= 0:
            max_attempts = max(1, int(POS_LEARN_MAX_ATTEMPTS))
            delay = max(1.0, float(poll_sec))
            last_err: Optional[Exception] = None

            for i in range(1, max_attempts + 1):
                await asyncio.sleep(delay)
                try:
                    amt_iu, dec = _sum_token_balance_iu(self.owner, mint)
                    if amt_iu > 0:
                        break
                    last_err = None
                except Exception as e:
                    last_err = e

                delay = min(delay * float(POS_LEARN_BACKOFF), 30.0)
                if i in (1, 2, 3) or (POS_LEARN_LOG_EVERY and i % POS_LEARN_LOG_EVERY == 0):
                    if last_err:
                        print(f"[pos] retry learn {mint} attempt {i}/{max_attempts}... last_bal={amt_iu} err={last_err}", flush=True)
                    else:
                        print(f"[pos] retry learn {mint} attempt {i}/{max_attempts}... last_bal={amt_iu}", flush=True)

            if amt_iu <= 0:
                if last_err:
                    print(f"[pos] giving up learn {mint}: {last_err}", flush=True)
                else:
                    print(f"[pos] giving up learn {mint}: still zero after retries", flush=True)
                return

        # estimate entry price via a tiny quote (1% of size, clamped)
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
            "peak_sol": price_sol,
            "entry_sol": price_sol,
            "last_ts": time.time(),
        }
        print(f"[pos] learned entry for {mint}: size_iu={amt_iu} entry≈{price_sol} SOL", flush=True)

    async def on_trade_tick(self, msg: Dict[str, Any]) -> None:
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
            return

        if not st.get("entry_sol"):
            st["entry_sol"] = px_sol
            st["peak_sol"]  = px_sol

        st["peak_sol"] = max(st.get("peak_sol") or px_sol, px_sol)
        st["last_ts"]  = time.time()

        entry = st["entry_sol"]
        if not entry or entry <= 0:
            return

        pnl_pct = (px_sol / entry - 1.0) * 100.0
        drawdown_from_peak_pct = (px_sol / (st["peak_sol"] or px_sol) - 1.0) * 100.0

        if pnl_pct <= EXIT_HARD_STOP_PCT:
            print(f"[exit] HARD STOP {mint} pnl={pnl_pct:.1f}%")
            await sell_on_jupiter(mint, pct_of_wallet=1.0)
            st["size_iu"] = 0
            return

        if (not st["tp1_done"]) and (pnl_pct >= EXIT_TP1_PCT):
            print(f"[exit] TP1 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP1_SELL_PCT:.0%}")
            await sell_on_jupiter(mint, pct_of_wallet=EXIT_TP1_SELL_PCT)
            st["tp1_done"] = True
            return

        if (not st["tp2_done"]) and (pnl_pct >= EXIT_TP2_PCT):
            print(f"[exit] TP2 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP2_SELL_PCT:.0%}")
            await sell_on_jupiter(mint, pct_of_wallet=EXIT_TP2_SELL_PCT)
            st["tp2_done"] = True
            return

        if pnl_pct >= EXIT_TSL_ARM_PCT:
            if drawdown_from_peak_pct <= -EXIT_TSL_TRAIL_PCT:
                print(f"[exit] TSL {mint} drop_from_peak={drawdown_from_peak_pct:.1f}% -> exit")
                await sell_on_jupiter(mint, pct_of_wallet=1.0)
                st["size_iu"] = 0
                return

# exported singleton
exit_engine = PositionsExitEngine()
