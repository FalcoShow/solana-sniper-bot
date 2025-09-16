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

def _is_true(v: str | None, default: bool = False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}

IS_DRYRUN   = _is_true(os.getenv("DRY_RUN"), False)
RPC_URL     = os.getenv("SOL_RPC_URL")
TX_RPC_URL  = os.getenv("SOL_RPC_TX_URL") or RPC_URL
OWNER_FROM_ENV = (os.getenv("PF_PP_ACCOUNT") or "").strip()

PF_DEBUG_TX = _is_true(os.getenv("PF_DEBUG_TX"), False)

# Learner tuning (can be overridden in env)
LEARN_MAX_ATTEMPTS   = _envi("PF_POS_LEARN_MAX_ATTEMPTS", 10)
LEARN_BASE_DELAY_SEC = _envf("PF_POS_LEARN_BASE_DELAY_SEC", 6.0)
LEARN_BACKOFF        = _envf("PF_POS_LEARN_BACKOFF", 1.5)
LEARN_LOG_EVERY      = _envi("PF_POS_LEARN_LOG_EVERY", 3)

# Canonical mints
SOL_MINT  = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
TOKEN_PROGRAM_ID      = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

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
# Exit / risk tuning (override with env)
# =============================================================================

EXIT_TP1_PCT         = _envf("EXIT_TP1_PCT", 40.0)       # +40% → partial
EXIT_TP1_SELL_PCT    = _envf("EXIT_TP1_SELL_PCT", 0.40)  # sell 40%
EXIT_TP2_PCT         = _envf("EXIT_TP2_PCT", 100.0)      # +100% → more
EXIT_TP2_SELL_PCT    = _envf("EXIT_TP2_SELL_PCT", 0.30)  # sell 30%
EXIT_TSL_ARM_PCT     = _envf("EXIT_TSL_ARM_PCT", 60.0)   # arm TSL at +60%
EXIT_TSL_TRAIL_PCT   = _envf("EXIT_TSL_TRAIL_PCT", 20.0) # trail 20% off peak
EXIT_HARD_STOP_PCT   = _envf("HARD_STOP_PCT", -32.0)     # hard stop

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

def _rpc_json(url: str, method: str, params: list) -> Any:
    if not url:
        raise RuntimeError("RPC URL not set")
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
    return _rpc_json(RPC_URL, method, params)

def _txrpc(method: str, params: list) -> Any:
    return _rpc_json(TX_RPC_URL, method, params)

def _get_transaction(sig: str) -> Optional[dict]:
    """
    Fetch a transaction with meta (for postTokenBalances). Use TX_RPC_URL if set.
    """
    try:
        # We want meta + token balance owners; request parsed with maxSupportedTransactionVersion=0
        res = _txrpc("getTransaction", [
            sig,
            {"maxSupportedTransactionVersion": 0, "commitment": "confirmed"}
        ])
        return res
    except Exception as e:
        if PF_DEBUG_TX:
            print(f"[txlearn] getTransaction failed for {sig}: {e}", flush=True)
        return None

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
# TX-based position learn
# =============================================================================

def _learn_from_tx_sig(owner_pubkey: str, mint: str, tx_sig: str) -> Tuple[int, int]:
    """
    Try to learn filled token amount immediately from the transaction meta.
    Returns (amount_iu, decimals); (0,0) on failure.
    """
    tx = _get_transaction(tx_sig)
    if not tx:
        return 0, 0
    meta = tx.get("meta") or {}
    ptb = meta.get("postTokenBalances") or []
    if PF_DEBUG_TX:
        print(f"[txlearn] postTokenBalances count={len(ptb)}", flush=True)

    total = 0
    decimals: Optional[int] = None
    for b in ptb:
        try:
            if b.get("mint") != mint:
                continue
            # Some RPCs include owner directly; otherwise resolve via accountIndex -> accountKeys
            owner = b.get("owner")
            if not owner:
                acc_idx = b.get("accountIndex")
                if acc_idx is not None:
                    keys = (tx.get("transaction") or {}).get("message", {}).get("accountKeys") or []
                    if isinstance(keys, list) and 0 <= acc_idx < len(keys):
                        owner = keys[acc_idx].get("pubkey") if isinstance(keys[acc_idx], dict) else keys[acc_idx]
            if owner != owner_pubkey:
                continue
            ui = b.get("uiTokenAmount") or {}
            amt = int(ui.get("amount", "0"))
            total += amt
            if decimals is None:
                decimals = int(ui.get("decimals", 0))
        except Exception:
            continue

    if total > 0:
        return total, (decimals or 0)
    return 0, 0

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
    owner = OWNER_FROM_ENV or get_pubkey_str(kp)

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
# Exit Engine (positions + rules) — robust tx-learn (no mint-filter RPC)
# =============================================================================

def _extract_mint(msg: Dict[str, Any]) -> str:
    return (msg.get("mint") or msg.get("mintAddress") or msg.get("mint_address") or "").strip()

# --- Config knobs (env) -------------------------------------------------------
OWNER_FROM_ENV        = (os.getenv("PF_PP_ACCOUNT") or "").strip()
PF_DEBUG_TX           = str(os.getenv("PF_DEBUG_TX", "0")).strip().lower() in {"1","true","yes","y","on"}
PF_TXLEARN_DUMP       = str(os.getenv("PF_TXLEARN_DUMP", "0")).strip().lower() in {"1","true","yes","y","on"}

LEARN_BASE_DELAY_SEC  = _envf("PF_POS_LEARN_BASE_DELAY_SEC", 6.0)
LEARN_MAX_ATTEMPTS    = _envi("PF_POS_LEARN_MAX_ATTEMPTS", 10)
LEARN_BACKOFF         = _envf("PF_POS_LEARN_BACKOFF", 1.5)
LEARN_LOG_EVERY       = _envi("PF_POS_LEARN_LOG_EVERY", 3)

TXLEARN_TRIES         = _envi("PF_TXLEARN_TRIES", 3)           # fast tx-meta attempts
TXLEARN_DELAY         = _envf("PF_TXLEARN_DELAY_SEC", 0.7)     # between tx-meta attempts
TXLEARN_WARMUP        = _envf("PF_TXLEARN_WARMUP_SEC", 0.25)   # tiny delay before first try

# Prefer a separate, faster RPC for transaction/meta lookups if provided
RPC_TX_URL            = os.getenv("SOL_RPC_TX_URL") or os.getenv("SOL_RPC_URL")

def _rpc_tx(method: str, params: list) -> Any:
    url = RPC_TX_URL
    if not url:
        raise RuntimeError("No RPC URL for tx lookups (set SOL_RPC_TX_URL or SOL_RPC_URL).")
    r = requests.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=20,
    )
    r.raise_for_status()
    js = r.json()
    if "error" in js:
        raise RuntimeError(js["error"])
    return js.get("result")

def _tx_get(meta_sig: str) -> Dict[str, Any] | None:
    return _rpc_tx("getTransaction", [
        meta_sig,
        {
            "encoding": "jsonParsed",
            "commitment": "finalized",
            "maxSupportedTransactionVersion": 0
        },
    ])

def _acct_info_jsonParsed(pubkey: str) -> Dict[str, Any] | None:
    """Fetch jsonParsed account; return result['value'] or None."""
    res = _rpc_tx("getAccountInfo", [
        pubkey,
        {"encoding": "jsonParsed", "commitment": "finalized"}
    ])
    return (res or {}).get("value")

def _learn_from_tx_sig(owner: str, mint: str, tx_sig: str) -> Tuple[int, int]:
    """
    Learn post-balance of `mint` for `owner` using the transaction data alone.
    Tries three passes:
      1) Direct owner+mint match in postTokenBalances (if RPC includes owner).
      2) Largest positive delta for this mint among postTokenBalances that belong to owner
         (inferred via accountKeys + jsonParsed getAccountInfo).
      3) If deltas not available, scan all accountKeys via getAccountInfo and return the
         token account owned by `owner` for `mint` with its parsed 'tokenAmount'.
    Returns (amount_iu, decimals). (0,0) if not found.
    """
    tx = _tx_get(tx_sig)
    if not tx:
        return 0, 0

    meta    = tx.get("meta") or {}
    message = (tx.get("transaction") or {}).get("message") or {}
    keys    = message.get("accountKeys") or []

    # Normalize keys to simple list of base58 strings
    key_list: list[str] = []
    for k in keys:
        if isinstance(k, str):
            key_list.append(k)
        else:
            pk = (k or {}).get("pubkey")
            if pk: key_list.append(pk)

    pre  = meta.get("preTokenBalances")  or []
    post = meta.get("postTokenBalances") or []

    if PF_TXLEARN_DUMP and PF_DEBUG_TX:
        try:
            mints_post = [(b.get("mint"), b.get("owner"), b.get("accountIndex")) for b in post]
            print(f"[txlearn] dump postTokenBalances (mint,owner,idx): {mints_post[:12]}", flush=True)
        except Exception:
            pass

    # Quick maps by accountIndex for delta checks
    def _map(bals: list) -> Dict[int, Tuple[str,int,int,str]]:
        out: Dict[int, Tuple[str,int,int,str]] = {}
        for b in bals:
            try:
                idx = int(b.get("accountIndex"))
                ui  = b.get("uiTokenAmount") or {}
                out[idx] = (
                    b.get("mint",""),
                    int(ui.get("amount", "0")),
                    int(ui.get("decimals", 0) or 0),
                    b.get("owner") or "",   # may be missing on some RPCs
                )
            except Exception:
                continue
        return out

    pre_map  = _map(pre)
    post_map = _map(post)

    # ----- Pass 1: direct owner + mint in postTokenBalances -----
    for b in post:
        try:
            if b.get("mint") != mint:
                continue
            if (b.get("owner") or "") == owner:
                ui  = b.get("uiTokenAmount") or {}
                amt = int(ui.get("amount", "0"))
                dec = int(ui.get("decimals", 0) or 0)
                if amt > 0:
                    return amt, dec
        except Exception:
            continue

    # Helper: probe account by key via getAccountInfo jsonParsed
    def _probe_account_for_owner_amount(acct_key: str) -> Tuple[int,int] | None:
        try:
            val = _acct_info_jsonParsed(acct_key)  # {'data':{'parsed':{'info':{...}}}}
            if not val:
                return None
            data = (val.get("data") or {}).get("parsed") or {}
            info = data.get("info") or {}
            if (info.get("mint") == mint) and (info.get("owner") == owner):
                ta = (info.get("tokenAmount") or {})
                amt = int((ta.get("amount") or "0"))
                dec = int((ta.get("decimals") or 0))
                return (amt, dec) if amt > 0 else None
        except Exception:
            return None
        return None

    # ----- Pass 2: infer owner via accountIndex -> accountKey -> getAccountInfo; pick largest +delta -----
    best_amt, best_dec = 0, 0
    for idx, (m_mint, post_amt, dec, b_owner) in post_map.items():
        if m_mint != mint:
            continue
        # If owner present and matches, we can use it immediately
        if b_owner == owner and post_amt > 0:
            return post_amt, dec
        # Else, resolve the accountKey and probe it
        if 0 <= idx < len(key_list):
            key = key_list[idx]
            probed = _probe_account_for_owner_amount(key)
            if probed:
                amt, d = probed
                # delta preference if we have pre
                pre_amt = (pre_map.get(idx) or ("",0,d,""))[1]
                eff_amt = max(amt - pre_amt, 0) or amt  # prefer delta, else absolute
                if eff_amt > best_amt:
                    best_amt, best_dec = amt, d

    if best_amt > 0:
        return best_amt, best_dec

    # ----- Pass 3: brute scan all accountKeys (jsonParsed) for owner's token acct of this mint -----
    for key in key_list:
        probed = _probe_account_for_owner_amount(key)
        if probed:
            return probed

    return 0, 0


class PositionsExitEngine:
    def __init__(self) -> None:
        # Use PF_PP_ACCOUNT when provided to match PumpPortal payer
        self.owner = OWNER_FROM_ENV or get_pubkey_str(load_keypair())
        self.pos: Dict[str, Dict[str, Any]] = {}  # mint -> state
        if PF_DEBUG_TX:
            print(f"[engine] owner set to {self.owner}", flush=True)

    async def on_buy_filled(
        self,
        mint: str,
        *,
        tx_sig: Optional[str] = None,
        poll_sec: float = LEARN_BASE_DELAY_SEC
    ) -> None:
        """
        After a buy, prefer to learn size from the transaction meta (low-variance).
        If that fails, fall back to wallet polling with backoff.
        """
        amt_iu, dec = 0, 0

        # ---------- Path 1: learn from transaction signature (quick retries) ----------
        if tx_sig:
            if PF_DEBUG_TX:
                print(f"[txlearn] start mint={mint} from tx={tx_sig}", flush=True)

            # tiny warmup to avoid racing indexer
            await asyncio.sleep(max(0.0, float(TXLEARN_WARMUP)))

            tries = max(1, int(TXLEARN_TRIES))
            for k in range(1, tries + 1):
                try:
                    amt_iu, dec = _learn_from_tx_sig(self.owner, mint, tx_sig)
                    if amt_iu > 0:
                        if PF_DEBUG_TX:
                            print(f"[txlearn] success on try {k}: amount={amt_iu} dec={dec}", flush=True)
                        break
                except Exception as e:
                    if PF_DEBUG_TX:
                        print(f"[txlearn] try {k} error: {e}", flush=True)
                if amt_iu <= 0 and k < tries:
                    await asyncio.sleep(max(0.05, float(TXLEARN_DELAY)))

        # ---------- Path 2: fallback to polling if still zero ----------
        if amt_iu <= 0:
            attempts = max(1, int(LEARN_MAX_ATTEMPTS))
            delay = max(1.0, float(poll_sec))
            last_err = None

            for i in range(1, attempts + 1):
                await asyncio.sleep(delay)
                try:
                    amt_iu, dec = _sum_token_balance_iu(self.owner, mint)
                    if amt_iu > 0:
                        break
                except Exception as e:
                    last_err = e

                delay = min(delay * max(1.01, LEARN_BACKOFF), 30.0)
                if i == 1 or (i % max(1, LEARN_LOG_EVERY) == 0):
                    print(f"[pos] retry learn {mint} attempt {i}/{attempts}... last_bal={amt_iu}", flush=True)

            if amt_iu <= 0:
                if last_err:
                    print(f"[pos] giving up learn {mint}: {last_err}", flush=True)
                else:
                    print(f"[pos] giving up learn {mint}: still zero after retries", flush=True)
                return

        # ---------- Estimate entry price via small quote ----------
        sample = max(1, min(amt_iu // 100, amt_iu))
        try:
            q = _jup_quote(mint, SOL_MINT, sample)
            out_lamports = float(q.get("outAmount", 0))
            price_sol = (out_lamports / sample) / 1_000_000_000.0 if sample > 0 else None
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
            px_sol = (out_lamports / sample) / 1_000_000_000.0 if sample > 0 else None
        except Exception:
            return  # skip if quote fails briefly

        if px_sol is None:
            return

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
