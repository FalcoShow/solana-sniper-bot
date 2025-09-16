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
# Exit Engine (positions + rules) — FINAL PATCH
# =============================================================================
from __future__ import annotations
import os, time, asyncio, requests
from typing import Dict, Any, Optional, Tuple

# import your existing helpers/constants:
# - load_keypair, get_pubkey_str
# - _jup_quote, _sum_token_balance_iu, sell_on_jupiter
# - SOL_MINT, EXIT_* envs, _envf, _envi, _tg_send
# Make sure they’re already in this module’s scope.

def _extract_mint(msg: Dict[str, Any]) -> str:
    return (msg.get("mint") or msg.get("mintAddress") or msg.get("mint_address") or "").strip()

def _normalize_mint(m: str) -> str:
    m = (m or "").strip()
    # Many pump mints arrive as "<mint>pump"; strip the suffix if present.
    if len(m) > 44 and m.endswith("pump"):
        return m[:-4]
    return m

# --- Config knobs (env) -------------------------------------------------------
OWNER_FROM_ENV        = (os.getenv("PF_PP_ACCOUNT") or "").strip()
PF_DEBUG_TX           = str(os.getenv("PF_DEBUG_TX", "0")).strip().lower() in {"1","true","yes","y","on"}
PF_TXLEARN_DUMP       = str(os.getenv("PF_TXLEARN_DUMP", "0")).strip().lower() in {"1","true","yes","y","on"}

LEARN_BASE_DELAY_SEC  = _envf("PF_POS_LEARN_BASE_DELAY_SEC", 6.0)
LEARN_MAX_ATTEMPTS    = _envi("PF_POS_LEARN_MAX_ATTEMPTS", 10)
LEARN_BACKOFF         = _envf("PF_POS_LEARN_BACKOFF", 1.5)
LEARN_LOG_EVERY       = _envi("PF_POS_LEARN_LOG_EVERY", 3)

TXLEARN_TRIES         = _envi("PF_TXLEARN_TRIES", 3)           # quick attempts to read tx meta
TXLEARN_DELAY         = _envf("PF_TXLEARN_DELAY_SEC", 0.7)     # between those tx meta attempts
TXLEARN_WARMUP        = _envf("PF_TXLEARN_WARMUP_SEC", 0.25)   # initial short wait for meta to land
TXLEARN_PREFETCH_ATAS = str(os.getenv("PF_TXLEARN_PREFETCH_ATAS", "1")).strip().lower() in {"1","true","yes","y","on"}

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

def _tx_get(sig: str) -> Optional[Dict[str, Any]]:
    # Try confirmed first (faster visibility), then finalized
    for commitment in ("confirmed", "finalized"):
        try:
            return _rpc_tx("getTransaction", [
                sig,
                {"encoding": "jsonParsed", "commitment": commitment, "maxSupportedTransactionVersion": 0},
            ])
        except Exception:
            continue
    return None

def _owner_token_accounts(owner: str, mint: str) -> set[str]:
    """Fetch the owner's token accounts for this (normalized) mint."""
    mint = _normalize_mint(mint)
    try:
        res = _rpc_tx("getTokenAccountsByOwner", [
            owner,
            {"mint": mint},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ])
        out = set()
        for it in (res or {}).get("value", []):
            try:
                out.add(it["pubkey"])
            except Exception:
                continue
        if PF_DEBUG_TX:
            print(f"[txlearn] owner ATAs for {mint}: {len(out)} found", flush=True)
        return out
    except Exception as e:
        if PF_DEBUG_TX:
            print(f"[txlearn] getTokenAccountsByOwner failed for {mint}: {e}", flush=True)
        return set()

def _learn_from_tx_sig(owner: str, mint: str, tx_sig: str, owner_atas: Optional[set[str]] = None) -> Tuple[int, int]:
    """
    Learn post-balance of `mint` for `owner` from a parsed transaction.
    Matching order:
      1) direct postTokenBalances owner+mint
      2) by accountIndex -> accountKeys ∈ owner_atas
      3) by positive delta (pre→post) for keys ∈ owner_atas
    Returns (amount_iu, decimals). (0,0) if not found.
    """
    mint = _normalize_mint(mint)
    res = _tx_get(tx_sig)
    if not res:
        if PF_DEBUG_TX:
            print(f"[txlearn] getTransaction returned no result (sig={tx_sig})", flush=True)
        return 0, 0

    meta = res.get("meta") or {}
    message = (res.get("transaction") or {}).get("message") or {}
    account_keys = message.get("accountKeys") or []

    post_bal = meta.get("postTokenBalances") or []
    pre_bal  = meta.get("preTokenBalances") or []

    if PF_DEBUG_TX:
        print(f"[txlearn] meta: pre={len(pre_bal)} post={len(post_bal)}", flush=True)
        if PF_TXLEARN_DUMP and post_bal:
            try:
                preview = [
                    {
                        "idx": b.get("accountIndex"),
                        "mint": b.get("mint"),
                        "owner": b.get("owner"),
                        "amt": (b.get("uiTokenAmount") or {}).get("amount"),
                        "dec": (b.get("uiTokenAmount") or {}).get("decimals"),
                    } for b in post_bal[:6]
                ]
                print(f"[txlearn] post preview: {preview}", flush=True)
            except Exception:
                pass

    def _to_map(bals: list) -> Dict[int, Tuple[str,int,int]]:
        m: Dict[int, Tuple[str,int,int]] = {}
        for b in bals:
            try:
                idx = int(b.get("accountIndex"))
                ui  = b.get("uiTokenAmount") or {}
                amt = int(str(ui.get("amount", "0")))
                dec = int(ui.get("decimals", 0) or 0)
                m[idx] = (b.get("mint",""), amt, dec)
            except Exception:
                continue
        return m

    pre_map  = _to_map(pre_bal)
    post_map = _to_map(post_bal)

    # 1) direct owner+mint
    for b in post_bal:
        try:
            if _normalize_mint(b.get("mint")) != mint:
                continue
            if b.get("owner") == owner:
                ui  = b.get("uiTokenAmount") or {}
                amt = int(str(ui.get("amount", "0")))
                dec = int(ui.get("decimals", 0) or 0)
                if PF_DEBUG_TX:
                    print(f"[txlearn] direct owner match idx={b.get('accountIndex')} amt={amt} dec={dec}", flush=True)
                return amt, dec
        except Exception:
            continue

    # 2) match by accountIndex -> accountKey ∈ owner_atas
    if owner_atas:
        for b in post_bal:
            try:
                if _normalize_mint(b.get("mint")) != mint:
                    continue
                idx = int(b.get("accountIndex"))
                key_entry = account_keys[idx]
                acct_key = key_entry if isinstance(key_entry, str) else (key_entry or {}).get("pubkey")
                if acct_key and acct_key in owner_atas:
                    ui  = b.get("uiTokenAmount") or {}
                    amt = int(str(ui.get("amount", "0")))
                    dec = int(ui.get("decimals", 0) or 0)
                    if PF_DEBUG_TX:
                        print(f"[txlearn] owner ATA match idx={idx} acct={acct_key} amt={amt} dec={dec}", flush=True)
                    return amt, dec
            except Exception:
                continue

    # 3) positive delta on any owner ATA
    if owner_atas and post_map:
        for idx, (m_mint, post_amt, dec) in post_map.items():
            if _normalize_mint(m_mint) != mint:
                continue
            key_entry = account_keys[idx]
            acct_key = key_entry if isinstance(key_entry, str) else (key_entry or {}).get("pubkey")
            if not acct_key or acct_key not in owner_atas:
                continue
            pre_amt = (pre_map.get(idx) or ("", 0, dec))[1]
            if post_amt > pre_amt:
                if PF_DEBUG_TX:
                    print(f"[txlearn] delta+ owner ATA idx={idx} acct={acct_key} {pre_amt}->{post_amt} (dec={dec})", flush=True)
                return post_amt, dec

    if PF_DEBUG_TX:
        print(f"[txlearn] no match for owner={owner} mint={mint} in tx={tx_sig}", flush=True)
    return 0, 0

class PositionsExitEngine:
    def __init__(self) -> None:
        self.owner = OWNER_FROM_ENV or get_pubkey_str(load_keypair())
        self.pos: Dict[str, Dict[str, Any]] = {}  # mint -> state
        if PF_DEBUG_TX:
            print(f"[engine] owner set to {self.owner}", flush=True)

    async def on_buy_filled(self, mint: str, *, tx_sig: Optional[str] = None, poll_sec: float = LEARN_BASE_DELAY_SEC) -> None:
        """
        After a buy, prefer to learn size from the transaction meta (instant/low-variance).
        If that fails, fall back to wallet polling with backoff.
        """
        mint = _normalize_mint(mint)
        amt_iu, dec = 0, 0

        # ---------- quick warmup so 'confirmed' has postTokenBalances ----------
        if tx_sig and TXLEARN_WARMUP > 0:
            await asyncio.sleep(max(0.0, float(TXLEARN_WARMUP)))

        # ---------- Path 1: learn from transaction signature (quick retries) ----------
        owner_atas: set[str] = set()
        if tx_sig:
            if PF_DEBUG_TX:
                print(f"[txlearn] start mint={mint} from tx={tx_sig}", flush=True)

            if TXLEARN_PREFETCH_ATAS:
                owner_atas = _owner_token_accounts(self.owner, mint)

            tries = max(1, int(TXLEARN_TRIES))
            for k in range(1, tries + 1):
                try:
                    amt_iu, dec = _learn_from_tx_sig(self.owner, mint, tx_sig, owner_atas or None)
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
                    # ensure we poll with normalized mint
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
        mint = _normalize_mint(_extract_mint(msg))
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

        # Hard stop
        if pnl_pct <= EXIT_HARD_STOP_PCT:
            print(f"[exit] HARD STOP {mint} pnl={pnl_pct:.1f}%")
            _tg_send(f"⛔ HARD STOP {mint} pnl={pnl_pct:.1f}%")
            await sell_on_jupiter(mint, pct_of_wallet=1.0)
            st["size_iu"] = 0
            return

        # TP1
        if (not st["tp1_done"]) and (pnl_pct >= EXIT_TP1_PCT):
            print(f"[exit] TP1 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP1_SELL_PCT:.0%}")
            _tg_send(f"🎯 TP1 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP1_SELL_PCT:.0%}")
            await sell_on_jupiter(mint, pct_of_wallet=EXIT_TP1_SELL_PCT)
            st["tp1_done"] = True
            return

        # TP2
        if (not st["tp2_done"]) and (pnl_pct >= EXIT_TP2_PCT):
            print(f"[exit] TP2 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP2_SELL_PCT:.0%}")
            _tg_send(f"🎯 TP2 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP2_SELL_PCT:.0%}")
            await sell_on_jupiter(mint, pct_of_wallet=EXIT_TP2_SELL_PCT)
            st["tp2_done"] = True
            return

        # Trailing stop (armed)
        if pnl_pct >= EXIT_TSL_ARM_PCT:
            if drawdown_from_peak_pct <= -EXIT_TSL_TRAIL_PCT:
                print(f"[exit] TSL {mint} drop_from_peak={drawdown_from_peak_pct:.1f}% -> exit")
                _tg_send(f"🪢 TSL {mint} drop={drawdown_from_peak_pct:.1f}% -> exit")
                await sell_on_jupiter(mint, pct_of_wallet=1.0)
                st["size_iu"] = 0
                return

# exported singleton
exit_engine = PositionsExitEngine()
