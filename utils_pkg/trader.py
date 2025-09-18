# utils_pkg/trader.py
from __future__ import annotations

import asyncio
import math
import os
import time
from typing import Any, Dict, Optional, Tuple

import base58
import requests

from utils_pkg.solana_wallet import (
    load_keypair,
    get_pubkey_str,
    sign_and_send_b64_tx,
)

# Always use resolver-first learner after buys
from utils_pkg.txlearn import resolve_then_learn_position as _tx_resolve_then_learn


# =============================================================================
# Env & config helpers
# =============================================================================

def _envf(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)


def _envi(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return int(default)


def _is_true(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


# -----------------------------------------------------------------------------
# Global env/config
# -----------------------------------------------------------------------------
IS_DRYRUN        = _is_true(os.getenv("DRY_RUN"), False)

# General RPC for normal calls
RPC_URL          = (os.getenv("SOL_RPC_URL") or "").strip()

# TX RPC override for faster tx/meta visibility; falls back to general RPC
TX_RPC_URL       = (os.getenv("SOL_RPC_TX_URL") or RPC_URL or "https://api.mainnet-beta.solana.com").strip()

OWNER_FROM_ENV   = (os.getenv("PF_PP_ACCOUNT") or "").strip()
PF_DEBUG_TX      = _is_true(os.getenv("PF_DEBUG_TX"), False)

# --- Exit rules (can be tuned via env) ---
EXIT_TP1_PCT       = _envf("EXIT_TP1_PCT", 40.0)        # +40% → partial
EXIT_TP1_SELL_PCT  = _envf("EXIT_TP1_SELL_PCT", 0.40)   # sell 40%
EXIT_TP2_PCT       = _envf("EXIT_TP2_PCT", 100.0)       # +100% → more
EXIT_TP2_SELL_PCT  = _envf("EXIT_TP2_SELL_PCT", 0.30)   # sell 30%
EXIT_TSL_ARM_PCT   = _envf("EXIT_TSL_ARM_PCT", 60.0)    # arm TSL at +60%
EXIT_TSL_TRAIL_PCT = _envf("EXIT_TSL_TRAIL_PCT", 20.0)  # trail 20% off peak
EXIT_HARD_STOP_PCT = _envf("HARD_STOP_PCT", -32.0)      # hard stop

# --- Learner/engine pacing (match your logs) ---
POS_RETRY_MAX               = _envi("PF_POS_RETRY_MAX", 10)
POS_RETRY_DELAY_SEC         = _envf("PF_POS_RETRY_DELAY_SEC", 1.2)

PF_TXLEARN_RESOLVE_TRIES    = _envi("PF_TXLEARN_RESOLVE_TRIES", 18)
PF_TXLEARN_RESOLVE_DELAY_SEC= _envf("PF_TXLEARN_RESOLVE_DELAY_SEC", 1.0)
PF_TXLEARN_TRIES            = _envi("PF_TXLEARN_TRIES", 24)
PF_TXLEARN_DELAY_SEC        = _envf("PF_TXLEARN_DELAY_SEC", 1.0)
PF_TXLEARN_WARMUP_SEC       = _envf("PF_TXLEARN_WARMUP_SEC", 1.5)
PF_POS_LEARN_BASE_DELAY_SEC = _envf("PF_POS_LEARN_BASE_DELAY_SEC", 0.2)

# Jupiter / slippage / priority
SOL_MINT                = "So11111111111111111111111111111111111111112"
USDC_MINT               = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
TOKEN_PROGRAM_ID        = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID   = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


def _jup_as_legacy() -> bool:
    v = os.getenv("JUP_AS_LEGACY", "1")
    return str(v).lower() in {"1", "true", "yes", "y"}


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
# Small utils
# =============================================================================

def _assert_mint(m: str) -> None:
    """
    Fast sanity: base58 decode to 32 bytes.
    """
    try:
        if len(base58.b58decode(m)) != 32:
            raise ValueError("decoded length is not 32 bytes")
    except Exception as e:
        raise ValueError(f"Invalid mint '{m}': {e}")


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
    if "error" in js and js["error"]:
        raise RuntimeError(js["error"])
    return js.get("result")


def _rpc(method: str, params: list) -> Any:
    # Default to general RPC for standard calls; fall back to TX RPC if needed.
    return _rpc_json(RPC_URL or TX_RPC_URL, method, params)


# =============================================================================
# Balance helpers
# =============================================================================

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
    """
    Returns (raw_amount_iu, decimals) for owner's balance of mint.
    """
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
        # Some nodes error with "could not find mint" until accounts exist
        msg = str(e)
        if "could not find mint" in msg or "Invalid param" in msg:
            return _sum_token_balance_iu_via_program_scan(owner, mint)
        total, dec = _sum_token_balance_iu_via_program_scan(owner, mint)
        if total > 0:
            return total, dec
        raise


# =============================================================================
# Jupiter quote / swap
# =============================================================================

def _jup_quote(input_mint: str, output_mint: str, amount_iu: int) -> dict:
    _assert_mint(input_mint)
    _assert_mint(output_mint)
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
# Transaction helpers (fallback resolver)
# =============================================================================

def _get_transaction(sig: str) -> Optional[dict]:
    """
    Fetch parsed transaction from TX RPC (or general RPC as fallback).
    """
    enc = {
        "encoding": "jsonParsed",
        "maxSupportedTransactionVersion": 0,
        "commitment": "confirmed",
    }
    try:
        res = _rpc_json(TX_RPC_URL or RPC_URL, "getTransaction", [sig, enc])
        return res
    except Exception:
        # try general RPC once if TX RPC fails
        try:
            res = _rpc_json(RPC_URL or TX_RPC_URL, "getTransaction", [sig, enc])
            return res
        except Exception:
            return None


def _resolve_mint_from_tx(sig: str, owner: str) -> Optional[str]:
    """
    Parse getTransaction(meta.postTokenBalances) to find the mint we just received.
    """
    tx = _get_transaction(sig)
    if not tx:
        return None

    meta = tx.get("meta") or {}
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    # Build quick lookup: mint -> (pre_amount, post_amount) for owner
    def _amt(x: dict) -> int:
        ui = x.get("uiTokenAmount") or {}
        # use raw integer "amount" (string)
        try:
            return int((ui.get("amount") or "0"))
        except Exception:
            return 0

    pre_map: Dict[Tuple[str, str], int] = {}
    for b in pre:
        if b.get("owner") == owner:
            pre_map[(b.get("mint", ""), b.get("owner", ""))] = _amt(b)

    candidate_mint = None
    best_delta = 0

    for b in post:
        if b.get("owner") != owner:
            continue
        mint = b.get("mint", "")
        if not mint or mint == SOL_MINT:
            continue
        after_amt = _amt(b)
        before_amt = pre_map.get((mint, owner), 0)
        delta = after_amt - before_amt
        if delta > best_delta:
            best_delta = delta
            candidate_mint = mint

    # Fallback: if nothing delta-positive, still return the only owned mint if unique
    if not candidate_mint:
        owned_post = [b.get("mint") for b in post if b.get("owner") == owner and b.get("mint")]
        uniq = list({m for m in owned_post if m and m != SOL_MINT})
        if len(uniq) == 1:
            candidate_mint = uniq[0]

    try:
        if candidate_mint:
            _assert_mint(candidate_mint)
            return candidate_mint
    except Exception:
        return None
    return None


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
# Exit Engine (resolver-first + local fallback)
# =============================================================================

class PositionsExitEngine:
    """
    After a BUY fill:
      - resolver-first learner (handles '<...>pump' ids + signature inference) using TX RPC override
      - if the tx failed → bail early
      - if balance > 0 → record entry & start managing position
      - fallback: parse getTransaction on TX RPC to resolve mint & proceed

    Live trade ticks:
      - maintain peak/entry
      - TP1, TP2, trailing & hard-stop exits
    """

    def __init__(self) -> None:
        self.owner = OWNER_FROM_ENV or get_pubkey_str(load_keypair())
        self.pos: Dict[str, Dict[str, Any]] = {}  # mint -> state
        self._sema = asyncio.Semaphore(max(1, _envi("PF_MAX_CONCURRENT", 2)))
        if PF_DEBUG_TX:
            print(f"[engine] owner set to {self.owner}", flush=True)

    async def _record_entry(self, resolved_mint: str) -> None:
        # Snapshot wallet amount to set entry state precisely
        try:
            amt_iu, _dec = _sum_token_balance_iu(self.owner, resolved_mint)
        except Exception:
            amt_iu = 0

        # Estimate entry price via a small quote
        entry_sol: Optional[float] = None
        if amt_iu > 0:
            sample = max(1, min(amt_iu // 100, amt_iu))
            try:
                q = _jup_quote(resolved_mint, SOL_MINT, sample)
                out_lamports = float(q.get("outAmount", 0))
                if sample > 0 and out_lamports > 0:
                    entry_sol = (out_lamports / sample) / 1_000_000_000.0
            except Exception:
                entry_sol = None

        self.pos[resolved_mint] = {
            "size_iu": int(amt_iu),
            "tp1_done": False,
            "tp2_done": False,
            "entry_sol": entry_sol,
            "peak_sol": entry_sol,
            "last_ts": time.time(),
        }
        print(f"[pos] learned entry for {resolved_mint}: size_iu={amt_iu} entry≈{entry_sol} SOL", flush=True)
        _tg_send(f"📥 ENTRY {resolved_mint} size_iu={amt_iu} entry≈{(entry_sol or 0):.6f} SOL")

    async def on_buy_filled(self, raw_id_or_mint: str, *, tx_sig: Optional[str] = None) -> None:
        """
        Kick off resolver-first learning after a fill. raw_id_or_mint can be a base58
        mint or a pump id ending with 'pump'.
        """
        if not tx_sig:
            if PF_DEBUG_TX:
                print("[exit] on_buy_filled: missing tx_sig — skip", flush=True)
            return

        async with self._sema:
            last_bal_ui = 0.0
            last_res: Optional[Dict[str, Any]] = None

            # --- Primary path: tx-learner (with TX RPC override) ---
            for i in range(1, POS_RETRY_MAX + 1):
                try:
                    last_res = await _tx_resolve_then_learn(
                        raw_id_or_mint=raw_id_or_mint,
                        tx_sig=tx_sig,
                        owner=self.owner,
                        rpc_url_tx=TX_RPC_URL,
                        rpc_url_general=RPC_URL or TX_RPC_URL,
                    )
                except Exception as e:
                    print(f"[pos] resolve_then_learn error ({i}/{POS_RETRY_MAX}): {e}", flush=True)
                    last_res = None

                if last_res:
                    status = last_res.get("status") or {}
                    if status.get("err") is not None:
                        print(f"[pos] tx failed (sig={tx_sig}) — err={status.get('err')}", flush=True)
                        return

                    last_bal_ui = float(last_res.get("owner_balance_ui") or 0.0)
                    if last_bal_ui > 0:
                        # Try to use their resolved mint if provided
                        resolved_mint = last_res.get("resolved_mint") or await self._resolve_mint_text(raw_id_or_mint, tx_sig)
                        if not resolved_mint:
                            resolved_mint = raw_id_or_mint
                        await self._record_entry(resolved_mint)
                        return  # done

                if i < POS_RETRY_MAX:
                    await asyncio.sleep(POS_RETRY_DELAY_SEC)

            # --- Fallback path: resolve from TX directly on TX RPC ---
            # small warmup to let the tx land on indexers
            if PF_TXLEARN_WARMUP_SEC > 0:
                await asyncio.sleep(PF_TXLEARN_WARMUP_SEC)

            resolved_mint = None
            for j in range(1, PF_TXLEARN_RESOLVE_TRIES + 1):
                resolved_mint = _resolve_mint_from_tx(tx_sig, self.owner)
                if resolved_mint:
                    break
                if j < PF_TXLEARN_RESOLVE_TRIES:
                    await asyncio.sleep(PF_TXLEARN_RESOLVE_DELAY_SEC)

            if not resolved_mint:
                if PF_DEBUG_TX:
                    print(f"[pos] fallback: could not resolve mint from tx (sig={tx_sig}) id={raw_id_or_mint}", flush=True)
                return

            # Once we have a mint, poll balance briefly until wallet reflects it
            have_balance = False
            for k in range(1, PF_TXLEARN_TRIES + 1):
                try:
                    amt_iu, _ = _sum_token_balance_iu(self.owner, resolved_mint)
                except Exception:
                    amt_iu = 0
                if amt_iu > 0:
                    have_balance = True
                    break
                if k < PF_TXLEARN_TRIES:
                    await asyncio.sleep(PF_TXLEARN_DELAY_SEC)

            if not have_balance and PF_DEBUG_TX:
                print(f"[pos] fallback: mint={resolved_mint} but no balance yet (sig={tx_sig})", flush=True)

            await self._record_entry(resolved_mint)

    async def _resolve_mint_text(self, raw_id_or_mint: str, tx_sig: str) -> Optional[str]:
        """
        Resolve a displayable/base58 mint string for exit logic & quoting.
        Prefer the pump_trader resolvers; fall back to sanity-checking the input.
        """
        try:
            from utils_pkg.pump_trader import resolve_true_mint, resolve_true_mint_from_pump_id
        except Exception:
            resolve_true_mint = None  # type: ignore
            resolve_true_mint_from_pump_id = None  # type: ignore

        # 1) Signature-aided resolver (best)
        if resolve_true_mint is not None:
            try:
                m = resolve_true_mint(raw_id_or_mint, signature=tx_sig)
                if asyncio.iscoroutine(m):
                    m = await m
                if m:
                    return m
            except Exception:
                pass

        # 2) Pump-id only resolver
        if resolve_true_mint_from_pump_id is not None:
            try:
                m = resolve_true_mint_from_pump_id(raw_id_or_mint)
                if asyncio.iscoroutine(m):
                    m = await m
                if m:
                    return m
            except Exception:
                pass

        # 3) Already a base58-looking key?
        try:
            _assert_mint(raw_id_or_mint)
            return raw_id_or_mint
        except Exception:
            s = (raw_id_or_mint or "").strip()
            if s.endswith("pump"):
                # tiny best-effort: some ids appear like "<base58>pump"
                try:
                    _assert_mint(s[:-4])
                    return s[:-4]
                except Exception:
                    return None
            return None

    async def _refresh_size_iu(self, mint: str) -> int:
        try:
            amt_iu, _dec = _sum_token_balance_iu(self.owner, mint)
            self.pos[mint]["size_iu"] = int(amt_iu)
            return int(amt_iu)
        except Exception:
            return int(self.pos.get(mint, {}).get("size_iu") or 0)

    async def on_trade_tick(self, raw_event: Dict[str, Any]) -> None:
        """
        Optional live-tick hook. Call this with each PumpPortal trade tick.
        Maintains TP/TSL logic using small Jupiter quotes for pricing.
        """
        mint = str(
            raw_event.get("mint")
            or raw_event.get("mintAddress")
            or raw_event.get("mint_address")
            or ""
        ).strip()
        if not mint:
            return

        # Normalize to our stored (resolved) key if present
        if mint not in self.pos:
            resolved = await self._resolve_mint_text(mint, raw_event.get("signature") or "")
            if not resolved or resolved not in self.pos:
                return
            mint = resolved

        st = self.pos.get(mint) or {}
        if (st.get("size_iu") or 0) <= 0:
            return

        # Price via small quote (~0.5–1% of position)
        sample = max(1, min(int(st["size_iu"]) // 200, int(st["size_iu"])))
        try:
            q = _jup_quote(mint, SOL_MINT, sample)
            out_lamports = float(q.get("outAmount", 0))
            px_sol = (out_lamports / sample) / 1_000_000_000.0 if sample > 0 and out_lamports > 0 else None
        except Exception:
            return  # transient quote failure
        if px_sol is None:
            return

        if not st.get("entry_sol"):
            st["entry_sol"] = px_sol
            st["peak_sol"] = px_sol

        st["peak_sol"] = max(st.get("peak_sol") or px_sol, px_sol)
        st["last_ts"] = time.time()

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
            await self._refresh_size_iu(mint)
            return

        # TP1
        if (not st["tp1_done"]) and (pnl_pct >= EXIT_TP1_PCT):
            print(f"[exit] TP1 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP1_SELL_PCT:.0%}")
            _tg_send(f"🎯 TP1 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP1_SELL_PCT:.0%}")
            await sell_on_jupiter(mint, pct_of_wallet=EXIT_TP1_SELL_PCT)
            st["tp1_done"] = True
            await self._refresh_size_iu(mint)
            return

        # TP2
        if (not st["tp2_done"]) and (pnl_pct >= EXIT_TP2_PCT):
            print(f"[exit] TP2 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP2_SELL_PCT:.0%}")
            _tg_send(f"🎯 TP2 {mint} pnl={pnl_pct:.1f}% sell={EXIT_TP2_SELL_PCT:.0%}")
            await sell_on_jupiter(mint, pct_of_wallet=EXIT_TP2_SELL_PCT)
            st["tp2_done"] = True
            await self._refresh_size_iu(mint)
            return

        # Trailing stop (armed)
        if pnl_pct >= EXIT_TSL_ARM_PCT and drawdown_from_peak_pct <= -EXIT_TSL_TRAIL_PCT:
            print(f"[exit] TSL {mint} drop_from_peak={drawdown_from_peak_pct:.1f}% -> exit")
            _tg_send(f"🪢 TSL {mint} drop={drawdown_from_peak_pct:.1f}% -> exit")
            await sell_on_jupiter(mint, pct_of_wallet=1.0)
            await self._refresh_size_iu(mint)
            return


# exported singleton used by scripts/main.py
exit_engine = PositionsExitEngine()

__all__ = [
    "IS_DRYRUN",
    "sell_on_jupiter",
    "exit_engine",
]
