# utils_pkg/trader.py
from __future__ import annotations

"""
utils_pkg/trader.py — Canonical trade execution primitives (P0)
==============================================================

This module is the *execution glue* between:
  - wallet/keypair primitives (utils_pkg/solana_wallet.py)
  - router swap execution (utils_pkg/router.py)  [canonical money pipe]
  - confirmation + fill inference (RPC)
  - optional ledger / DB event logging
  - optional exit registration wiring (execution/exits_wiring.py)

Design goals (God-Tier / production-first):
  ✅ No side effects at import time (no tasks spawned, no network calls)
  ✅ Deterministic + safe: retries, timeouts, DRYRUN, idempotency (best-effort)
  ✅ Fill-tracking: confirm signature, infer deltas from balances (token + SOL)
  ✅ Backwards compatibility:
        - keeps sell_on_jupiter(mint, pct_of_wallet, slippage_bps) → Optional[str]
        - keeps token balance helpers used by scripts/tools

Important:
  - Router is canonical for swap building/sending. Trader confirms + infers fills.
  - Exit brain is canonical elsewhere. Trader may *optionally* register filled entries
    into the exit supervisor when available, but does not own exit logic.
"""

import asyncio
import contextlib
import os
import random
import time
from dataclasses import asdict, dataclass
from typing import Any

import requests

from logging_setup import get_logger
from utils_pkg.solana_wallet import get_pubkey_str, load_keypair

log = get_logger()

LAMPORTS_PER_SOL = 1_000_000_000

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else v.strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int((_env_str(name, str(default)) or str(default)).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float((_env_str(name, str(default)) or str(default)).strip())
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


# DRYRUN detection (any of these turns it on)
IS_DRYRUN: bool = (
    _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)
)

# RPC (confirmation + balance inference)
TX_RPC_URL: str = _env_str("TX_RPC_URL") or _env_str("SOL_RPC_URL") or _env_str("SOLANA_RPC_URL")

TX_COMMITMENT: str = (
    _env_str("PF_TXLEARN_COMMITMENT")
    or _env_str("SOL_COMMITMENT")
    or _env_str("TX_COMMITMENT")
    or "confirmed"
)

_RPC_TIMEOUT_SEC = _env_float("TRADER_RPC_TIMEOUT_SEC", _env_float("ROUTER_RPC_TIMEOUT_SEC", 10.0))
_RPC_RETRIES = max(0, _env_int("TRADER_RPC_RETRIES", 2))
_RPC_RETRY_BASE_SLEEP = _env_float("TRADER_RPC_RETRY_BASE_SLEEP_SEC", 0.35)
_RPC_RETRY_JITTER = _env_float("TRADER_RPC_RETRY_JITTER_SEC", 0.20)

# Confirmation controls
CONFIRM_ENABLED = _env_bool("TRADER_CONFIRM_ENABLED", True)
CONFIRM_TIMEOUT_SEC = _env_float("TRADER_CONFIRM_TIMEOUT_SEC", 45.0)
CONFIRM_POLL_SEC = _env_float("TRADER_CONFIRM_POLL_SEC", 1.25)
CONFIRM_COMMITMENT = _env_str("TRADER_CONFIRM_COMMITMENT", TX_COMMITMENT)

# Balance inference controls
BALANCE_RETRIES = max(0, _env_int("TRADER_BALANCE_RETRIES", 2))
BALANCE_RETRY_SLEEP = _env_float("TRADER_BALANCE_RETRY_SLEEP_SEC", 0.35)

# Best-effort in-process dedupe window to prevent double-sends on hot paths
IDEMPOTENCY_TTL_SEC = _env_float("TRADER_IDEMPOTENCY_TTL_SEC", 45.0)

_UA = {"user-agent": "pnlsniper/trader", "accept": "application/json"}


# Optional budget hooks (fail-open)
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover

    class _BudgetNoop:
        def before_rpc(self, method: str, *, weight: int | None = None) -> bool:
            return True

        def after_rpc(self, method: str, *, weight: int | None = None) -> None:
            return None

    budget = _BudgetNoop()  # type: ignore


# Optional: structured DB logger (fail-open)
try:
    from db.logger import log_trade_event  # type: ignore
except Exception:  # pragma: no cover

    async def log_trade_event(*_a, **_k):  # type: ignore
        return None


# Optional: execution exit supervisor wiring (fail-open)
try:
    from execution.exits_wiring import register_entry as _register_exit_entry  # type: ignore
except Exception:  # pragma: no cover
    _register_exit_entry = None  # type: ignore


# Router: canonical swap sender(s). We adapt to whatever signature exists.
try:
    import utils_pkg.router as router  # type: ignore
except Exception:  # pragma: no cover
    router = None  # type: ignore


# ──────────────────────────────────────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RpcResult:
    ok: bool
    value: dict[str, Any]
    err: str | None = None


@dataclass
class FillSnapshot:
    ts: float
    owner: str
    mint: str
    token_raw: int
    sol_lamports: int


@dataclass
class TradeFill:
    ok: bool
    signature: str | None
    mint: str
    owner: str
    side: str  # "BUY" | "SELL" | "SWAP"
    amount_in_raw: int | None = None
    amount_out_raw: int | None = None
    sol_lamports_delta: int | None = None
    token_raw_delta: int | None = None
    confirmed: bool = False
    reason: str = ""
    meta: dict[str, Any] = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["meta"] = d["meta"] or {}
        return d


# ──────────────────────────────────────────────────────────────────────────────
# Internal: simple idempotency (process-local)
# ──────────────────────────────────────────────────────────────────────────────


_idem_lock = asyncio.Lock()
_idem: dict[str, float] = {}  # key -> expiry_ts


async def _idem_check_and_set(key: str) -> bool:
    """
    Returns True if allowed (not seen recently), and sets TTL.
    Returns False if duplicate within TTL.
    """
    if IDEMPOTENCY_TTL_SEC <= 0:
        return True
    now = time.time()
    async with _idem_lock:
        # prune
        dead = [k for k, exp in _idem.items() if exp <= now]
        for k in dead:
            _idem.pop(k, None)

        exp = _idem.get(key)
        if exp and exp > now:
            return False

        _idem[key] = now + IDEMPOTENCY_TTL_SEC
        return True


# ──────────────────────────────────────────────────────────────────────────────
# RPC client (requests in worker thread)
# ──────────────────────────────────────────────────────────────────────────────


def _rpc_post_blocking(method: str, params: list) -> dict[str, Any]:
    if not TX_RPC_URL:
        raise RuntimeError("RPC URL not configured (TX_RPC_URL / SOL_RPC_URL / SOLANA_RPC_URL)")

    if not budget.before_rpc(method):
        # behave as if the call failed pre-flight (budget exceeded)
        return {"jsonrpc": "2.0", "id": 1, "result": None}

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(
            TX_RPC_URL,
            json=payload,
            timeout=_RPC_TIMEOUT_SEC,
            headers=_UA,
        )
        r.raise_for_status()
        return r.json()
    finally:
        with contextlib.suppress(Exception):
            budget.after_rpc(method)


async def rpc_call(method: str, params: list) -> RpcResult:
    """
    Safe RPC call with retries and jitter. Returns RpcResult(ok, value, err).
    """
    last_err: str | None = None
    for attempt in range(_RPC_RETRIES + 1):
        try:
            js = await asyncio.to_thread(_rpc_post_blocking, method, params)
            if isinstance(js, dict) and "error" in js and js["error"]:
                last_err = str(js["error"])
                raise RuntimeError(last_err)
            return RpcResult(ok=True, value=js or {}, err=None)
        except Exception as e:
            last_err = repr(e)
            if attempt >= _RPC_RETRIES:
                break
            sleep = _RPC_RETRY_BASE_SLEEP * (2**attempt) + random.random() * _RPC_RETRY_JITTER
            await asyncio.sleep(sleep)
    return RpcResult(ok=False, value={}, err=last_err or "rpc_failed")


# ──────────────────────────────────────────────────────────────────────────────
# Balance helpers (SOL + SPL token raw)
# ──────────────────────────────────────────────────────────────────────────────


def _sum_token_balance_raw_from_resp(js: dict[str, Any]) -> int:
    vals = (js or {}).get("result", {}).get("value", [])
    if not isinstance(vals, list) or not vals:
        return 0

    total = 0
    for it in vals:
        # parsed amount path
        try:
            amt = it["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
            total += int(amt)
            continue
        except Exception:
            pass

        # fallback uiAmount path
        try:
            tok = it["account"]["data"]["parsed"]["info"]["tokenAmount"]
            ui = float(tok.get("uiAmount") or 0.0)
            dec = int(tok.get("decimals") or 0)
            total += int(round(ui * (10**dec)))
        except Exception:
            continue

    return int(total)


async def get_token_balance_raw(owner: str, mint: str, *, commitment: str | None = None) -> int:
    """
    Sum raw SPL balance across all token accounts for `owner` and `mint`.
    """
    c = commitment or TX_COMMITMENT
    for attempt in range(BALANCE_RETRIES + 1):
        try:
            res = await rpc_call(
                "getTokenAccountsByOwner",
                [owner, {"mint": mint}, {"encoding": "jsonParsed", "commitment": c}],
            )
            if not res.ok:
                raise RuntimeError(res.err or "rpc_fail")
            raw = _sum_token_balance_raw_from_resp(res.value)
            return int(raw)
        except Exception as e:
            if attempt >= BALANCE_RETRIES:
                log.event("RPC_BALANCE_ERR", mint=mint, err=str(e)[:200])
                return 0
            await asyncio.sleep(BALANCE_RETRY_SLEEP)


async def get_sol_balance_lamports(owner: str, *, commitment: str | None = None) -> int:
    c = commitment or TX_COMMITMENT
    for attempt in range(BALANCE_RETRIES + 1):
        try:
            res = await rpc_call("getBalance", [owner, {"commitment": c}])
            if not res.ok:
                raise RuntimeError(res.err or "rpc_fail")
            lamports = int(((res.value or {}).get("result") or {}).get("value") or 0)
            return lamports
        except Exception as e:
            if attempt >= BALANCE_RETRIES:
                log.event("RPC_SOL_BAL_ERR", err=str(e)[:200])
                return 0
            await asyncio.sleep(BALANCE_RETRY_SLEEP)


async def get_wallet_token_balance_raw(mint: str) -> int:
    kp = load_keypair()
    owner = get_pubkey_str(kp)
    return await get_token_balance_raw(owner, mint)


async def snapshot_wallet(owner: str, mint: str, *, commitment: str | None = None) -> FillSnapshot:
    tok = await get_token_balance_raw(owner, mint, commitment=commitment)
    sol = await get_sol_balance_lamports(owner, commitment=commitment)
    return FillSnapshot(
        ts=time.time(), owner=owner, mint=mint, token_raw=int(tok), sol_lamports=int(sol)
    )


# ──────────────────────────────────────────────────────────────────────────────
# Confirmation helpers
# ──────────────────────────────────────────────────────────────────────────────


async def get_signature_status(signature: str, *, commitment: str | None = None) -> dict[str, Any]:
    """
    Returns the raw entry from getSignatureStatuses for a single signature.
    """
    c = commitment or CONFIRM_COMMITMENT
    res = await rpc_call("getSignatureStatuses", [[signature], {"searchTransactionHistory": True}])
    if not res.ok:
        return {"ok": False, "err": res.err}
    value = ((res.value or {}).get("result") or {}).get("value") or []
    st = value[0] if isinstance(value, list) and value else None
    out = {"ok": True, "status": st, "commitment": c}
    return out


async def wait_for_confirmation(
    signature: str, *, timeout_sec: float = CONFIRM_TIMEOUT_SEC
) -> tuple[bool, dict[str, Any]]:
    """
    Poll getSignatureStatuses until confirmed/finalized (or timeout).
    Returns (confirmed_ok, status_dict).
    """
    if not CONFIRM_ENABLED:
        return True, {"skipped": True}

    deadline = time.time() + max(1.0, float(timeout_sec))
    last: dict[str, Any] = {}
    while time.time() < deadline:
        st = await get_signature_status(signature)
        last = st or {}
        if st.get("ok") and st.get("status"):
            s = st["status"]
            # Any "err" means the tx failed.
            if s.get("err") is not None:
                return False, {"confirmed": True, "err": s.get("err"), "status": s}
            # If it has confirmations=None it is finalized (Solana convention).
            if (
                s.get("confirmationStatus") in ("confirmed", "finalized")
                or s.get("confirmations") is None
            ):
                return True, {"confirmed": True, "status": s}
        await asyncio.sleep(CONFIRM_POLL_SEC)
    return False, {"confirmed": False, "timeout": True, "last": last}


# ──────────────────────────────────────────────────────────────────────────────
# Router adapters (best-effort)
# ──────────────────────────────────────────────────────────────────────────────


async def _router_sell_token_raw(
    *,
    mint: str,
    amount_raw: int,
    slippage_bps: int | None = None,
    token_program: Any = None,
) -> str | None:
    """
    Calls utils_pkg.router.sell_via_lightning if present.
    Returns signature (str) or None.
    """
    if router is None:
        return None

    fn = getattr(router, "sell_via_lightning", None)
    if fn is None:
        return None

    # Prefer named kwargs; fallback to positional if needed.
    try:
        if slippage_bps is not None:
            return await fn(
                mint=mint,
                token_program=token_program,
                amount_raw=int(amount_raw),
                slippage_bps=int(slippage_bps),
            )
        return await fn(mint=mint, token_program=token_program, amount_raw=int(amount_raw))
    except TypeError:
        # older call signature support
        return await fn(mint, token_program, int(amount_raw))


async def _router_buy_token_raw(
    *,
    mint: str,
    amount_in_lamports: int,
    slippage_bps: int | None = None,
    token_program: Any = None,
) -> str | None:
    """
    Best-effort buy adapter.
    If your router exposes buy_via_lightning, we use it; otherwise None.
    """
    if router is None:
        return None

    fn = getattr(router, "buy_via_lightning", None)
    if fn is None:
        return None

    try:
        if slippage_bps is not None:
            return await fn(
                mint=mint,
                token_program=token_program,
                amount_in_lamports=int(amount_in_lamports),
                slippage_bps=int(slippage_bps),
            )
        return await fn(
            mint=mint, token_program=token_program, amount_in_lamports=int(amount_in_lamports)
        )
    except TypeError:
        return await fn(mint, token_program, int(amount_in_lamports))


# ──────────────────────────────────────────────────────────────────────────────
# Canonical trade helpers (confirm + infer fill deltas)
# ──────────────────────────────────────────────────────────────────────────────


async def sell_token_raw(
    mint: str,
    amount_raw: int,
    *,
    reason: str = "manual",
    slippage_bps: int | None = None,
    owner: str | None = None,
    confirm: bool = True,
) -> TradeFill:
    """
    Sell SPL token `mint` amount_raw → (typically) SOL via router.

    - Confirms the tx (optional)
    - Infers deltas from wallet token/SOL balances (best-effort)
    """
    kp = load_keypair()
    owner = owner or get_pubkey_str(kp)

    amount_raw = int(max(0, amount_raw))
    if amount_raw <= 0:
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner,
            side="SELL",
            confirmed=False,
            reason="amount_raw<=0",
            meta={},
        )

    # idempotency: same mint+amount+reason within TTL won’t send twice
    idem_key = f"SELL:{owner}:{mint}:{amount_raw}:{reason}"
    if not await _idem_check_and_set(idem_key):
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner,
            side="SELL",
            confirmed=False,
            reason="idempotency_block",
            meta={},
        )

    pre = await snapshot_wallet(owner, mint, commitment=TX_COMMITMENT)

    if IS_DRYRUN:
        log.event("SELL_DRYRUN", mint=mint, amount_raw=amount_raw, reason=reason)
        return TradeFill(
            ok=True,
            signature=None,
            mint=mint,
            owner=owner,
            side="SELL",
            confirmed=True,
            reason="dryrun",
            meta={"dryrun": True},
        )

    sig = await _router_sell_token_raw(mint=mint, amount_raw=amount_raw, slippage_bps=slippage_bps)
    if not sig:
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner,
            side="SELL",
            confirmed=False,
            reason="router_unavailable_or_failed",
            meta={},
        )

    confirmed = True
    cmeta: dict[str, Any] = {}
    if confirm:
        confirmed, cmeta = await wait_for_confirmation(sig)

    post = await snapshot_wallet(owner, mint, commitment=TX_COMMITMENT)

    token_delta = int(post.token_raw - pre.token_raw)
    sol_delta = int(post.sol_lamports - pre.sol_lamports)

    out = TradeFill(
        ok=bool(confirmed),
        signature=str(sig),
        mint=mint,
        owner=owner,
        side="SELL",
        amount_in_raw=int(amount_raw),
        amount_out_raw=None,
        sol_lamports_delta=sol_delta,
        token_raw_delta=token_delta,
        confirmed=bool(confirmed),
        reason="ok" if confirmed else "confirm_failed",
        meta={"confirm": cmeta, "pre": asdict(pre), "post": asdict(post)},
    )

    # best-effort structured log
    with contextlib.suppress(Exception):
        await log_trade_event(
            type="sell_fill" if confirmed else "sell_unconfirmed",
            mint=mint,
            symbol=None,
            entry_sol_per_token=None,
            sol_spent=None,
            usd_spent=None,
            signature=str(sig),
            cu_price=None,
            jito_tip=None,
            data=out.to_dict(),
        )

    return out


async def buy_token_with_sol(
    mint: str,
    sol_in: float,
    *,
    reason: str = "entry",
    slippage_bps: int | None = None,
    owner: str | None = None,
    confirm: bool = True,
    register_for_exits: bool = True,
    entry_price_unit: float | None = None,
    size_usd: float | None = None,
    usd_spent: float | None = None,
) -> TradeFill:
    """
    Buy SPL token `mint` spending `sol_in` SOL via router (if supported).

    This function is *router dependent*; if your router doesn't expose buy_via_lightning,
    it will return router_unavailable_or_failed.

    If register_for_exits=True and execution.exits_wiring.register_entry is available,
    we'll register the position using best-effort info you pass (entry_price_unit, size_usd/usd_spent).
    """
    kp = load_keypair()
    owner = owner or get_pubkey_str(kp)

    sol_in = float(max(0.0, sol_in))
    if sol_in <= 0:
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner,
            side="BUY",
            confirmed=False,
            reason="sol_in<=0",
            meta={},
        )

    lamports_in = int(sol_in * LAMPORTS_PER_SOL)
    if lamports_in <= 0:
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner,
            side="BUY",
            confirmed=False,
            reason="lamports_in<=0",
            meta={},
        )

    idem_key = f"BUY:{owner}:{mint}:{lamports_in}:{reason}"
    if not await _idem_check_and_set(idem_key):
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner,
            side="BUY",
            confirmed=False,
            reason="idempotency_block",
            meta={},
        )

    pre = await snapshot_wallet(owner, mint, commitment=TX_COMMITMENT)

    if IS_DRYRUN:
        log.event("BUY_DRYRUN", mint=mint, sol_in=sol_in, reason=reason)
        return TradeFill(
            ok=True,
            signature=None,
            mint=mint,
            owner=owner,
            side="BUY",
            confirmed=True,
            reason="dryrun",
            meta={"dryrun": True},
        )

    sig = await _router_buy_token_raw(
        mint=mint, amount_in_lamports=lamports_in, slippage_bps=slippage_bps
    )
    if not sig:
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner,
            side="BUY",
            confirmed=False,
            reason="router_unavailable_or_failed",
            meta={},
        )

    confirmed = True
    cmeta: dict[str, Any] = {}
    if confirm:
        confirmed, cmeta = await wait_for_confirmation(sig)

    post = await snapshot_wallet(owner, mint, commitment=TX_COMMITMENT)

    token_delta = int(post.token_raw - pre.token_raw)
    sol_delta = int(post.sol_lamports - pre.sol_lamports)

    out = TradeFill(
        ok=bool(confirmed),
        signature=str(sig),
        mint=mint,
        owner=owner,
        side="BUY",
        amount_in_raw=int(lamports_in),
        amount_out_raw=None,
        sol_lamports_delta=sol_delta,
        token_raw_delta=token_delta,
        confirmed=bool(confirmed),
        reason="ok" if confirmed else "confirm_failed",
        meta={"confirm": cmeta, "pre": asdict(pre), "post": asdict(post)},
    )

    # Best-effort exit registration (only if we have the info to size it)
    # entry_price_unit must match your price_fn units in exit supervisor world.
    if confirmed and register_for_exits and _register_exit_entry is not None:
        try:
            # You *must* give either usd_spent or size_usd (or sol_spent + USD_PER_SOL in exits_wiring).
            # We prefer usd_spent/size_usd. If you don't have them yet, skip (fail-open).
            if usd_spent is not None or size_usd is not None:
                _register_exit_entry(
                    signature=str(sig),
                    mint=mint,
                    qty=None,
                    entry_price=float(entry_price_unit or 0.0),
                    symbol=None,
                    usd_spent=float(usd_spent) if usd_spent is not None else None,
                    size_usd=float(size_usd) if size_usd is not None else None,
                    sol_spent=sol_in,
                    sell_fn=None,  # must be supplied by the caller in real wiring
                    price_fn=None,  # must be supplied by the caller in real wiring
                )
        except Exception:
            # We never fail the buy because exit registration is missing/incomplete.
            pass

    # Structured log
    with contextlib.suppress(Exception):
        await log_trade_event(
            type="buy_fill" if confirmed else "buy_unconfirmed",
            mint=mint,
            symbol=None,
            entry_sol_per_token=entry_price_unit,
            sol_spent=sol_in,
            usd_spent=usd_spent if usd_spent is not None else size_usd,
            signature=str(sig),
            cu_price=None,
            jito_tip=None,
            data=out.to_dict(),
        )

    return out


# ──────────────────────────────────────────────────────────────────────────────
# Backwards-compatible SELL API (kept for scripts/tools)
# ──────────────────────────────────────────────────────────────────────────────


async def sell_on_jupiter(
    mint: str,
    pct_of_wallet: float = 1.0,
    slippage_bps: int | None = None,
) -> str | None:
    """
    Back-compat wrapper.

    Sell `pct_of_wallet` (0..1) of wallet SPL balance for `mint` via router sell.
    Returns tx signature on success else None (including DRYRUN).
    """
    pct = max(0.0, min(1.0, float(pct_of_wallet)))
    if pct <= 0:
        log.event("SELL_SKIP_ZERO_PCT", mint=mint)
        return None

    kp = load_keypair()
    owner = get_pubkey_str(kp)

    bal = await get_token_balance_raw(owner, mint)
    if bal <= 0:
        log.event("SELL_SKIP_NO_BAL", mint=mint)
        return None

    amount_raw = max(1, int(round(bal * pct)))
    fill = await sell_token_raw(
        mint, amount_raw, reason="manual_pct", slippage_bps=slippage_bps, owner=owner, confirm=True
    )

    if IS_DRYRUN:
        return None
    return fill.signature if fill.ok and isinstance(fill.signature, str) else None


# ──────────────────────────────────────────────────────────────────────────────
# Optional exit engine alias (compat only; canonical exit brain is elsewhere)
# ──────────────────────────────────────────────────────────────────────────────


def get_exit_engine() -> Any:
    """
    Compatibility helper for legacy code that expects trader.exit_engine.
    Prefer importing the canonical exit engine directly instead.
    """
    # Prefer canonical brain if present
    try:
        from utils_pkg import exit_engine as ee  # type: ignore

        return ee
    except Exception:
        pass

    # Fall back to worker driver if present
    try:
        from utils_pkg.auto_exit_worker import exit_engine as worker_ee  # type: ignore

        return worker_ee
    except Exception:
        return None


# Keep legacy name (read-only getter style to avoid importing side effects early)
exit_engine = get_exit_engine()

__all__ = [
    # constants/config
    "IS_DRYRUN",
    "LAMPORTS_PER_SOL",
    "TX_COMMITMENT",
    "TX_RPC_URL",
    "buy_token_with_sol",
    # compat
    "exit_engine",
    "get_signature_status",
    "get_sol_balance_lamports",
    # balances
    "get_token_balance_raw",
    "get_wallet_token_balance_raw",
    # rpc
    "rpc_call",
    # back-compat
    "sell_on_jupiter",
    # execution primitives
    "sell_token_raw",
    "snapshot_wallet",
    "wait_for_confirmation",
]
