from __future__ import annotations

"""
utils_pkg/trader.py — Canonical trade execution primitives (P0)
==============================================================

Execution glue between:
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
        - keeps sell_on_jupiter(mint, pct_of_wallet, slippage_bps) -> Optional[str]
        - keeps token balance helpers used by scripts/tools
"""

import asyncio
import contextlib
import importlib
import inspect
import os
import random
import time
from dataclasses import asdict, dataclass, field
from types import ModuleType
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, cast

import requests

from utils_pkg.solana_wallet import get_pubkey_str, load_keypair

LAMPORTS_PER_SOL = 1_000_000_000

# ──────────────────────────────────────────────────────────────────────────────
# Logging (canonical contract if available; safe fallback otherwise)
# ──────────────────────────────────────────────────────────────────────────────

if TYPE_CHECKING:
    from utils_pkg.log_compat import EventLogger, get_logger
else:  # pragma: no cover
    try:
        from utils_pkg.log_compat import EventLogger, get_logger  # type: ignore
    except Exception:  # pragma: no cover
        EventLogger = Any  # type: ignore[misc,assignment]

        def get_logger(name: str | None = None, **_kwargs: Any) -> Any:  # type: ignore[no-redef]
            import logging

            lg = logging.getLogger(name or "trader")
            if not lg.handlers:
                logging.basicConfig(level=logging.INFO)
            return lg


log: "EventLogger" = get_logger(__name__)


def _log_event(name: str, **fields: Any) -> None:
    fn = getattr(log, "event", None)
    if callable(fn):
        with contextlib.suppress(Exception):
            fn(name, **fields)  # type: ignore[misc]
        return
    with contextlib.suppress(Exception):
        info = getattr(log, "info", None)
        if callable(info):
            info("%s %s", name, fields)


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


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


IS_DRYRUN: bool = (
    _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)
)

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

CONFIRM_ENABLED = _env_bool("TRADER_CONFIRM_ENABLED", True)
CONFIRM_TIMEOUT_SEC = _env_float("TRADER_CONFIRM_TIMEOUT_SEC", 45.0)
CONFIRM_POLL_SEC = _env_float("TRADER_CONFIRM_POLL_SEC", 1.25)
CONFIRM_COMMITMENT = _env_str("TRADER_CONFIRM_COMMITMENT", TX_COMMITMENT)

BALANCE_RETRIES = max(0, _env_int("TRADER_BALANCE_RETRIES", 2))
BALANCE_RETRY_SLEEP = _env_float("TRADER_BALANCE_RETRY_SLEEP_SEC", 0.35)

IDEMPOTENCY_TTL_SEC = _env_float("TRADER_IDEMPOTENCY_TTL_SEC", 45.0)

_UA = {"user-agent": "pnlsniper/trader", "accept": "application/json"}


# ──────────────────────────────────────────────────────────────────────────────
# Optional budget hooks (fail-open) — typed adapter (not a Module)
# ──────────────────────────────────────────────────────────────────────────────


class BudgetLike(Protocol):
    def before_rpc(self, method: str, *, weight: int | None = None) -> bool: ...
    def after_rpc(self, method: str, *, weight: int | None = None) -> None: ...


class _BudgetNoop:
    def before_rpc(self, method: str, *, weight: int | None = None) -> bool:
        return True

    def after_rpc(self, method: str, *, weight: int | None = None) -> None:
        return None


budget: BudgetLike = _BudgetNoop()

with contextlib.suppress(Exception):
    _bm = importlib.import_module("utils_pkg.budget")
    _inst = getattr(_bm, "budget", None)
    if (
        _inst is not None
        and callable(getattr(_inst, "before_rpc", None))
        and callable(getattr(_inst, "after_rpc", None))
    ):
        budget = cast(BudgetLike, _inst)
    else:
        _before = getattr(_bm, "before_rpc", None)
        _after = getattr(_bm, "after_rpc", None)
        if callable(_before) and callable(_after):

            class _BudgetWrapper:
                def before_rpc(self, method: str, *, weight: int | None = None) -> bool:
                    return bool(_before(method, weight=weight))  # type: ignore[misc]

                def after_rpc(self, method: str, *, weight: int | None = None) -> None:
                    _after(method, weight=weight)  # type: ignore[misc]

            budget = _BudgetWrapper()


# ──────────────────────────────────────────────────────────────────────────────
# Optional: structured DB logger (fail-open)
# ──────────────────────────────────────────────────────────────────────────────

try:
    from db.logger import log_trade_event as _log_trade_event  # type: ignore
except Exception:  # pragma: no cover

    async def _log_trade_event(*_a: Any, **_k: Any) -> None:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Optional: execution exit supervisor wiring (fail-open)
# IMPORTANT: explicitly type Optional callable to avoid full-project mypy failure.
# ──────────────────────────────────────────────────────────────────────────────

SellFn = Callable[[str, float, str, float], Awaitable[tuple[bool, str | None, dict[str, Any]]]]
PriceFn = Callable[[str], Awaitable[float | None]]


class RegisterEntryFn(Protocol):
    def __call__(
        self,
        signature: str,
        mint: str,
        qty: int | None,
        entry_price: float,
        *,
        symbol: str | None = None,
        usd_spent: float | None = None,
        size_usd: float | None = None,
        sol_spent: float | None = None,
        sell_fn: SellFn | None = None,
        price_fn: PriceFn | None = None,
        entry_ts: float | None = None,
        tags: dict[str, Any] | None = None,
    ) -> None: ...


_register_exit_entry: RegisterEntryFn | None = None
try:
    from execution.exits_wiring import register_entry as _register_exit_entry  # type: ignore
except Exception:  # pragma: no cover
    _register_exit_entry = None


# ──────────────────────────────────────────────────────────────────────────────
# Router: canonical swap sender(s). Best-effort import.
# IMPORTANT: router must be Optional for mypy to not mark guards as unreachable.
# ──────────────────────────────────────────────────────────────────────────────

router: ModuleType | None
try:
    import utils_pkg.router as _router  # type: ignore
except Exception:  # pragma: no cover
    router = None
else:
    router = _router


# ──────────────────────────────────────────────────────────────────────────────
# Types
# ──────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RpcResult:
    ok: bool
    value: dict[str, Any]
    err: str | None = None


@dataclass(frozen=True)
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
    meta: dict[str, Any] = field(default_factory=dict)

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
    if IDEMPOTENCY_TTL_SEC <= 0:
        return True

    now = time.time()
    async with _idem_lock:
        for k, exp_ts in list(_idem.items()):
            if exp_ts <= now:
                _idem.pop(k, None)

        exp_opt: float | None = _idem.get(key)
        if exp_opt is not None and exp_opt > now:
            return False

        _idem[key] = now + float(IDEMPOTENCY_TTL_SEC)
        return True


# ──────────────────────────────────────────────────────────────────────────────
# RPC client (requests in worker thread)
# ──────────────────────────────────────────────────────────────────────────────


def _rpc_post_blocking(method: str, params: list[Any]) -> dict[str, Any]:
    if not TX_RPC_URL:
        raise RuntimeError("RPC URL not configured (TX_RPC_URL / SOL_RPC_URL / SOLANA_RPC_URL)")

    if not budget.before_rpc(method):
        return {"jsonrpc": "2.0", "id": 1, "result": None}

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        r = requests.post(
            TX_RPC_URL,
            json=payload,
            timeout=float(_RPC_TIMEOUT_SEC),
            headers=_UA,
        )
        r.raise_for_status()
        js = r.json()
        return js if isinstance(js, dict) else {"jsonrpc": "2.0", "id": 1, "result": js}
    finally:
        with contextlib.suppress(Exception):
            budget.after_rpc(method)


async def rpc_call(method: str, params: list[Any]) -> RpcResult:
    last_err: str | None = None
    for attempt in range(_RPC_RETRIES + 1):
        try:
            js = await asyncio.to_thread(_rpc_post_blocking, method, params)
            if isinstance(js, dict) and js.get("error"):
                last_err = str(js["error"])
                raise RuntimeError(last_err)
            return RpcResult(ok=True, value=js or {}, err=None)
        except Exception as e:
            last_err = repr(e)
            if attempt >= _RPC_RETRIES:
                break
            sleep = _RPC_RETRY_BASE_SLEEP * (2**attempt) + random.random() * _RPC_RETRY_JITTER
            await asyncio.sleep(float(sleep))
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
        try:
            amt = it["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
            total += int(amt)
            continue
        except Exception:
            pass

        try:
            tok = it["account"]["data"]["parsed"]["info"]["tokenAmount"]
            ui = float(tok.get("uiAmount") or 0.0)
            dec = int(tok.get("decimals") or 0)
            total += int(round(ui * (10**dec)))
        except Exception:
            continue

    return int(total)


async def get_token_balance_raw(owner: str, mint: str, *, commitment: str | None = None) -> int:
    c = commitment or TX_COMMITMENT
    for attempt in range(BALANCE_RETRIES + 1):
        try:
            res = await rpc_call(
                "getTokenAccountsByOwner",
                [owner, {"mint": mint}, {"encoding": "jsonParsed", "commitment": c}],
            )
            if not res.ok:
                raise RuntimeError(res.err or "rpc_fail")
            return int(_sum_token_balance_raw_from_resp(res.value))
        except Exception as e:
            if attempt >= BALANCE_RETRIES:
                _log_event("RPC_BALANCE_ERR", mint=mint, err=str(e)[:200])
                return 0
            await asyncio.sleep(float(BALANCE_RETRY_SLEEP))
    return 0


async def get_sol_balance_lamports(owner: str, *, commitment: str | None = None) -> int:
    c = commitment or TX_COMMITMENT
    for attempt in range(BALANCE_RETRIES + 1):
        try:
            res = await rpc_call("getBalance", [owner, {"commitment": c}])
            if not res.ok:
                raise RuntimeError(res.err or "rpc_fail")
            return int(((res.value or {}).get("result") or {}).get("value") or 0)
        except Exception as e:
            if attempt >= BALANCE_RETRIES:
                _log_event("RPC_SOL_BAL_ERR", err=str(e)[:200])
                return 0
            await asyncio.sleep(float(BALANCE_RETRY_SLEEP))
    return 0


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
    c = commitment or CONFIRM_COMMITMENT
    res = await rpc_call("getSignatureStatuses", [[signature], {"searchTransactionHistory": True}])
    if not res.ok:
        return {"ok": False, "err": res.err, "commitment": c}
    value = ((res.value or {}).get("result") or {}).get("value") or []
    st = value[0] if isinstance(value, list) and value else None
    return {"ok": True, "status": st, "commitment": c}


async def wait_for_confirmation(
    signature: str, *, timeout_sec: float = CONFIRM_TIMEOUT_SEC
) -> tuple[bool, dict[str, Any]]:
    if not CONFIRM_ENABLED:
        return True, {"skipped": True}

    deadline = time.time() + max(1.0, float(timeout_sec))
    last: dict[str, Any] = {}
    while time.time() < deadline:
        st = await get_signature_status(signature)
        last = st or {}
        if st.get("ok") and st.get("status"):
            s = st["status"]
            if isinstance(s, dict) and s.get("err") is not None:
                return False, {"confirmed": True, "err": s.get("err"), "status": s}
            if isinstance(s, dict) and (
                s.get("confirmationStatus") in ("confirmed", "finalized")
                or s.get("confirmations") is None
            ):
                return True, {"confirmed": True, "status": s}
        await asyncio.sleep(float(CONFIRM_POLL_SEC))
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
    if router is None:
        return None

    fn = getattr(router, "sell_via_lightning", None)
    if not callable(fn):
        return None

    try:
        if slippage_bps is not None:
            res = fn(
                mint=mint,
                token_program=token_program,
                amount_raw=int(amount_raw),
                slippage_bps=int(slippage_bps),
            )
        else:
            res = fn(mint=mint, token_program=token_program, amount_raw=int(amount_raw))
        out = await _maybe_await(res)
        return str(out) if isinstance(out, str) and out else None
    except TypeError:
        res2 = fn(mint, token_program, int(amount_raw))
        out2 = await _maybe_await(res2)
        return str(out2) if isinstance(out2, str) and out2 else None
    except Exception:
        return None


async def _router_buy_token_raw(
    *,
    mint: str,
    amount_in_lamports: int,
    slippage_bps: int | None = None,
    token_program: Any = None,
) -> str | None:
    if router is None:
        return None

    fn = getattr(router, "buy_via_lightning", None)
    if not callable(fn):
        return None

    try:
        if slippage_bps is not None:
            res = fn(
                mint=mint,
                token_program=token_program,
                amount_in_lamports=int(amount_in_lamports),
                slippage_bps=int(slippage_bps),
            )
        else:
            res = fn(
                mint=mint, token_program=token_program, amount_in_lamports=int(amount_in_lamports)
            )
        out = await _maybe_await(res)
        return str(out) if isinstance(out, str) and out else None
    except TypeError:
        res2 = fn(mint, token_program, int(amount_in_lamports))
        out2 = await _maybe_await(res2)
        return str(out2) if isinstance(out2, str) and out2 else None
    except Exception:
        return None


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
    kp = load_keypair()
    owner2 = owner or get_pubkey_str(kp)

    amt = int(max(0, amount_raw))
    if amt <= 0:
        return TradeFill(
            ok=False, signature=None, mint=mint, owner=owner2, side="SELL", reason="amount_raw<=0"
        )

    idem_key = f"SELL:{owner2}:{mint}:{amt}:{reason}"
    if not await _idem_check_and_set(idem_key):
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner2,
            side="SELL",
            reason="idempotency_block",
        )

    pre = await snapshot_wallet(owner2, mint, commitment=TX_COMMITMENT)

    if IS_DRYRUN:
        _log_event("SELL_DRYRUN", mint=mint, amount_raw=amt, reason=reason)
        return TradeFill(
            ok=True,
            signature=None,
            mint=mint,
            owner=owner2,
            side="SELL",
            confirmed=True,
            reason="dryrun",
            meta={"dryrun": True},
        )

    sig = await _router_sell_token_raw(mint=mint, amount_raw=amt, slippage_bps=slippage_bps)
    if not sig:
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner2,
            side="SELL",
            reason="router_unavailable_or_failed",
        )

    confirmed_ok = True
    cmeta: dict[str, Any] = {}
    if confirm:
        confirmed_ok, cmeta = await wait_for_confirmation(sig)

    post = await snapshot_wallet(owner2, mint, commitment=TX_COMMITMENT)
    token_delta = int(post.token_raw - pre.token_raw)
    sol_delta = int(post.sol_lamports - pre.sol_lamports)

    out = TradeFill(
        ok=bool(confirmed_ok),
        signature=sig,
        mint=mint,
        owner=owner2,
        side="SELL",
        amount_in_raw=amt,
        sol_lamports_delta=sol_delta,
        token_raw_delta=token_delta,
        confirmed=bool(confirmed_ok),
        reason="ok" if confirmed_ok else "confirm_failed",
        meta={"confirm": cmeta, "pre": asdict(pre), "post": asdict(post)},
    )

    with contextlib.suppress(Exception):
        await _log_trade_event(
            type="sell_fill" if confirmed_ok else "sell_unconfirmed",
            mint=mint,
            symbol=None,
            entry_sol_per_token=None,
            sol_spent=None,
            usd_spent=None,
            signature=sig,
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
    kp = load_keypair()
    owner2 = owner or get_pubkey_str(kp)

    sol_amt = float(max(0.0, sol_in))
    if sol_amt <= 0:
        return TradeFill(
            ok=False, signature=None, mint=mint, owner=owner2, side="BUY", reason="sol_in<=0"
        )

    lamports_in = int(sol_amt * LAMPORTS_PER_SOL)
    if lamports_in <= 0:
        return TradeFill(
            ok=False, signature=None, mint=mint, owner=owner2, side="BUY", reason="lamports_in<=0"
        )

    idem_key = f"BUY:{owner2}:{mint}:{lamports_in}:{reason}"
    if not await _idem_check_and_set(idem_key):
        return TradeFill(
            ok=False,
            signature=None,
            mint=mint,
            owner=owner2,
            side="BUY",
            reason="idempotency_block",
        )

    pre = await snapshot_wallet(owner2, mint, commitment=TX_COMMITMENT)

    if IS_DRYRUN:
        _log_event("BUY_DRYRUN", mint=mint, sol_in=sol_amt, reason=reason)
        return TradeFill(
            ok=True,
            signature=None,
            mint=mint,
            owner=owner2,
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
            owner=owner2,
            side="BUY",
            reason="router_unavailable_or_failed",
        )

    confirmed_ok = True
    cmeta: dict[str, Any] = {}
    if confirm:
        confirmed_ok, cmeta = await wait_for_confirmation(sig)

    post = await snapshot_wallet(owner2, mint, commitment=TX_COMMITMENT)
    token_delta = int(post.token_raw - pre.token_raw)
    sol_delta = int(post.sol_lamports - pre.sol_lamports)

    out = TradeFill(
        ok=bool(confirmed_ok),
        signature=sig,
        mint=mint,
        owner=owner2,
        side="BUY",
        amount_in_raw=lamports_in,
        sol_lamports_delta=sol_delta,
        token_raw_delta=token_delta,
        confirmed=bool(confirmed_ok),
        reason="ok" if confirmed_ok else "confirm_failed",
        meta={"confirm": cmeta, "pre": asdict(pre), "post": asdict(post)},
    )

    if confirmed_ok and register_for_exits and _register_exit_entry is not None:
        with contextlib.suppress(Exception):
            if usd_spent is not None or size_usd is not None:
                _register_exit_entry(
                    signature=sig,
                    mint=mint,
                    qty=None,
                    entry_price=float(entry_price_unit or 0.0),
                    symbol=None,
                    usd_spent=float(usd_spent) if usd_spent is not None else None,
                    size_usd=float(size_usd) if size_usd is not None else None,
                    sol_spent=sol_amt,
                    sell_fn=None,
                    price_fn=None,
                )

    with contextlib.suppress(Exception):
        await _log_trade_event(
            type="buy_fill" if confirmed_ok else "buy_unconfirmed",
            mint=mint,
            symbol=None,
            entry_sol_per_token=entry_price_unit,
            sol_spent=sol_amt,
            usd_spent=usd_spent if usd_spent is not None else size_usd,
            signature=sig,
            cu_price=None,
            jito_tip=None,
            data=out.to_dict(),
        )

    return out


async def sell_on_jupiter(
    mint: str, pct_of_wallet: float = 1.0, slippage_bps: int | None = None
) -> str | None:
    pct = max(0.0, min(1.0, float(pct_of_wallet)))
    if pct <= 0:
        _log_event("SELL_SKIP_ZERO_PCT", mint=mint)
        return None

    kp = load_keypair()
    owner = get_pubkey_str(kp)

    bal = await get_token_balance_raw(owner, mint)
    if bal <= 0:
        _log_event("SELL_SKIP_NO_BAL", mint=mint)
        return None

    amount_raw = max(1, int(round(bal * pct)))
    fill = await sell_token_raw(
        mint, amount_raw, reason="manual_pct", slippage_bps=slippage_bps, owner=owner, confirm=True
    )

    if IS_DRYRUN:
        return None
    return fill.signature if fill.ok and isinstance(fill.signature, str) else None


# ──────────────────────────────────────────────────────────────────────────────
# Optional exit engine alias (compat only; lazy to avoid import side effects)
# ──────────────────────────────────────────────────────────────────────────────


class _LazyValue:
    def __init__(self, loader: Any):
        self._loader = loader
        self._loaded = False
        self._value: Any = None

    def _load(self) -> Any:
        if not self._loaded:
            self._loaded = True
            with contextlib.suppress(Exception):
                self._value = self._loader()
        return self._value

    def __getattr__(self, name: str) -> Any:
        v = self._load()
        return getattr(v, name)

    def __repr__(self) -> str:
        v = self._load()
        return repr(v)

    def __bool__(self) -> bool:
        v = self._load()
        return bool(v)


def get_exit_engine() -> Any:
    ee: Any = None
    try:
        from utils_pkg import exit_engine as ee  # type: ignore
    except Exception:
        ee = None

    if ee is not None:
        return ee

    worker_ee: Any = None
    try:
        from utils_pkg.auto_exit_worker import exit_engine as worker_ee  # type: ignore
    except Exception:
        worker_ee = None

    return worker_ee


exit_engine: Any = _LazyValue(get_exit_engine)

__all__ = [
    "IS_DRYRUN",
    "LAMPORTS_PER_SOL",
    "TX_COMMITMENT",
    "TX_RPC_URL",
    "buy_token_with_sol",
    "exit_engine",
    "get_signature_status",
    "get_sol_balance_lamports",
    "get_token_balance_raw",
    "get_wallet_token_balance_raw",
    "rpc_call",
    "sell_on_jupiter",
    "sell_token_raw",
    "snapshot_wallet",
    "wait_for_confirmation",
]
