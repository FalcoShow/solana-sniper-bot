from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

# Import core_utils as a module so we can flexibly access attributes
from . import core_utils

# Pull commonly-used symbols from core_utils
redis = getattr(core_utils, "redis", None)
create_task = getattr(core_utils, "create_task", asyncio.create_task)

# Optional: best-effort sell result logger (if defined in core_utils)
try:  # pragma: no cover - optional dependency
    record_sell_result = getattr(core_utils, "record_sell_result")  # type: ignore[assignment]
except Exception:  # noqa: BLE001
    record_sell_result = None  # type: ignore[assignment]

from utils_pkg.lightning_impl import sell_via_lightning

# -----------------------------------------------------------------------------
# Logger (with safety shims for .event / .every)
# -----------------------------------------------------------------------------
log = logging.getLogger("sniper.exit_worker")


if not hasattr(log, "event"):

    def _event(name: str, **kw: Any) -> None:  # pragma: no cover - shim
        log.info("%s %s", name, kw)

    setattr(log, "event", _event)

if not hasattr(log, "every"):

    _every_cache: Dict[str, int] = {}

    def _every(
        key: str,
        n: int = 60,
        msg: str | None = None,
        **kw: Any,
    ) -> None:  # pragma: no cover - shim
        c = _every_cache.get(key, 0) + 1
        _every_cache[key] = c
        if c % max(1, n) == 0:
            if msg:
                log.info("%s %s", msg, kw)
            else:
                log.info("%s %s", key, kw)

    setattr(log, "every", _every)

# Type alias
ExitRequest = Dict[str, Any]


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return str(v).lower() in ("1", "true", "t", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


# -----------------------------------------------------------------------------
# Global config
# -----------------------------------------------------------------------------
DRYRUN: bool = (
    _env_bool("DRYRUN", False)
    or _env_bool("DRY_RUN", False)
    or _env_bool("IS_DRYRUN", False)
)

EXIT_ENGINE_ENABLED: bool = _env_bool("EXIT_ENGINE_ENABLED", True)

# Background worker that actually sends sells
EXIT_WORKER_ENABLED: bool = _env_bool("EXIT_WORKER_ENABLED", True)
EXIT_WORKER_POLL_SEC: float = _env_float("EXIT_WORKER_POLL_SEC", 3.0)
EXIT_WORKER_BATCH: int = _env_int("EXIT_WORKER_BATCH", 5)

# Heartbeat / observability
EXIT_WORKER_HEARTBEAT_SEC: float = _env_float("EXIT_WORKER_HEARTBEAT_SEC", 30.0)
EXIT_WORKER_IDLE_LOG_EVERY_TICKS: int = _env_int(
    "EXIT_WORKER_IDLE_LOG_EVERY_TICKS",
    10,
)

# Slippage configuration
PF_SLIPPAGE_BPS: int = _env_int("PF_SLIPPAGE_BPS", 200)
EXIT_WORKER_SLIPPAGE_BPS: int = _env_int(
    "EXIT_WORKER_SLIPPAGE_BPS",
    PF_SLIPPAGE_BPS,
)

# Default exit config (used when a new BUY fills)
EXIT_DEFAULT_SL_PCT: float = _env_float("EXIT_STOP_LOSS_PCT", 8.0)
EXIT_DEFAULT_TSL_ARM_PCT: float = _env_float("PF_TSL_ARM_PCT", 10.0)
EXIT_DEFAULT_TSL_PCT: float = _env_float("PF_TSL_PCT", 4.5)
EXIT_DEFAULT_STAGNANT_MAX_MIN: int = _env_int("EXIT_STAGNANT_MAX_MIN", 7)
EXIT_DEFAULT_STAGNANT_MIN_MOVE_PCT: float = _env_float(
    "EXIT_STAGNANT_MIN_MOVE_PCT",
    6.0,
)

# How much of the position to dump; 1.0 = 100%
try:
    EXIT_WORKER_SELL_PCT: float = float(os.getenv("EXIT_WORKER_SELL_PCT", "1.0"))
except Exception:
    EXIT_WORKER_SELL_PCT = 1.0
EXIT_WORKER_SELL_PCT = max(0.01, min(1.0, EXIT_WORKER_SELL_PCT))

# Whether on_buy_filled() should try to execute immediately
EXIT_ON_BUY_FILLED: bool = _env_bool("EXIT_ON_BUY_FILLED", False)

# TTL for exit_request:* keys in Redis (seconds)
EXIT_REQUEST_TTL_SEC: int = _env_int("EXIT_REQUEST_TTL_SEC", 600)

# Optional dust guard – if >0, skip exits smaller than this raw amount
EXIT_WORKER_MIN_RAW: int = _env_int("EXIT_WORKER_MIN_RAW", 0)

# Only used for sanity logs when comparing to SOL
LAMPORTS_PER_SOL: int = 1_000_000_000

# -----------------------------------------------------------------------------
# Worker stats (for heartbeat + diagnostics)
# -----------------------------------------------------------------------------
_worker_stats: Dict[str, Any] = {
    "ticks": 0,
    "jobs_seen": 0,
    "jobs_processed": 0,
    "sells_ok": 0,
    "sells_err": 0,
    "skipped_no_amount": 0,
    "last_job_ts": None,
}


# -----------------------------------------------------------------------------
# Helper: get AlertManager instance (Telegram / Notion)
# -----------------------------------------------------------------------------
def _get_am():
    """
    Best-effort accessor for the AlertManager / alert manager singleton.

    We try several common names on core_utils:
      - _am()               (old pattern)
      - am() / am
      - get_am() / get_alert_manager()
      - alert_manager / ALERT_MANAGER

    If nothing works, returns None and callers should simply skip alerts.
    """
    candidates = [
        getattr(core_utils, "_am", None),
        getattr(core_utils, "am", None),
        getattr(core_utils, "get_am", None),
        getattr(core_utils, "get_alert_manager", None),
        getattr(core_utils, "alert_manager", None),
        getattr(core_utils, "ALERT_MANAGER", None),
    ]

    for cand in candidates:
        if cand is None:
            continue

        try:
            # If it's callable (factory), call it; otherwise assume instance
            am = cand() if callable(cand) else cand
        except TypeError:
            # Some callables might not need args; if that fails, treat as instance
            am = cand
        except Exception:
            continue

        if am is not None:
            return am

    return None


# -----------------------------------------------------------------------------
# Size helpers
# -----------------------------------------------------------------------------
def _job_spend_sol(job: Dict[str, Any]) -> float:
    """
    Normalize exit job size into SOL and write back job['spend_sol'].

    This mirrors the _normalize_spend_sol logic used on the entry side so that
    alerts / analytics / exit decisions all see a consistent spend_sol.
    """
    cand = job.get("spend_sol", None)

    # SOL-sized fields first
    if cand in (None, "", 0, 0.0):
        for k in ("amount_sol", "size_sol", "size"):
            v = job.get(k)
            if v not in (None, ""):
                cand = v
                break

    # Lamports -> SOL
    if cand in (None, "", 0, 0.0):
        lamports_val = None
        for k in ("lamports_in", "in_amount_raw", "amount_raw", "base_size_raw"):
            v = job.get(k)
            if v not in (None, ""):
                lamports_val = v
                break

        if lamports_val is not None:
            try:
                lamports_f = float(lamports_val)
                cand = lamports_f / float(LAMPORTS_PER_SOL)
            except Exception:
                cand = 0.0

    try:
        size_sol = float(cand) if cand not in (None, "") else 0.0
    except Exception:
        size_sol = 0.0

    if size_sol < 0:
        size_sol = 0.0

    job["spend_sol"] = size_sol
    return size_sol


# -----------------------------------------------------------------------------
# Redis helpers (exit_request storage)
# -----------------------------------------------------------------------------
async def _scan_exit_requests(limit: int) -> List[str]:
    """
    Scan Redis for exit_request:* keys, up to `limit`.
    Returns a list of key names.
    """
    if redis is None:
        log.event("EXIT_WORKER_NO_REDIS", where="scan")
        return []

    keys: List[str] = []
    cursor: Any = 0

    try:
        while True:
            cursor, batch = await redis.scan(
                cursor=cursor,
                match="exit_request:*",
                count=limit,
            )
            for k in batch:
                if isinstance(k, (bytes, bytearray)):
                    k = k.decode("utf-8", "ignore")
                keys.append(str(k))
                if len(keys) >= limit:
                    return keys
            if cursor == 0:
                break
    except Exception as e:  # noqa: BLE001
        log.debug("EXIT_WORKER_SCAN_ERROR %s", e, exc_info=True)

    return keys


async def _load_exit_request(key: str) -> Optional[ExitRequest]:
    """
    Fetch and JSON-decode an exit_request payload.
    Returns None if missing or invalid.
    """
    if redis is None:
        return None

    try:
        raw = await redis.get(key)
        if raw is None:
            return None

        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", "ignore")

        data = json.loads(raw)
        if not isinstance(data, dict):
            return None

        mint = str(data.get("mint", "")).strip()
        if not mint:
            return None
        data["mint"] = mint

        return data
    except Exception as e:  # noqa: BLE001
        log.debug("EXIT_WORKER_LOAD_ERROR key=%s err=%s", key, e, exc_info=True)
        return None


async def _delete_exit_request(key: str) -> None:
    """
    Best-effort deletion of an exit_request:* key.
    """
    if redis is None:
        return

    try:
        await redis.delete(key)
    except Exception:  # noqa: BLE001
        log.debug("EXIT_WORKER_DELETE_ERROR key=%s", key, exc_info=True)


async def _store_exit_request(payload: ExitRequest) -> Optional[str]:
    """
    Persist an exit_request to Redis, returning the key.

    Key format: exit_request:{mint}

    We deliberately key by mint so that multiple modules that race to enqueue
    exits for the same mint simply overwrite each other (idempotent semantics).
    """
    if redis is None:
        log.event("EXIT_ENGINE_NO_REDIS_FOR_STORE")
        return None

    mint = str(payload.get("mint", "")).strip()
    if not mint:
        return None

    key = f"exit_request:{mint}"

    try:
        await redis.set(key, json.dumps(payload), ex=EXIT_REQUEST_TTL_SEC)
        return key
    except Exception as e:  # noqa: BLE001
        log.warning(
            "EXIT_ENGINE_REDIS_ERROR",
            extra={"mint": mint, "err": str(e)[:200]},
        )
        return None


# -----------------------------------------------------------------------------
# Amount helpers (token raw, not SOL)
# -----------------------------------------------------------------------------
def _derive_raw_from_ui(req: ExitRequest) -> Optional[int]:
    """
    Try to derive a raw token amount from ui amount + decimals.

    Recognised keys:

      UI amount keys:
        position_ui / amount_ui / size_ui / size / qty_ui / qty

      Decimal keys:
        dec / decimals / token_decimals
    """
    ui_keys = ("position_ui", "amount_ui", "size_ui", "size", "qty_ui", "qty")
    dec_keys = ("dec", "decimals", "token_decimals")

    ui: Optional[float] = None
    for k in ui_keys:
        v = req.get(k)
        if isinstance(v, (int, float)) and v > 0:
            ui = float(v)
            break
        if isinstance(v, str):
            try:
                fv = float(v)
            except Exception:
                continue
            if fv > 0:
                ui = fv
                break

    if ui is None or ui <= 0:
        return None

    dec: Optional[int] = None
    for k in dec_keys:
        v = req.get(k)
        if isinstance(v, int) and v >= 0:
            dec = v
            break
        if isinstance(v, str):
            try:
                dv = int(v)
            except Exception:
                continue
            if dv >= 0:
                dec = dv
                break

    if dec is None:
        return None

    try:
        scale = 10 ** int(dec)
        raw = int(ui * scale)
        if raw > 0:
            return raw
    except Exception:
        return None

    return None


def _extract_amount_raw_from_req(req: ExitRequest) -> Optional[int]:
    """
    Determine how many raw tokens to sell from an exit_request payload.

    Preferred field:
        "position_raw": int

    Fallbacks:
        amount_raw / size_raw / raw_amount_out / raw_out / token_amount_raw
        or derived from ui+decimals.

    Applies EXIT_WORKER_SELL_PCT and EXIT_WORKER_MIN_RAW guards.
    """
    raw_keys = (
        "position_raw",
        "amount_raw",
        "size_raw",
        "raw_amount_out",
        "raw_out",
        "token_amount_raw",
    )

    amt: Optional[int] = None
    for k in raw_keys:
        v = req.get(k)
        if isinstance(v, int) and v > 0:
            amt = v
            break
        if isinstance(v, float) and v > 0:
            amt = int(v)
            break

    if amt is None or amt <= 0:
        amt = _derive_raw_from_ui(req)

    if amt is None or amt <= 0:
        return None

    try:
        sell_pct = float(req.get("sell_pct", EXIT_WORKER_SELL_PCT))
    except Exception:
        sell_pct = EXIT_WORKER_SELL_PCT
    sell_pct = max(0.01, min(1.0, sell_pct))

    amt_final = int(int(amt) * sell_pct)

    if EXIT_WORKER_MIN_RAW > 0 and amt_final < EXIT_WORKER_MIN_RAW:
        return None

    return amt_final if amt_final > 0 else None


def _build_default_exit_config(ev: ExitRequest | Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a default exit config for a new BUY.

    Uses env defaults but lets the event override them via:
      sl_pct, tsl_arm_pct, tsl_pct, stagnant_max_min, stagnant_min_move_pct
    """
    cfg: Dict[str, Any] = {}

    def _float(ev_key: str, default: float) -> float:
        v = ev.get(ev_key)
        try:
            return float(v)
        except Exception:
            return float(default)

    def _int(ev_key: str, default: int) -> int:
        v = ev.get(ev_key)
        try:
            return int(v)
        except Exception:
            return int(default)

    cfg["sl_pct"] = _float("sl_pct", EXIT_DEFAULT_SL_PCT)
    cfg["tsl_arm_pct"] = _float("tsl_arm_pct", EXIT_DEFAULT_TSL_ARM_PCT)
    cfg["tsl_pct"] = _float("tsl_pct", EXIT_DEFAULT_TSL_PCT)
    cfg["stagnant_max_min"] = _int(
        "stagnant_max_min",
        EXIT_DEFAULT_STAGNANT_MAX_MIN,
    )
    cfg["stagnant_min_move_pct"] = _float(
        "stagnant_min_move_pct",
        EXIT_DEFAULT_STAGNANT_MIN_MOVE_PCT,
    )
    return cfg


# -----------------------------------------------------------------------------
# Exit engine – logic front-end
# -----------------------------------------------------------------------------
class _ExitEngine:
    """
    Exit engine (front-end):

    - on_trade_tick: placeholder for follow-through / stats (currently no-op).
    - on_buy_filled: tolerant API used by Strategy to enqueue exits.
    - enqueue_exit_request: generic API for risk manager / manual exits.

    Stores exit_request payloads in Redis. The background worker
    (exit_worker_loop) performs the actual sells by calling sell_via_lightning.
    """

    def __init__(self) -> None:
        self.enabled = EXIT_ENGINE_ENABLED

    # ------------------------------------------------------------------ #
    # Hooks called by Strategy / scanners
    # ------------------------------------------------------------------ #
    async def on_trade_tick(self, raw_event: Dict[str, Any]) -> None:
        """
        Called on every trade tick (for future use).
        Currently a no-op so we don't disturb callers.
        """
        if not self.enabled:
            return
        # hook point for future follow-through logic
        return

    async def on_buy_filled(
        self,
        ev: Dict[str, Any],
        tx_sig: Optional[str] = None,
        *,
        amount_raw: Optional[int] = None,
    ) -> None:
        """
        Called by Strategy AFTER a BUY is confirmed on-chain.

        We construct an exit_request payload and put it into Redis.
        A separate worker loop later executes the sell.
        """
        if not self.enabled:
            return
        if redis is None:
            log.event("EXIT_ENGINE_NO_REDIS", where="on_buy_filled")
            return
        if not isinstance(ev, dict):
            return

        # Mint
        mint = (
            ev.get("mint")
            or ev.get("token")
            or ev.get("mintAddress")
            or ev.get("mint_address")
            or ev.get("id")
        )
        if not mint:
            log.info(
                "EXIT_ENGINE_SKIP_NO_MINT",
                extra={"ev_keys": list(ev.keys())},
            )
            return
        mint = str(mint).strip()

        # Signature
        if tx_sig is None:
            tx_sig = (
                ev.get("signature")
                or ev.get("tx_sig")
                or ev.get("sig")
                or ev.get("tx")
            )
        if tx_sig:
            tx_sig = str(tx_sig)

        # Spend SOL (diagnostic only)
        spend_sol = ev.get("spend_sol") or ev.get("entry_spend_sol") or 0.0
        try:
            spend_sol = float(spend_sol)
        except Exception:
            spend_sol = 0.0

        # Try to infer raw size
        if amount_raw is None:
            for k in (
                "position_raw",
                "amount_raw",
                "out_amount",
                "out_amount_raw",
                "amount_out",
                "raw_amount_out",
                "raw_out",
                "size_raw",
                "base_amount_raw",
                "base_size_raw",
                "token_amount_raw",
            ):
                v = ev.get(k)
                if isinstance(v, (int, float)) and v > 0:
                    amount_raw = int(v)
                    break

        if amount_raw is None or amount_raw <= 0:
            probe = dict(ev)
            maybe = _extract_amount_raw_from_req(probe)
            if maybe is not None and maybe > 0:
                amount_raw = maybe

        if amount_raw is None or amount_raw <= 0:
            derived = _derive_raw_from_ui(ev)
            if derived is not None and derived > 0:
                amount_raw = derived

        if amount_raw is None or amount_raw <= 0:
            price_keys = (
                "price_entry_ui",
                "entry_price_ui",
                "fill_price_ui",
                "price_ui",
                "price",
            )
            price = None
            for pk in price_keys:
                v = ev.get(pk)
                if v is None:
                    continue
                try:
                    f = float(v)
                    if f > 0:
                        price = f
                        break
                except Exception:
                    continue

            dec = None
            for dk in ("dec", "decimals", "token_decimals"):
                v = ev.get(dk)
                if v is None:
                    continue
                try:
                    di = int(v)
                    if di >= 0:
                        dec = di
                        break
                except Exception:
                    continue

            if spend_sol > 0 and price and dec is not None:
                try:
                    ui_qty = float(spend_sol) / float(price)
                    rough_raw = int(ui_qty * (10 ** int(dec)))
                    if rough_raw > 0:
                        amount_raw = rough_raw
                        log.info(
                            "EXIT_ENGINE_FALLBACK_FROM_SPEND",
                            extra={
                                "mint": mint,
                                "spend_sol": spend_sol,
                                "price": price,
                                "dec": dec,
                                "amount_raw": rough_raw,
                            },
                        )
                except Exception:
                    pass

        if amount_raw is None or amount_raw <= 0:
            log.info(
                "EXIT_ENGINE_FALLBACK_FULL_SIZE_FAILED",
                extra={
                    "mint": mint,
                    "keys": list(ev.keys()),
                    "spend_sol": spend_sol,
                    "pool": ev.get("pool"),
                },
            )
            return

        reason = str(ev.get("reason", "on_buy_filled"))
        try:
            sell_pct = float(ev.get("sell_pct", EXIT_WORKER_SELL_PCT))
        except Exception:
            sell_pct = EXIT_WORKER_SELL_PCT
        sell_pct = max(0.01, min(1.0, sell_pct))

        follow_stats = ev.get("follow_stats")
        if not isinstance(follow_stats, dict):
            follow_stats = {}

        exit_cfg = _build_default_exit_config(ev)

        payload: ExitRequest = {
            "mint": mint,
            "position_raw": int(amount_raw),
            "entry_spend_sol": float(spend_sol),
            "sell_pct": float(sell_pct),
            "reason": reason,
            "follow_stats": follow_stats,
            "source": "on_buy_filled",
            "exit_config": exit_cfg,
            # extra context for alerts
            "symbol": ev.get("symbol") or ev.get("name"),
            "pool": ev.get("pool") or "unknown",
        }
        if tx_sig:
            payload["tx_sig"] = tx_sig

        key = await _store_exit_request(payload)
        if key:
            log.info(
                "EXIT_ENGINE_REQUEST_ENQUEUED",
                extra={
                    "key": key,
                    "mint": mint,
                    "amount_raw": int(amount_raw),
                    "sell_pct": sell_pct,
                    "reason": reason,
                    "tx_sig": tx_sig,
                },
            )

        # Optional: immediate execution (still uses the worker logic)
        if EXIT_ON_BUY_FILLED:
            log.info(
                "EXIT_ENGINE_IMMEDIATE_ON_BUY_FILLED",
                extra={
                    "mint": mint,
                    "amount_raw": int(amount_raw),
                    "reason": reason,
                },
            )
            await _execute_exit(payload)

    # ------------------------------------------------------------------ #
    # Generic API for risk manager / manual exits
    # ------------------------------------------------------------------ #
    async def enqueue_exit_request(
        self,
        *,
        mint: str,
        amount_raw: int,
        reason: str = "manual",
        sell_pct: float | None = None,
        entry_spend_sol: float | None = None,
        follow_stats: Optional[Dict[str, Any]] = None,
        tx_sig: Optional[str] = None,
    ) -> Optional[str]:
        """
        Generic way to schedule an exit for a position.

        Intended for:
          - risk manager / TSL / SL logic
          - "panic sell" or manual controls
        """
        if not self.enabled:
            return None
        if redis is None:
            log.event("EXIT_ENGINE_NO_REDIS", where="enqueue_manual")
            return None

        mint = str(mint).strip()
        if not mint:
            return None

        if amount_raw <= 0:
            log.info(
                "EXIT_ENGINE_ENQUEUE_SKIP_BAD_SIZE",
                extra={"mint": mint, "amount_raw": amount_raw},
            )
            return None

        if sell_pct is None:
            sell_pct = EXIT_WORKER_SELL_PCT
        sell_pct = max(0.01, min(1.0, float(sell_pct)))

        if follow_stats is None:
            follow_stats = {}

        payload: ExitRequest = {
            "mint": mint,
            "position_raw": int(amount_raw),
            "sell_pct": float(sell_pct),
            "reason": str(reason),
            "entry_spend_sol": float(entry_spend_sol or 0.0),
            "follow_stats": follow_stats,
            "source": "manual",
        }
        if tx_sig:
            payload["tx_sig"] = str(tx_sig)

        key = await _store_exit_request(payload)
        if key:
            log.info(
                "EXIT_ENGINE_MANUAL_ENQUEUED",
                extra={
                    "key": key,
                    "mint": mint,
                    "amount_raw": int(amount_raw),
                    "sell_pct": sell_pct,
                    "reason": reason,
                },
            )
        return key


# Global instance used elsewhere (Strategy, risk manager, etc.)
exit_engine = _ExitEngine()


# -----------------------------------------------------------------------------
# Core exit execution (worker)
# -----------------------------------------------------------------------------
async def _execute_exit(req: ExitRequest) -> None:
    """
    Perform a forced exit for a single mint based on an exit_request payload.
    """
    global _worker_stats

    mint = str(req.get("mint"))
    reason = str(req.get("reason", "unknown"))
    source = str(req.get("source", "unknown"))
    follow = req.get("follow_stats", {}) or {}

    spend_sol = _job_spend_sol(req)

    amount_raw = _extract_amount_raw_from_req(req)
    if amount_raw is None or amount_raw <= 0:
        _worker_stats["skipped_no_amount"] += 1
        log.info(
            "EXIT_WORKER_SKIP_NO_AMOUNT",
            extra={
                "mint": mint,
                "reason": reason,
                "source": source,
                "spend_sol": spend_sol,
            },
        )
        if record_sell_result:
            try:
                record_sell_result(
                    ok=False,
                    reason="no_amount",
                    mint=mint,
                    meta={"source": source, "spend_sol": spend_sol},
                )
            except Exception:
                pass
        return

    if DRYRUN:
        _worker_stats["sells_ok"] += 1  # treat as logically successful
        log.info(
            "EXIT_WORKER_DRYRUN",
            extra={
                "mint": mint,
                "amount_raw": int(amount_raw),
                "reason": reason,
                "source": source,
                "follow": follow,
                "spend_sol": spend_sol,
            },
        )
        if record_sell_result:
            try:
                record_sell_result(
                    ok=False,
                    reason="dryrun",
                    mint=mint,
                    meta={"source": source, "amount_raw": int(amount_raw)},
                )
            except Exception:
                pass
        return

    log.info(
        "EXIT_WORKER_SELL_START",
        extra={
            "mint": mint,
            "amount_raw": int(amount_raw),
            "slippage_bps": EXIT_WORKER_SLIPPAGE_BPS,
            "reason": reason,
            "source": source,
            "spend_sol": spend_sol,
        },
    )

    sig = None
    try:
        sig = await sell_via_lightning(
            mint=mint,
            token_program=None,
            amount_raw=int(amount_raw),
            slippage_bps=int(EXIT_WORKER_SLIPPAGE_BPS),
        )
    except Exception as e:  # noqa: BLE001
        _worker_stats["sells_err"] += 1
        log.warning(
            "EXIT_WORKER_SELL_ERROR",
            extra={
                "mint": mint,
                "err": str(e)[:200],
                "reason": reason,
                "source": source,
                "spend_sol": spend_sol,
            },
        )
        if record_sell_result:
            try:
                record_sell_result(
                    ok=False,
                    reason=str(e),
                    mint=mint,
                    meta={"source": source, "amount_raw": int(amount_raw)},
                )
            except Exception:
                pass
        return

    _worker_stats["sells_ok"] += 1
    _worker_stats["last_job_ts"] = time.time()

    log.info(
        "EXIT_WORKER_SELL_DONE",
        extra={
            "mint": mint,
            "amount_raw": int(amount_raw),
            "sig": sig,
            "reason": reason,
            "source": source,
            "spend_sol": spend_sol,
        },
    )

    if record_sell_result:
        try:
            record_sell_result(
                ok=True,
                reason=reason,
                mint=mint,
                sig=sig,
                meta={"source": source, "amount_raw": int(amount_raw)},
            )
        except Exception:
            pass

    # Optional AlertManager notification (Telegram / Notion)
    try:
        am = _get_am()
        if am is not None:
            symbol = req.get("symbol") or req.get("name") or mint
            pool = req.get("pool") or "unknown"

            create_task(
                am.notify_trade_fill(
                    side="SELL",
                    mint=mint,
                    symbol=symbol,
                    pool=pool,
                    sig=str(sig) if sig else None,
                    spend_sol=spend_sol,
                    out_amount=amount_raw,
                )
            )
        else:
            log.event("EXIT_WORKER_NO_ALERT_MANAGER", mint=mint)
    except Exception as e:
        log.warning(
            "EXIT_WORKER_ALERT_FAIL",
            extra={"mint": mint, "err": str(e)[:200]},
        )


async def _process_once() -> int:
    """
    Single tick: scan for exit_request:* keys and process up to EXIT_WORKER_BATCH.

    Returns:
        int: number of exit jobs processed in this tick.
    """
    global _worker_stats

    if not EXIT_WORKER_ENABLED:
        return 0
    if redis is None:
        log.event("EXIT_WORKER_NO_REDIS", where="process_once")
        return 0

    _worker_stats["ticks"] += 1

    keys = await _scan_exit_requests(EXIT_WORKER_BATCH)
    job_count = len(keys)
    _worker_stats["jobs_seen"] += job_count

    if not keys:
        return 0

    for key in keys:
        req = await _load_exit_request(key)
        if not req:
            await _delete_exit_request(key)
            continue

        mint = str(req.get("mint"))
        reason = str(req.get("reason", "unknown"))
        source = str(req.get("source", "unknown"))

        log.info(
            "EXIT_WORKER_REQUEST",
            extra={
                "key": key,
                "mint": mint,
                "reason": reason,
                "source": source,
            },
        )

        _worker_stats["jobs_processed"] += 1
        await _execute_exit(req)

        # Always delete; re-queues should create a new request
        await _delete_exit_request(key)

    return job_count


# -----------------------------------------------------------------------------
# Heartbeat / observability
# -----------------------------------------------------------------------------
def _heartbeat_payload() -> Dict[str, Any]:
    """
    Build a structured snapshot of worker stats for logging.
    """
    now_ts = time.time()
    last_ts = _worker_stats.get("last_job_ts")
    since_last = None
    if isinstance(last_ts, (int, float)) and last_ts > 0:
        since_last = now_ts - last_ts

    return {
        "ticks": _worker_stats.get("ticks"),
        "jobs_seen": _worker_stats.get("jobs_seen"),
        "jobs_processed": _worker_stats.get("jobs_processed"),
        "sells_ok": _worker_stats.get("sells_ok"),
        "sells_err": _worker_stats.get("sells_err"),
        "skipped_no_amount": _worker_stats.get("skipped_no_amount"),
        "seconds_since_last_job": since_last,
        "dryrun": DRYRUN,
        "poll_sec": EXIT_WORKER_POLL_SEC,
        "batch": EXIT_WORKER_BATCH,
        "slippage_bps": EXIT_WORKER_SLIPPAGE_BPS,
        "sell_pct": EXIT_WORKER_SELL_PCT,
    }


def _log_heartbeat() -> None:
    """
    Emit a loud, structured heartbeat so you can SEE that the worker lives.
    """
    payload = _heartbeat_payload()
    log.event("EXIT_WORKER_HEARTBEAT", **payload)


# -----------------------------------------------------------------------------
# Public loop entrypoint
# -----------------------------------------------------------------------------
async def exit_worker_loop(stop_evt: asyncio.Event) -> None:
    """
    Background loop: periodically scans Redis for exit_request:* keys
    and executes forced exits for them.
    """
    if not EXIT_WORKER_ENABLED:
        log.info("EXIT_WORKER_DISABLED", extra={"enabled": False})
        return

    log.info(
        "EXIT_WORKER_START",
        extra={
            "poll_sec": EXIT_WORKER_POLL_SEC,
            "batch": EXIT_WORKER_BATCH,
            "slippage_bps": EXIT_WORKER_SLIPPAGE_BPS,
            "sell_pct": EXIT_WORKER_SELL_PCT,
            "dryrun": DRYRUN,
        },
    )
    log.event(
        "EXIT_WORKER_CONFIG",
        poll_sec=EXIT_WORKER_POLL_SEC,
        batch=EXIT_WORKER_BATCH,
        slippage_bps=EXIT_WORKER_SLIPPAGE_BPS,
        sell_pct=EXIT_WORKER_SELL_PCT,
        dryrun=int(DRYRUN),
        heartbeat_sec=EXIT_WORKER_HEARTBEAT_SEC,
    )

    last_hb = 0.0
    idle_ticks = 0

    while not stop_evt.is_set():
        jobs_processed = 0
        try:
            jobs_processed = await _process_once()
        except Exception:  # noqa: BLE001
            log.exception("EXIT_WORKER_TICK_ERROR")

        if jobs_processed == 0:
            idle_ticks += 1
            if idle_ticks >= max(1, EXIT_WORKER_IDLE_LOG_EVERY_TICKS):
                idle_ticks = 0
                log.event(
                    "EXIT_WORKER_IDLE",
                    ticks=_worker_stats.get("ticks"),
                    jobs_seen=_worker_stats.get("jobs_seen"),
                    jobs_processed=_worker_stats.get("jobs_processed"),
                )
        else:
            idle_ticks = 0

        now = time.time()
        if (now - last_hb) >= max(5.0, EXIT_WORKER_HEARTBEAT_SEC):
            last_hb = now
            _log_heartbeat()

        # Wait with timeout so the loop can exit promptly when stop_evt is set
        try:
            await asyncio.wait_for(stop_evt.wait(), timeout=EXIT_WORKER_POLL_SEC)
        except asyncio.TimeoutError:
            # normal tick – go again
            continue
