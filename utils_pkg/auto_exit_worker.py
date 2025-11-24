from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional

from .core_utils import redis
from utils_pkg.lightning_impl import sell_via_lightning

log = logging.getLogger("sniper.exit_worker")

# Type alias for readability
ExitRequest = Dict[str, Any]

# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────


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


# ──────────────────────────────────────────────────────────────────────────────
# Global config
# ──────────────────────────────────────────────────────────────────────────────

# Legacy aliases – any of these turn dryrun on
DRYRUN: bool = (
    _env_bool("DRYRUN", False)
    or _env_bool("DRY_RUN", False)
    or _env_bool("IS_DRYRUN", False)
)

# Master flag for the overall exit engine (logic side)
EXIT_ENGINE_ENABLED: bool = _env_bool("EXIT_ENGINE_ENABLED", True)

# Background worker that actually sends sells
EXIT_WORKER_ENABLED: bool = _env_bool("EXIT_WORKER_ENABLED", True)
EXIT_WORKER_POLL_SEC: float = _env_float("EXIT_WORKER_POLL_SEC", 3.0)
EXIT_WORKER_BATCH: int = _env_int("EXIT_WORKER_BATCH", 5)

# Slippage settings
PF_SLIPPAGE_BPS: int = _env_int("PF_SLIPPAGE_BPS", 200)
EXIT_WORKER_SLIPPAGE_BPS: int = _env_int(
    "EXIT_WORKER_SLIPPAGE_BPS", PF_SLIPPAGE_BPS
)

# How much of the position to dump, 1.0 = 100%
EXIT_WORKER_SELL_PCT: float = float(os.getenv("EXIT_WORKER_SELL_PCT", "1.0"))

# Whether requests coming from on_buy_filled() should be executed immediately.
# Safe default is False – other modules (risk manager) can enqueue their own
# "sell now" requests or transform these placeholders.
EXIT_ON_BUY_FILLED: bool = _env_bool("EXIT_ON_BUY_FILLED", False)

# TTL for exit_request keys in Redis (seconds)
EXIT_REQUEST_TTL_SEC: int = _env_int("EXIT_REQUEST_TTL_SEC", 600)

# Optional dust guard – if >0, we skip sells that are smaller than this.
EXIT_WORKER_MIN_RAW: int = _env_int("EXIT_WORKER_MIN_RAW", 0)

LAMPORTS_PER_SOL = 1_000_000_000  # only used for logging / sanity


# ──────────────────────────────────────────────────────────────────────────────
# Redis helpers
# ──────────────────────────────────────────────────────────────────────────────


async def _scan_exit_requests(limit: int) -> List[str]:
    """
    SCAN Redis for exit_request:* keys, up to `limit`.
    Returns a list of key names as str.
    """
    if redis is None:
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

        # ensure mint present
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
        # non-fatal
        log.debug("EXIT_WORKER_DELETE_ERROR key=%s", key, exc_info=True)


async def _store_exit_request(payload: ExitRequest) -> Optional[str]:
    """
    Internal helper to persist an exit_request to Redis, returning the key.

    Key format: exit_request:{mint}
    We stick to per-mint keys for idempotency: multiple modules that try
    to enqueue for the same mint will just clobber the previous request.
    """
    if redis is None:
        log.debug("EXIT_ENGINE_NO_REDIS_FOR_STORE")
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


# ──────────────────────────────────────────────────────────────────────────────
# Amount helpers
# ──────────────────────────────────────────────────────────────────────────────


def _derive_raw_from_ui(req: ExitRequest) -> Optional[int]:
    """
    Try to derive raw token amount from ui amount + decimals.

    Supports a bunch of common key shapes used across the codebase.
    """
    # any ui-style size
    ui_keys = (
        "position_ui",
        "amount_ui",
        "size_ui",
        "size",
        "qty_ui",
        "qty",
    )
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

    We also handle a bunch of alternative shapes and derive from ui+decimals
    when possible.

    Returns:
        amount_raw: int or None if we can't determine.
    """
    # 1) direct raw fields
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

    # 2) if no direct raw, try deriving from ui+dec
    if amt is None or amt <= 0:
        amt = _derive_raw_from_ui(req)

    if amt is None or amt <= 0:
        return None

    # Apply sell_pct if present
    sell_pct = float(req.get("sell_pct", EXIT_WORKER_SELL_PCT))
    sell_pct = max(0.01, min(1.0, sell_pct))
    amt_final = int(int(amt) * sell_pct)

    # Optional dust guard
    if EXIT_WORKER_MIN_RAW > 0 and amt_final < EXIT_WORKER_MIN_RAW:
        return None

    return amt_final if amt_final > 0 else None


# ──────────────────────────────────────────────────────────────────────────────
# Exit engine – logic side (front-end API)
# ──────────────────────────────────────────────────────────────────────────────


class _ExitEngine:
    """
    Exit engine (front-end):

    - on_trade_tick: placeholder for future follow-through logic.
    - on_buy_filled: tolerant API used by Strategy to enqueue exit requests.
    - enqueue_exit_request: generic way for risk manager / manual controls
      to schedule exits.

    It is deliberately conservative: if we can't determine mint or size,
    we just log and skip instead of throwing.
    """

    def __init__(self) -> None:
        self.enabled = EXIT_ENGINE_ENABLED

    # ------------------------------------------------------------------ #
    # Hooks used by scanners / strategy
    # ------------------------------------------------------------------ #

    async def on_trade_tick(self, raw_event: Dict[str, Any]) -> None:
        """
        Called on every trade tick (for future momentum/follow-through usage).
        Currently a no-op, but wired so we can extend later without changing
        callers.
        """
        if not self.enabled:
            return
        # Could accumulate per-mint stats here later.
        return

    async def on_buy_filled(
        self,
        ev: Dict[str, Any],
        tx_sig: Optional[str] = None,
        *,
        amount_raw: Optional[int] = None,
    ) -> None:
        """
        Called when a BUY is filled.

        Tolerant signature:

            await exit_engine.on_buy_filled(ev)                         # legacy
            await exit_engine.on_buy_filled(ev, tx_sig)                 # legacy
            await exit_engine.on_buy_filled(ev, tx_sig, amount_raw=..)  # preferred

        We pull what we can from `ev`, construct an exit_request payload,
        and store it in Redis. The background worker will perform the
        actual sell if/when the request should be honored.

        If we truly cannot infer size, we log EXIT_ENGINE_SKIP_NO_SIZE and
        return. Upstream code (main/strategy) *must* pass in a size
        (`amount_raw` or ui+decimals) for exits to be actionable.
        """
        if not self.enabled:
            return
        if redis is None:
            log.debug("EXIT_ENGINE_NO_REDIS")
            return
        if not isinstance(ev, dict):
            return

        # --- Mint --------------------------------------------------------
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
        mint = str(mint)

        # --- Signature (tolerant) ---------------------------------------
        if tx_sig is None:
            tx_sig = (
                ev.get("signature")
                or ev.get("tx_sig")
                or ev.get("sig")
                or ev.get("tx")
            )
            if tx_sig:
                tx_sig = str(tx_sig)

        # --- Position size ----------------------------------------------
        # Preferred: explicit amount_raw argument from caller (main.py).
        if amount_raw is None:
            # Raw-ish fields from event
            for k in (
                "position_raw",
                "amount_raw",
                "out_amount",
                "raw_amount_out",
                "raw_out",
                "size_raw",
            ):
                v = ev.get(k)
                if isinstance(v, (int, float)) and v > 0:
                    amount_raw = int(v)
                    break

        # Derive from ui+dec if still missing
        if amount_raw is None or amount_raw <= 0:
            derived = _derive_raw_from_ui(ev)
            if derived is not None and derived > 0:
                amount_raw = derived

        if amount_raw is None or amount_raw <= 0:
            # This is the behavior you saw in logs: without size information
            # we cannot schedule a meaningful exit.
            log.info(
                "EXIT_ENGINE_SKIP_NO_SIZE",
                extra={
                    "mint": mint,
                    "keys": list(ev.keys()),
                    "spend_sol": ev.get("spend_sol"),
                    "pool": ev.get("pool"),
                },
            )
            return

        # --- Spend SOL (diagnostic only) --------------------------------
        spend_sol = ev.get("spend_sol") or ev.get("entry_spend_sol") or 0.0
        try:
            spend_sol = float(spend_sol)
        except Exception:
            spend_sol = 0.0

        # --- General metadata -------------------------------------------
        reason = str(ev.get("reason", "on_buy_filled"))
        sell_pct = float(ev.get("sell_pct", EXIT_WORKER_SELL_PCT))
        sell_pct = max(0.01, min(1.0, sell_pct))

        follow_stats = ev.get("follow_stats")
        if not isinstance(follow_stats, dict):
            follow_stats = {}

        payload: ExitRequest = {
            "mint": mint,
            "position_raw": int(amount_raw),
            "entry_spend_sol": float(spend_sol),
            "sell_pct": float(sell_pct),
            "reason": reason,
            "follow_stats": follow_stats,
            "source": "on_buy_filled",
        }
        if tx_sig:
            payload["tx_sig"] = tx_sig

        # Option A: enqueue to worker (default / recommended)
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

        # Option B: immediate execution if explicitly enabled
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

        Intended for the risk manager, stop-loss logic, or manual
        "panic sell" triggers.

        Returns the Redis key used, or None on failure.
        """
        if not self.enabled:
            return None
        if redis is None:
            log.debug("EXIT_ENGINE_NO_REDIS")
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


# Global instance used by scripts/main.py and others
exit_engine = _ExitEngine()


# ──────────────────────────────────────────────────────────────────────────────
# Core exit execution (worker)
# ──────────────────────────────────────────────────────────────────────────────


async def _execute_exit(req: ExitRequest) -> None:
    """
    Perform a forced exit for a single mint based on an exit_request payload.
    """
    mint = str(req.get("mint"))
    reason = str(req.get("reason", "unknown"))
    source = str(req.get("source", "unknown"))
    follow = req.get("follow_stats", {}) or {}

    # If this request is just the placeholder from on_buy_filled() and the
    # operator has not enabled EXIT_ON_BUY_FILLED, we still treat it as a
    # real request, but only if a size is present. If size is missing,
    # the earlier stage (on_buy_filled) should have skipped it entirely.
    amount_raw = _extract_amount_raw_from_req(req)
    if amount_raw is None or amount_raw <= 0:
        log.info(
            "EXIT_WORKER_SKIP_NO_AMOUNT",
            extra={"mint": mint, "reason": reason, "source": source},
        )
        return

    if DRYRUN:
        log.info(
            "EXIT_WORKER_DRYRUN",
            extra={
                "mint": mint,
                "amount_raw": int(amount_raw),
                "reason": reason,
                "source": source,
                "follow": follow,
            },
        )
        return

    log.info(
        "EXIT_WORKER_SELL_START",
        extra={
            "mint": mint,
            "amount_raw": int(amount_raw),
            "slippage_bps": EXIT_WORKER_SLIPPAGE_BPS,
            "reason": reason,
            "source": source,
        },
    )

    try:
        sig = await sell_via_lightning(
            mint=mint,
            token_program=None,
            amount_raw=int(amount_raw),
            slippage_bps=int(EXIT_WORKER_SLIPPAGE_BPS),
        )
    except Exception as e:  # noqa: BLE001
        log.warning(
            "EXIT_WORKER_SELL_ERROR",
            extra={"mint": mint, "err": str(e)[:200], "reason": reason},
        )
        return

    log.info(
        "EXIT_WORKER_SELL_DONE",
        extra={
            "mint": mint,
            "amount_raw": int(amount_raw),
            "sig": sig,
            "reason": reason,
            "source": source,
        },
    )


async def _process_once() -> None:
    """
    Single tick: scan for exit requests and process up to EXIT_WORKER_BATCH.
    """
    if not EXIT_WORKER_ENABLED:
        return
    if redis is None:
        log.debug("EXIT_WORKER_NO_REDIS")
        return

    keys = await _scan_exit_requests(EXIT_WORKER_BATCH)
    if not keys:
        return

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

        await _execute_exit(req)

        # Remove the request regardless of success; if the sell failed,
        # follow-up logic (like a re-queue) should recreate a new request.
        await _delete_exit_request(key)


# ──────────────────────────────────────────────────────────────────────────────
# Public loop entrypoint
# ──────────────────────────────────────────────────────────────────────────────


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

    while not stop_evt.is_set():
        try:
            await _process_once()
        except Exception:  # noqa: BLE001
            log.exception("EXIT_WORKER_TICK_ERROR")

        # wait with timeout so we can exit quickly when stop_evt is set
        try:
            await asyncio.wait_for(
                stop_evt.wait(), timeout=EXIT_WORKER_POLL_SEC
            )
        except asyncio.TimeoutError:
            # normal: timeout means go to next tick
            continue
