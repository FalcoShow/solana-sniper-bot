from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional

from .core_utils import redis
from utils_pkg.lightning_impl import sell_via_lightning

log = logging.getLogger("sniper.exit_worker")

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

DRYRUN = (
    _env_bool("DRYRUN", False)
    or _env_bool("DRY_RUN", False)
    or _env_bool("IS_DRYRUN", False)
)

EXIT_ENGINE_ENABLED = _env_bool("EXIT_ENGINE_ENABLED", True)

EXIT_WORKER_ENABLED = _env_bool("EXIT_WORKER_ENABLED", True)
EXIT_WORKER_POLL_SEC = _env_float("EXIT_WORKER_POLL_SEC", 3.0)
EXIT_WORKER_BATCH = _env_int("EXIT_WORKER_BATCH", 5)

PF_SLIPPAGE_BPS = _env_int("PF_SLIPPAGE_BPS", 200)
EXIT_WORKER_SLIPPAGE_BPS = _env_int("EXIT_WORKER_SLIPPAGE_BPS", PF_SLIPPAGE_BPS)

# how much of the position to dump, 1.0 = 100%
EXIT_WORKER_SELL_PCT = float(os.getenv("EXIT_WORKER_SELL_PCT", "1.0"))

# TTL for exit_request keys in Redis (seconds)
EXIT_REQUEST_TTL_SEC = _env_int("EXIT_REQUEST_TTL_SEC", 600)

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


async def _load_exit_request(key: str) -> Optional[Dict[str, Any]]:
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
        if not data.get("mint"):
            return None
        return data
    except Exception as e:  # noqa: BLE001
        log.debug("EXIT_WORKER_LOAD_ERROR key=%s err=%s", key, e, exc_info=True)
        return None


async def _delete_exit_request(key: str) -> None:
    if redis is None:
        return
    try:
        await redis.delete(key)
    except Exception:  # noqa: BLE001
        # non-fatal
        log.debug("EXIT_WORKER_DELETE_ERROR key=%s", key, exc_info=True)


# ──────────────────────────────────────────────────────────────────────────────
# Position helpers
# NOTE: we do NOT try to be too smart here; we assume lightning_impl
#       can sell an explicit raw amount. For now we use the raw amount
#       provided in the exit_request payload if present.
#       If it's missing, we bail and leave the key for a later pass.
# ──────────────────────────────────────────────────────────────────────────────


def _extract_amount_raw(req: Dict[str, Any]) -> Optional[int]:
    """
    Determine how many raw tokens to sell from an exit_request payload.

    Expected shapes (all optional, we just try our best):

        {
          "mint": "...",
          "position_raw": 123456,       # preferred
          "entry_spend_sol": 0.15,      # informational
          "sell_pct": 1.0,              # optional override
          ...
        }

    Returns:
        amount_raw: int or None if we can't determine.
    """
    amt_raw = req.get("position_raw")
    if isinstance(amt_raw, (int, float)) and amt_raw > 0:
        sell_pct = float(req.get("sell_pct", EXIT_WORKER_SELL_PCT))
        sell_pct = max(0.01, min(1.0, sell_pct))
        return int(int(amt_raw) * sell_pct)

    # Fallback: if no explicit position_raw is stored, we give up for now.
    # This keeps the worker safe if earlier code didn't persist size info.
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Exit engine
# ──────────────────────────────────────────────────────────────────────────────


class _ExitEngine:
    """
    Exit engine:

    - on_trade_tick: placeholder for future follow-through logic.
    - on_buy_filled: tolerant API used by Strategy to enqueue exit requests.

    It is deliberately conservative: if we can't determine mint or size,
    we just log and skip instead of throwing.
    """

    def __init__(self) -> None:
        self.enabled = EXIT_ENGINE_ENABLED

    async def on_trade_tick(self, raw_event: Dict[str, Any]) -> None:
        """
        Called on every trade tick (for future momentum/follow-through usage).
        At the moment this is a no-op, but we keep the hook so the rest
        of the system doesn't break.
        """
        if not self.enabled:
            return
        # NOTE: you can add per-mint tracking / stats here later if you want.
        return

    async def on_buy_filled(
        self,
        ev: Dict[str, Any],
        tx_sig: Optional[str] = None,
    ) -> None:
        """
        Called when a BUY is filled.

        Tolerant signature:

            await exit_engine.on_buy_filled(ev)          # old style
            await exit_engine.on_buy_filled(ev, tx_sig)  # new style

        We pull what we can from `ev`, enqueue an exit_request in Redis,
        and let the background worker actually perform the sell.
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
            log.info("EXIT_ENGINE_SKIP_NO_MINT", extra={"ev_keys": list(ev.keys())})
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
        amount_raw: Optional[int] = None
        for k in ("position_raw", "amount_raw", "out_amount", "raw_amount_out"):
            v = ev.get(k)
            if isinstance(v, (int, float)) and v > 0:
                amount_raw = int(v)
                break

        if amount_raw is None or amount_raw <= 0:
            log.info(
                "EXIT_ENGINE_SKIP_NO_SIZE",
                extra={"mint": mint, "keys": list(ev.keys())},
            )
            return

        # --- Spend SOL (for diagnostics) --------------------------------
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

        key = f"exit_request:{mint}"

        payload: Dict[str, Any] = {
            "mint": mint,
            "position_raw": int(amount_raw),
            "entry_spend_sol": float(spend_sol),
            "sell_pct": float(sell_pct),
            "reason": reason,
            "follow_stats": follow_stats,
        }
        if tx_sig:
            payload["tx_sig"] = tx_sig

        try:
            await redis.set(key, json.dumps(payload), ex=EXIT_REQUEST_TTL_SEC)
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
        except Exception as e:  # noqa: BLE001
            log.warning(
                "EXIT_ENGINE_REDIS_ERROR",
                extra={"mint": mint, "err": str(e)[:200]},
            )


# Global instance used by scripts/main.py
exit_engine = _ExitEngine()


# ──────────────────────────────────────────────────────────────────────────────
# Core exit execution (worker)
# ──────────────────────────────────────────────────────────────────────────────


async def _execute_exit(req: Dict[str, Any]) -> None:
    """
    Perform a forced exit for a single mint based on an exit_request payload.
    """
    mint = str(req.get("mint"))
    reason = str(req.get("reason", "unknown"))
    follow = req.get("follow_stats", {}) or {}

    amount_raw = _extract_amount_raw(req)
    if amount_raw is None or amount_raw <= 0:
        log.info(
            "EXIT_WORKER_SKIP_NO_AMOUNT",
            extra={"mint": mint, "reason": reason},
        )
        return

    if DRYRUN:
        log.info(
            "EXIT_WORKER_DRYRUN",
            extra={
                "mint": mint,
                "amount_raw": int(amount_raw),
                "reason": reason,
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
            extra={"mint": mint, "err": str(e)[:200]},
        )
        return

    log.info(
        "EXIT_WORKER_SELL_DONE",
        extra={
            "mint": mint,
            "amount_raw": int(amount_raw),
            "sig": sig,
            "reason": reason,
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

        log.info(
            "EXIT_WORKER_REQUEST",
            extra={"key": key, "mint": mint, "reason": reason},
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
            await asyncio.wait_for(stop_evt.wait(), timeout=EXIT_WORKER_POLL_SEC)
        except asyncio.TimeoutError:
            # normal: timeout means go to next tick
            continue
