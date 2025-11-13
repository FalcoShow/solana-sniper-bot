# utils_pkg/exit_engine.py
from __future__ import annotations

import os
import time
import logging
from typing import Dict, Any, Optional

log = logging.getLogger("sniper.exit_engine")

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
# Global config from env
# ──────────────────────────────────────────────────────────────────────────────

DRYRUN = (
    _env_bool("DRYRUN", False)
    or _env_bool("DRY_RUN", False)
    or _env_bool("IS_DRYRUN", False)
)

# Exit engine can be feature-gated from env
EXIT_ENGINE_FLAG = _env_bool("EXIT_ENGINE", True)
EXIT_ENGINE_ENABLED = _env_bool("EXIT_ENGINE_ENABLED", True)
EXIT_ENGINE_ACTIVE = EXIT_ENGINE_FLAG and EXIT_ENGINE_ENABLED

MIN_SELL_SOL = _env_float("PF_MIN_SELL_SOL", 0.001)
LAMPORTS_PER_SOL = 1_000_000_000
DEFAULT_SLIPPAGE_BPS = _env_int("PF_SLIPPAGE_BPS", 200)

# ──────────────────────────────────────────────────────────────────────────────
# Optional: AI-driven / advanced sell engine hook
# We don't assume anything about the internals beyond a `schedule_after_buy(ev)` API.
# ──────────────────────────────────────────────────────────────────────────────

try:
    # modern path: central AI sell logic module
    from utils_pkg import ai_sell_logic as SELL
except Exception:  # pragma: no cover
    SELL = None  # type: ignore
    log.debug("ai_sell_logic not available; will use direct Jup fallback only")


# ──────────────────────────────────────────────────────────────────────────────
# Fallback direct Jupiter V6 seller
# Used when the main sell engine is unavailable or fails.
# ──────────────────────────────────────────────────────────────────────────────

async def _direct_sell_via_jup(
    mint: str,
    amount_raw: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
) -> Optional[str]:
    """
    Minimal direct sell path using the same Lightning / Jupiter V6 plumbing as buys.
    Returns:
        tx signature string on success, or None on failure.
    """
    try:
        from utils_pkg.lightning_impl import sell_via_lightning

        sig = await sell_via_lightning(
            mint=mint,
            token_program=None,
            amount_raw=amount_raw,
            slippage_bps=slippage_bps,
        )
        return sig
    except Exception as e:
        log.warning(
            "DIRECT_SELL_FAIL",
            extra={"mint": mint, "err": str(e)[:200]},
        )
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Core hook: called when a buy has filled
# ──────────────────────────────────────────────────────────────────────────────

async def on_buy_filled(ev: Dict[str, Any]) -> None:
    """
    Standard entry hook. Called when a BUY is confirmed/filled.

    Expected event payload (as produced by sniper core):

        ev = {
            "mint": str,
            "signature": str,      # entry tx sig
            "symbol": str,         # human-readable token symbol (optional)
            "spend_sol": float,    # SOL spent on entry (may be 0 if unknown)
            "pool": str,           # "pump" / "raydium" / etc.
            "ts": float,           # timestamp (seconds since epoch)
            ...
        }

    Responsibilities:
    - Validate & normalize the event.
    - If the advanced sell engine is present (ai_sell_logic, auto_sell module),
      schedule a position into that engine via `SELL.schedule_after_buy(ev)`.
    - If advanced engine is unavailable, place a small direct Jup sell "probe"
      (10% of position, bounded by PF_MIN_SELL_SOL) just to verify sellability.
    """
    mint = ev.get("mint")
    sym = ev.get("symbol") or (mint[:4] if isinstance(mint, str) and len(mint) >= 4 else str(mint))
    pool = ev.get("pool") or "unknown"

    # Try to infer spend_sol if not explicitly provided
    spend_sol_raw = ev.get("spend_sol", 0.0)
    try:
        spend_sol = float(spend_sol_raw)
    except Exception:
        spend_sol = 0.0

    if not mint:
        log.warning(
            "EXIT_HOOK_BAD_EVENT",
            extra={"reason": "missing_mint", "pool": pool, "ev_min": {k: ev.get(k) for k in ("signature", "spend_sol")}},
        )
        return

    if spend_sol <= 0:
        # We allow spend_sol to be missing, but log it for debugging.
        log.info(
            "EXIT_HOOK_NO_SPEND_SOL",
            extra={"mint": mint, "pool": pool, "symbol": sym},
        )

    # If the entire exit engine is disabled, just log and return.
    if not EXIT_ENGINE_ACTIVE:
        log.info(
            "EXIT_ENGINE_DISABLED",
            extra={
                "mint": mint,
                "symbol": sym,
                "pool": pool,
                "EXIT_ENGINE": EXIT_ENGINE_FLAG,
                "EXIT_ENGINE_ENABLED": EXIT_ENGINE_ENABLED,
            },
        )
        return

    log.info(
        "EXIT_HOOK_START",
        extra={
            "mint": mint,
            "symbol": sym,
            "spend_sol": spend_sol,
            "pool": pool,
            "dryrun": DRYRUN,
        },
    )

    # ── Preferred path: delegate to advanced sell engine ─────────────────────
    # We only assume that SELL.schedule_after_buy(ev: dict) exists and returns
    # an awaitable; the actual TP/SL/DCA/AI logic runs elsewhere.
    if SELL is not None:
        schedule_fn = getattr(SELL, "schedule_after_buy", None)
        if callable(schedule_fn):
            try:
                await schedule_fn(ev)
                log.info(
                    "EXIT_SCHED_OK",
                    extra={
                        "mint": mint,
                        "symbol": sym,
                        "engine": "ai_sell_logic.schedule_after_buy",
                    },
                )
                return
            except Exception as e:
                log.warning(
                    "EXIT_SCHED_FAIL_AI",
                    extra={"mint": mint, "symbol": sym, "err": str(e)[:200]},
                )
        else:
            log.debug(
                "EXIT_NO_SCHEDULE_FN",
                extra={"detail": "ai_sell_logic.schedule_after_buy not found"},
            )

    # ── Fallback: small direct Jup sell probe ────────────────────────────────
    # This is a safety net: we try to sell a small portion just to ensure the
    # token is actually sellable. In DRYRUN mode, we only log.
    probe_sol = max(MIN_SELL_SOL, spend_sol * 0.10) if spend_sol > 0 else MIN_SELL_SOL
    probe_raw = int(probe_sol * LAMPORTS_PER_SOL)

    if DRYRUN:
        log.info(
            "EXIT_FALLBACK_DRYRUN",
            extra={
                "mint": mint,
                "symbol": sym,
                "probe_sol": probe_sol,
                "pool": pool,
            },
        )
        return

    sig = await _direct_sell_via_jup(
        mint=mint,
        amount_raw=probe_raw,
        slippage_bps=DEFAULT_SLIPPAGE_BPS,
    )

    log.info(
        "EXIT_FALLBACK_RESULT",
        extra={
            "mint": mint,
            "symbol": sym,
            "probe_sol": probe_sol,
            "pool": pool,
            "sig": sig,
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# Legacy signature compatibility
# Some older call sites may only have (mint, tx_sig) with no extra metadata.
# ──────────────────────────────────────────────────────────────────────────────

async def on_buy_filled_legacy(raw_id_or_mint: str, tx_sig: str) -> None:
    """
    Backwards-compatible wrapper for call sites that only know (mint, tx_sig).

    We synthesize a minimal event dict and pass it through `on_buy_filled`.
    The spend_sol is set to MIN_SELL_SOL just so the fallback probe has
    a reasonable baseline if used.
    """
    ev: Dict[str, Any] = {
        "mint": raw_id_or_mint,
        "signature": tx_sig,
        "symbol": "",
        "spend_sol": MIN_SELL_SOL,
        "pool": "legacy",
        "ts": time.time(),
    }
    await on_buy_filled(ev)
