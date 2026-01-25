# perps/connector_drift.py
"""
perps.connector_drift
=====================

Drift PERPS connector — **connector-only**, not a strategy engine.

Overhaul contract:
- No hidden side effects on import.
- Deterministic behavior: no random IDs; stable error handling.
- Never raises to caller in normal flow; returns structured results.
- Supports:
    - DISABLED mode (feature-flagged off)
    - DRY_RUN mode (simulated fills + in-memory position book)
    - LIVE mode (optional driftpy integration if installed; otherwise fail-closed)

This connector is meant to be called by a higher-level strategy/runner that already:
- chose symbol/side/size
- enforced risk limits
- computed stop/take profit

Env (minimal, safe defaults):
  PERPS_ENABLED                "true"/"false"   (default false)
  PERPS_DRY_RUN                "true"/"false"   (default true)
  PERPS_PROVIDER               "drift"          (default "drift")
  PERPS_TIMEOUT_SEC            (default 15)
  PERPS_RETRIES                (default 1)
  PERPS_SYMBOL_MAP_JSON        JSON mapping symbol->market index or id (default "{}")
    example: {"SOL-PERP": 0, "BTC-PERP": 1}

Optional (for LIVE; only used if driftpy is available in your env):
  DRIFT_ENV                    "mainnet-beta" / "devnet"  (default mainnet-beta)
  DRIFT_RPC_URL                RPC url string (optional; fallback to your core RPC)
  DRIFT_WALLET_PRIVATE_KEY     base58/bytes or path, depending on your wallet loader
  DRIFT_SUBACCOUNT_ID          integer (default 0)

Notes:
- If driftpy is not installed, LIVE calls will return ok=False with reason="driftpy_missing".
- This keeps your repo deployable on Render without extra deps until you intentionally add them.

Public API:
  - class DriftPerps
      await place_order(...)
      await close_position(...)
      await close_all_positions(...)
      await fetch_position(...)
      await health(...)
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Literal

Side = Literal["buy", "sell", "long", "short"]
OrderType = Literal["market", "limit"]


# -----------------------------------------------------------------------------
# Logging (safe fallback)
# -----------------------------------------------------------------------------
def _get_logger():
    try:
        # your canonical logger (if present)
        from utils_pkg.core_utils import logger  # type: ignore

        return logger
    except Exception:
        import logging

        lg = logging.getLogger("perps.connector_drift")
        if not lg.handlers:
            logging.basicConfig(level=logging.INFO)
        return lg


log = _get_logger()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        return int(v) if v not in (None, "", "None") else int(default)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        return float(v) if v not in (None, "", "None") else float(default)
    except Exception:
        return float(default)


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_side(side: str) -> str:
    s = (side or "").strip().lower()
    if s in ("buy", "long"):
        return "buy"
    if s in ("sell", "short"):
        return "sell"
    raise ValueError(f"invalid side: {side}")


# -----------------------------------------------------------------------------
# Config + Models
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class PerpsConfig:
    enabled: bool
    dry_run: bool
    provider: str
    timeout_sec: float
    retries: int
    symbol_map: dict[str, Any]
    drift_env: str
    drift_rpc_url: str | None
    drift_subaccount_id: int

    @staticmethod
    def from_env() -> PerpsConfig:
        enabled = _env_bool("PERPS_ENABLED", False)
        dry_run = _env_bool("PERPS_DRY_RUN", True)
        provider = (os.getenv("PERPS_PROVIDER", "drift") or "drift").strip().lower()
        timeout_sec = float(_env_float("PERPS_TIMEOUT_SEC", 15.0))
        retries = int(_env_int("PERPS_RETRIES", 1))

        raw_map = os.getenv("PERPS_SYMBOL_MAP_JSON", "{}") or "{}"
        try:
            symbol_map = json.loads(raw_map)
            if not isinstance(symbol_map, dict):
                symbol_map = {}
        except Exception:
            symbol_map = {}

        drift_env = (os.getenv("DRIFT_ENV", "mainnet-beta") or "mainnet-beta").strip()
        drift_rpc_url = (os.getenv("DRIFT_RPC_URL", "") or "").strip() or None
        drift_subaccount_id = int(_env_int("DRIFT_SUBACCOUNT_ID", 0))

        return PerpsConfig(
            enabled=enabled,
            dry_run=dry_run,
            provider=provider,
            timeout_sec=timeout_sec,
            retries=retries,
            symbol_map=symbol_map,
            drift_env=drift_env,
            drift_rpc_url=drift_rpc_url,
            drift_subaccount_id=drift_subaccount_id,
        )


@dataclass
class OrderResult:
    ok: bool
    order_id: str
    reason: str = ""
    raw: dict[str, Any] | None = None


@dataclass
class PositionInfo:
    symbol: str
    side: str  # "long"/"short"
    notional: float
    leverage: float
    entry_price: float | None = None
    stop_price: float | None = None
    take_profit: float | None = None
    updated_ms: int = 0


@dataclass
class HealthInfo:
    ok: bool
    mode: str  # "disabled"/"dry_run"/"live"
    provider: str
    detail: str = ""


# -----------------------------------------------------------------------------
# Deterministic ID generator (no random)
# -----------------------------------------------------------------------------
_id_lock = asyncio.Lock()
_id_counter = 0


async def _next_id(prefix: str) -> str:
    global _id_counter
    async with _id_lock:
        _id_counter += 1
        # deterministic enough for logs: timestamp + monotonic counter
        return f"{prefix}-{_now_ms()}-{_id_counter}"


# -----------------------------------------------------------------------------
# Retry wrapper (never raises; returns (ok, value|None, reason))
# -----------------------------------------------------------------------------
async def _with_retries(coro_fn, *, retries: int, base_delay: float = 0.45):
    delay = base_delay
    for i in range(max(0, retries) + 1):
        try:
            val = await coro_fn()
            return True, val, ""
        except Exception as e:
            if i >= retries:
                return False, None, f"{type(e).__name__}: {e}"
            await asyncio.sleep(delay)
            delay *= 1.7


# -----------------------------------------------------------------------------
# Connector
# -----------------------------------------------------------------------------
class DriftPerps:
    """
    Canonical PERPS connector (Drift).

    NOTE: This is not a strategy. It only executes what it is told.
    """

    def __init__(self, cfg: PerpsConfig | None = None):
        self.cfg = cfg or PerpsConfig.from_env()
        self._dry_positions: dict[str, PositionInfo] = {}
        self._live_ready: bool | None = None

    # ----------------------------- mode helpers -----------------------------

    def mode(self) -> str:
        if not self.cfg.enabled:
            return "disabled"
        if self.cfg.dry_run:
            return "dry_run"
        return "live"

    def enabled(self) -> bool:
        return bool(self.cfg.enabled)

    # ----------------------------- symbol helpers ----------------------------

    def _resolve_market(self, symbol: str) -> Any:
        """
        Resolve symbol -> drift market index/id. Uses PERPS_SYMBOL_MAP_JSON.
        If not found, returns symbol itself (some integrations accept it).
        """
        sym = (symbol or "").strip()
        return self.cfg.symbol_map.get(sym, sym)

    # ----------------------------- dry-run book -----------------------------

    async def _dry_place(
        self,
        symbol: str,
        side: str,
        notional: float,
        leverage: float,
        stop_price: float,
        take_profit: float,
    ) -> OrderResult:
        oid = await _next_id("DRY-PERPS")
        # naive: overwrite position per symbol
        pos_side = "long" if side == "buy" else "short"
        self._dry_positions[symbol] = PositionInfo(
            symbol=symbol,
            side=pos_side,
            notional=float(notional),
            leverage=float(leverage),
            entry_price=None,
            stop_price=float(stop_price) if stop_price else None,
            take_profit=float(take_profit) if take_profit else None,
            updated_ms=_now_ms(),
        )
        return OrderResult(ok=True, order_id=oid, reason="dry_run")

    async def _dry_close(self, symbol: str, reduce_only: bool = True) -> OrderResult:
        oid = await _next_id("DRY-CLOSE")
        if symbol in self._dry_positions:
            del self._dry_positions[symbol]
        return OrderResult(ok=True, order_id=oid, reason="dry_run")

    # ----------------------------- live readiness ---------------------------

    def _check_live_deps(self) -> bool:
        """
        LIVE mode requires driftpy (or your chosen drift client).
        We keep this optional so Render deploys don't break until you opt in.
        """
        if self._live_ready is not None:
            return self._live_ready
        try:
            import driftpy  # noqa: F401  # type: ignore

            self._live_ready = True
        except Exception:
            self._live_ready = False
        return self._live_ready

    # ----------------------------- public API --------------------------------

    async def place_order(
        self,
        symbol: str,
        side: Side,
        notional: float,
        leverage: float,
        stop_price: float,
        take_profit: float,
        *,
        order_type: OrderType = "market",
        reduce_only: bool = False,
        client_tag: str | None = None,
    ) -> tuple[bool, str]:
        """
        Place a perps order. Returns (ok, order_id_or_reason).

        Fail-closed:
        - If disabled -> (False, "perps_disabled")
        - If live but driftpy missing -> (False, "driftpy_missing")
        - If invalid inputs -> (False, "bad_request:...")
        """
        if not self.cfg.enabled:
            return False, "perps_disabled"
        try:
            s_side = _normalize_side(str(side))
        except Exception:
            return False, "bad_request:invalid_side"

        notional = float(notional)
        leverage = float(leverage)
        if notional <= 0 or leverage <= 0:
            return False, "bad_request:notional_or_leverage"

        leverage = _clamp(leverage, 1.0, 50.0)
        stop_price = float(stop_price or 0.0)
        take_profit = float(take_profit or 0.0)

        if self.cfg.dry_run:
            res = await self._dry_place(symbol, s_side, notional, leverage, stop_price, take_profit)
            return res.ok, res.order_id

        # LIVE
        if not self._check_live_deps():
            return False, "driftpy_missing"

        market = self._resolve_market(symbol)

        async def _do_live():
            """
            LIVE integration stub: keep deterministic & safe.
            Replace this block with your driftpy client call once you add the dependency.
            """
            # IMPORTANT: no partial execution here. If not implemented, raise to be caught by retry wrapper.
            raise RuntimeError("live_not_implemented")

        ok, _, reason = await _with_retries(_do_live, retries=self.cfg.retries)
        if not ok:
            log.warning(
                "[perps] place_order failed: symbol=%s side=%s market=%s reason=%s",
                symbol,
                s_side,
                market,
                reason,
            )
            return False, reason
        # If you implement live, return the exchange order id / tx sig here.
        return True, "live_ok"

    async def close_position(self, symbol: str, *, reduce_only: bool = True) -> tuple[bool, str]:
        """
        Close a position for symbol. Returns (ok, close_id_or_reason).
        """
        if not self.cfg.enabled:
            return False, "perps_disabled"

        if self.cfg.dry_run:
            res = await self._dry_close(symbol, reduce_only=reduce_only)
            return res.ok, res.order_id

        if not self._check_live_deps():
            return False, "driftpy_missing"

        market = self._resolve_market(symbol)

        async def _do_live():
            raise RuntimeError("live_not_implemented")

        ok, _, reason = await _with_retries(_do_live, retries=self.cfg.retries)
        if not ok:
            log.warning(
                "[perps] close_position failed: symbol=%s market=%s reason=%s",
                symbol,
                market,
                reason,
            )
            return False, reason
        return True, "live_ok"

    async def close_all_positions(self) -> tuple[bool, str]:
        """
        Best-effort close-all. In dry-run, clears the book.
        """
        if not self.cfg.enabled:
            return False, "perps_disabled"

        if self.cfg.dry_run:
            self._dry_positions.clear()
            oid = await _next_id("DRY-CLOSE-ALL")
            return True, oid

        if not self._check_live_deps():
            return False, "driftpy_missing"

        async def _do_live():
            raise RuntimeError("live_not_implemented")

        ok, _, reason = await _with_retries(_do_live, retries=self.cfg.retries)
        if not ok:
            return False, reason
        return True, "live_ok"

    async def fetch_position(self, symbol: str) -> tuple[bool, PositionInfo | None, str]:
        """
        Fetch current position (connector-level view).
        Returns (ok, PositionInfo|None, reason).
        """
        if not self.cfg.enabled:
            return False, None, "perps_disabled"

        if self.cfg.dry_run:
            return True, self._dry_positions.get(symbol), "dry_run"

        if not self._check_live_deps():
            return False, None, "driftpy_missing"

        market = self._resolve_market(symbol)

        async def _do_live():
            raise RuntimeError("live_not_implemented")

        ok, pos, reason = await _with_retries(_do_live, retries=self.cfg.retries)
        if not ok:
            return False, None, reason
        # If you implement live: return PositionInfo parsed from drift.
        return True, None, f"live_ok market={market}"

    async def health(self) -> HealthInfo:
        """
        Connector health (no network calls in dry-run/disabled).
        """
        if not self.cfg.enabled:
            return HealthInfo(
                ok=True, mode="disabled", provider=self.cfg.provider, detail="PERPS_ENABLED=false"
            )

        if self.cfg.dry_run:
            return HealthInfo(
                ok=True,
                mode="dry_run",
                provider=self.cfg.provider,
                detail=f"positions={len(self._dry_positions)}",
            )

        # live
        if not self._check_live_deps():
            return HealthInfo(
                ok=False, mode="live", provider=self.cfg.provider, detail="driftpy_missing"
            )
        return HealthInfo(
            ok=False, mode="live", provider=self.cfg.provider, detail="live_not_implemented"
        )


__all__ = [
    "DriftPerps",
    "HealthInfo",
    "OrderResult",
    "PerpsConfig",
    "PositionInfo",
]
