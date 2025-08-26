# daytrader/position.py
from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# ------------------------------------------------------------------------------
# Storage (atomic JSON on disk)
# ------------------------------------------------------------------------------
STATE_DIR = Path("./state")
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "positions.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_all() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {}
    return {}


def _save_all(data: Dict[str, Any]) -> None:
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
    tmp.replace(STATE_FILE)


# ------------------------------------------------------------------------------
# Position model
# ------------------------------------------------------------------------------
@dataclass
class Position:
    # Identity / meta
    mint: str
    symbol: str
    side: str = "long"           # "long" or "short"
    leverage: float = 1.0        # informational for risk stats (spot = 1.0)

    # Sizing / pricing (SOL terms by default; keep sign of qty positive)
    qty_token: float = 0.0       # current open qty (tokens)
    entry_sol: float = 0.0       # VWAP entry price (SOL per token)
    stop_sol: float = 0.0
    tp_sol: float = 0.0

    # Live marks
    last_price_sol: Optional[float] = None

    # PnL accounting
    realized_pnl_sol: float = 0.0
    fees_sol: float = 0.0

    # Bookkeeping
    opened_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
    fills: List[Dict[str, Any]] = field(default_factory=list)  # [{ts, side, qty, px, fee}]

    # --- Basic helpers ---------------------------------------------------------
    @property
    def is_flat(self) -> bool:
        return self.qty_token <= 0

    @property
    def direction(self) -> int:
        return 1 if self.side == "long" else -1

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Position":
        # backward compatibility (old files that didn’t store side/leverage)
        d = d.copy()
        d.setdefault("side", "long")
        d.setdefault("leverage", 1.0)
        d.setdefault("realized_pnl_sol", 0.0)
        d.setdefault("fees_sol", 0.0)
        d.setdefault("fills", [])
        return Position(**d)

    # --- Marking & PnL ---------------------------------------------------------
    def mark(self, px_sol: float) -> None:
        """Update mark price (does not change realized)."""
        self.last_price_sol = px_sol
        self.updated_at = _now_iso()

    def unrealized_pnl_sol(self) -> Optional[float]:
        """Unrealized PnL in SOL (uses last_price_sol)."""
        if self.last_price_sol is None or self.is_flat:
            return None
        # For short, pnl flips sign because entry above current is profit
        return (self.last_price_sol - self.entry_sol) * self.qty_token * self.direction

    def total_pnl_sol(self) -> Optional[float]:
        """Realized + Unrealized PnL in SOL."""
        u = self.unrealized_pnl_sol()
        if u is None:
            return self.realized_pnl_sol
        return self.realized_pnl_sol + u

    # --- Risk / R-multiple -----------------------------------------------------
    def risk_per_token_sol(self) -> float:
        """Entry-to-stop distance (positive)."""
        if self.stop_sol <= 0 or self.entry_sol <= 0:
            return 0.0
        dist = (self.entry_sol - self.stop_sol) if self.side == "long" else (self.stop_sol - self.entry_sol)
        return max(dist, 0.0)

    def position_risk_sol(self) -> float:
        """Total nominal risk in SOL (qty * distance)."""
        return self.qty_token * self.risk_per_token_sol()

    def r_multiple(self) -> Optional[float]:
        """Unrealized R multiple = (current - entry) / (entry - stop) with side direction."""
        risk = self.risk_per_token_sol()
        if self.last_price_sol is None or risk <= 0:
            return None
        move = (self.last_price_sol - self.entry_sol) * self.direction
        return move / risk if risk > 0 else None

    # --- Trailing stop logic ---------------------------------------------------
    def trail_stop_pct(self, pct: float, new_price_sol: float) -> bool:
        """
        Raise (long) or lower (short) stop by a trailing % if it improves protection.
        Returns True if stop was updated.
        """
        if new_price_sol <= 0 or pct <= 0:
            return False

        if self.side == "long":
            new_stop = new_price_sol * (1 - pct)
            if new_stop > self.stop_sol:
                self.stop_sol = new_stop
                self.updated_at = _now_iso()
                return True
        else:
            # short: stop trails above price
            new_stop = new_price_sol * (1 + pct)
            if self.stop_sol == 0 or new_stop < self.stop_sol:
                self.stop_sol = new_stop
                self.updated_at = _now_iso()
                return True
        return False

    # --- Fills / VWAP accounting ----------------------------------------------
    def add_fill(self, side: str, qty_token: float, px_sol: float, fee_sol: float = 0.0) -> None:
        """
        Apply a trade fill to the position.
        - side: "buy" or "sell" relative to the market (spot semantics).
        - qty_token: positive number of tokens traded.
        - px_sol: execution price (SOL per token).
        - fee_sol: fee in SOL.
        Handles:
          * Increasing position (DCA)
          * Decreasing position (partial close) with realized PnL
          * Flips (if close > open, we realize + open opposite side)
        """
        assert side in ("buy", "sell"), "side must be 'buy' or 'sell'"
        assert qty_token >= 0, "qty_token must be non-negative"
        if qty_token == 0:
            return

        self.updated_at = _now_iso()
        self.fees_sol += fee_sol
        self.fills.append({
            "ts": self.updated_at,
            "side": side,
            "qty": qty_token,
            "px": px_sol,
            "fee": fee_sol,
        })

        # Normalize to "position direction": long increases on buys, short increases on sells.
        increasing = (side == "buy" and self.side == "long") or (side == "sell" and self.side == "short")

        if self.is_flat:
            # Opening fresh position in the current side direction
            self.qty_token = qty_token
            self.entry_sol = px_sol
            self.last_price_sol = px_sol
            return

        if increasing:
            # DCA: new vwap = (old_cost + new_cost) / (old_qty + new_qty)
            old_cost = self.entry_sol * self.qty_token
            new_cost = px_sol * qty_token
            self.qty_token += qty_token
            self.entry_sol = (old_cost + new_cost) / self.qty_token
            self.last_price_sol = px_sol
        else:
            # Decreasing / closing or flipping
            close_qty = min(qty_token, self.qty_token)
            # Realized PnL for the closed portion
            pnl_per_token = (px_sol - self.entry_sol) * self.direction  # direction flips sign for short
            self.realized_pnl_sol += pnl_per_token * close_qty

            self.qty_token -= close_qty
            self.last_price_sol = px_sol

            remain = qty_token - close_qty
            if self.qty_token == 0:
                # Fully closed; if remain > 0, flip the book to the opposite side and open
                if remain > 0:
                    # Flip side
                    self.side = "short" if self.side == "long" else "long"
                    # Open new position with 'remain'
                    self.qty_token = remain
                    self.entry_sol = px_sol
                    self.stop_sol = 0.0  # require caller to set a new stop
                    # tp can be reset by caller as well
            else:
                # Partial close — VWAP stays the same
                pass

    # --- Convenience operations ------------------------------------------------
    def size_value_sol(self) -> Optional[float]:
        """Notional size in SOL using last mark."""
        if self.last_price_sol is None:
            return None
        return self.qty_token * self.last_price_sol

    def set_targets(self, stop_sol: float, tp_sol: float) -> None:
        self.stop_sol = stop_sol
        self.tp_sol = tp_sol
        self.updated_at = _now_iso()

    def to_tuple(self) -> Tuple[str, str, str, float, float, float, float, float, float]:
        """
        Handy compact tuple for quick table prints:
        (mint, symbol, side, qty, entry, stop, tp, last, realized)
        """
        return (
            self.mint,
            self.symbol,
            self.side,
            self.qty_token,
            self.entry_sol,
            self.stop_sol,
            self.tp_sol,
            (self.last_price_sol or 0.0),
            self.realized_pnl_sol,
        )


# ------------------------------------------------------------------------------
# CRUD helpers (compatible with your existing calls)
# ------------------------------------------------------------------------------
def load_position(mint: str) -> Optional[Position]:
    blob = _load_all()
    d = blob.get(mint)
    if not d:
        return None
    try:
        return Position.from_dict(d)
    except Exception:
        return None


def save_position(pos: Position) -> None:
    blob = _load_all()
    blob[pos.mint] = pos.to_dict()
    _save_all(blob)


def list_positions() -> List[Position]:
    blob = _load_all()
    out: List[Position] = []
    for d in blob.values():
        try:
            out.append(Position.from_dict(d))
        except Exception:
            continue
    return out


def close_position(mint: str) -> None:
    blob = _load_all()
    if mint in blob:
        del blob[mint]
        _save_all(blob)


def close_all_positions() -> None:
    _save_all({})


def new_position(
    mint: str,
    symbol: str,
    qty_token: float,
    entry_sol: float,
    stop_sol: float,
    tp_sol: float,
    side: str = "long",
    leverage: float = 1.0,
) -> Position:
    """
    Backward-compatible factory (your code already calls this signature).
    Extra args side/leverage are optional.
    """
    return Position(
        mint=mint,
        symbol=symbol,
        side=side,
        leverage=leverage,
        qty_token=max(qty_token, 0.0),
        entry_sol=entry_sol,
        stop_sol=stop_sol,
        tp_sol=tp_sol,
        opened_at=_now_iso(),
        updated_at=_now_iso(),
        last_price_sol=entry_sol,
        realized_pnl_sol=0.0,
        fees_sol=0.0,
        fills=[],
    )
