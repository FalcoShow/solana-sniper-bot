# daytrader/position.py
from __future__ import annotations

"""
daytrader.position
------------------
Canonical Daytrader position state + persistence.

Hard goals
- Deterministic, production-safe, no surprises.
- Backward compatible with your existing helpers:
    load_position(mint), save_position(pos), list_positions(),
    close_position(mint), close_all_positions(), new_position(...)
- Atomic writes + best-effort cross-platform process lock (Windows-safe).
- Fill accounting:
    * DCA (VWAP)
    * partial closes with realized PnL
    * optional flip support (spot-short informational; used only if you call it)
    * dedupe via fill_id

IMPORTANT (architecture / “no competing engines”)
- This is Daytrader-only state. It does NOT compete with sniper/positions.
- Keep Daytrader disabled unless you explicitly run daytrader/runner.py.

Storage backend
- Default: JSON file in DT_STATE_DIR (./state) named DT_POSITIONS_FILE (positions.json).
- Optional: Redis (DT_POS_BACKEND=redis + REDIS_URL) using key DT_POS_REDIS_KEY.
  Redis backend is “single-writer friendly” but still uses a lock to avoid stomps.

Units
- This module stores prices in an abstract quote unit (px_ccy).
  Legacy fields use SOL terms (entry_sol/stop_sol/...), so default px_ccy="SOL".
  If your daytrader uses USD/USDC quotes, set px_ccy="USD" or "USDC" when creating.

"""

import contextlib
import json
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ----------------------------- Config -----------------------------------------

DT_STATE_DIR = Path(os.getenv("DT_STATE_DIR", "./state"))
DT_POSITIONS_FILE = os.getenv("DT_POSITIONS_FILE", "positions.json")
DT_LOCK_TIMEOUT_SEC = float(os.getenv("DT_POS_LOCK_TIMEOUT_SEC", "4.0"))
DT_LOCK_POLL_MS = int(os.getenv("DT_POS_LOCK_POLL_MS", "80"))
DT_LOCK_STALE_SEC = float(os.getenv("DT_POS_LOCK_STALE_SEC", "60.0"))

DT_POS_BACKEND = os.getenv("DT_POS_BACKEND", "file").strip().lower()  # file | redis | none
DT_POS_REDIS_KEY = os.getenv("DT_POS_REDIS_KEY", "daytrader:positions")

DT_STATE_DIR.mkdir(parents=True, exist_ok=True)
_STATE_FILE = DT_STATE_DIR / DT_POSITIONS_FILE
_LOCK_FILE = DT_STATE_DIR / f"{DT_POSITIONS_FILE}.lock"

_SCHEMA_VERSION = 2  # bump when changing serialized layout


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if (v == v and abs(v) != float("inf")) else default  # NaN/inf guard
    except Exception:
        return default


def _s(x: Any, default: str = "") -> str:
    try:
        return str(x) if x is not None else default
    except Exception:
        return default


# ----------------------------- Locking (cross-platform) -----------------------


class _FileLock:
    """
    Cross-platform best-effort lock using atomic lock-file creation (O_EXCL).
    Safe on Windows. Avoids external deps.

    If a lock becomes stale (process crashed), we reclaim after DT_POS_LOCK_STALE_SEC.
    """

    def __init__(self, path: Path, timeout_sec: float, poll_ms: int, stale_sec: float):
        self.path = path
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.poll_ms = max(10, int(poll_ms))
        self.stale_sec = max(1.0, float(stale_sec))
        self._held = False

    def acquire(self) -> bool:
        deadline = time.time() + self.timeout_sec
        while time.time() < deadline:
            try:
                fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                try:
                    payload = {"pid": os.getpid(), "ts": time.time()}
                    os.write(fd, json.dumps(payload).encode("utf-8"))
                finally:
                    os.close(fd)
                self._held = True
                return True
            except FileExistsError:
                # Reclaim stale lock
                try:
                    st = self.path.stat()
                    age = time.time() - st.st_mtime
                    if age > self.stale_sec:
                        try:
                            self.path.unlink(missing_ok=True)  # py3.8+: missing_ok
                        except TypeError:
                            if self.path.exists():
                                self.path.unlink()
                except Exception:
                    pass
                time.sleep(self.poll_ms / 1000.0)
            except Exception:
                time.sleep(self.poll_ms / 1000.0)
        return False

    def release(self) -> None:
        if not self._held:
            return
        try:
            self.path.unlink(missing_ok=True)  # type: ignore[arg-type]
        except TypeError:
            if self.path.exists():
                with contextlib.suppress(Exception):
                    self.path.unlink()
        except Exception:
            pass
        self._held = False

    def __enter__(self) -> _FileLock:
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


# ----------------------------- Redis (optional) --------------------------------


def _redis_client():
    url = os.getenv("REDIS_URL", "").strip()
    if not url:
        return None
    try:
        import redis  # type: ignore

        return redis.from_url(url, decode_responses=True, ssl=url.startswith("rediss://"))
    except Exception:
        return None


# ----------------------------- Storage IO -------------------------------------


def _read_json_file() -> dict[str, Any]:
    if not _STATE_FILE.exists():
        return {}
    try:
        txt = _STATE_FILE.read_text(encoding="utf-8")
        if not txt.strip():
            return {}
        obj = json.loads(txt)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _write_json_file_atomic(obj: dict[str, Any]) -> None:
    tmp = _STATE_FILE.with_suffix(_STATE_FILE.suffix + ".tmp")
    data = json.dumps(obj, indent=2, sort_keys=True)
    tmp.write_text(data, encoding="utf-8")
    os.replace(str(tmp), str(_STATE_FILE))


def _read_redis() -> dict[str, Any]:
    r = _redis_client()
    if not r:
        return {}
    try:
        raw = r.get(DT_POS_REDIS_KEY)
        if not raw:
            return {}
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _write_redis(obj: dict[str, Any]) -> None:
    r = _redis_client()
    if not r:
        return
    try:
        r.set(DT_POS_REDIS_KEY, json.dumps(obj))
    except Exception:
        return


def _load_all() -> dict[str, Any]:
    if DT_POS_BACKEND == "redis":
        return _read_redis()
    if DT_POS_BACKEND == "none":
        return {}
    return _read_json_file()


def _save_all(obj: dict[str, Any]) -> None:
    if DT_POS_BACKEND == "redis":
        _write_redis(obj)
        return
    if DT_POS_BACKEND == "none":
        return
    _write_json_file_atomic(obj)


# ----------------------------- Position Model ---------------------------------


@dataclass
class Position:
    # Identity
    mint: str
    symbol: str

    # Optional: pair name (e.g., "SOL/USDC") for daytrader strategies
    pair: str | None = None

    # Direction
    side: str = "long"  # "long" | "short" (short is informational in spot)
    leverage: float = 1.0

    # Sizing / pricing
    qty_token: float = 0.0  # open qty in token units (always positive)
    entry_px: float = 0.0  # VWAP entry price (quote per token)
    stop_px: float = 0.0
    tp_px: float = 0.0
    px_ccy: str = "SOL"  # legacy default; can be "USDC"/"USD" etc.

    # Marks
    last_px: float | None = None

    # Accounting
    realized_pnl: float = 0.0
    fees: float = 0.0

    # Bookkeeping
    opened_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)

    fills: list[dict[str, Any]] = field(
        default_factory=list
    )  # [{ts, side, qty, px, fee, id?, sig?}]
    fill_ids: list[str] = field(default_factory=list)  # dedupe ids

    meta: dict[str, Any] = field(default_factory=dict)
    schema_version: int = _SCHEMA_VERSION

    # --------------------------- Derived --------------------------------------

    @property
    def is_flat(self) -> bool:
        return self.qty_token <= 1e-18

    @property
    def direction(self) -> int:
        return 1 if self.side.strip().lower() == "long" else -1

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # keep schema_version explicit
        d["schema_version"] = int(self.schema_version or _SCHEMA_VERSION)
        return d

    @staticmethod
    def from_dict(d: dict[str, Any]) -> Position:
        """
        Backward-compatible loader:
        - Accepts legacy keys: entry_sol/stop_sol/tp_sol/last_price_sol/realized_pnl_sol/fees_sol
        - Accepts old files missing side/leverage/fills/fill_ids/meta/px_ccy
        """
        x = dict(d or {})

        # migrate legacy naming
        if "entry_sol" in x and "entry_px" not in x:
            x["entry_px"] = x.pop("entry_sol")
        if "stop_sol" in x and "stop_px" not in x:
            x["stop_px"] = x.pop("stop_sol")
        if "tp_sol" in x and "tp_px" not in x:
            x["tp_px"] = x.pop("tp_sol")
        if "last_price_sol" in x and "last_px" not in x:
            x["last_px"] = x.pop("last_price_sol")
        if "realized_pnl_sol" in x and "realized_pnl" not in x:
            x["realized_pnl"] = x.pop("realized_pnl_sol")
        if "fees_sol" in x and "fees" not in x:
            x["fees"] = x.pop("fees_sol")

        # defaults
        x.setdefault("pair", None)
        x.setdefault("side", "long")
        x.setdefault("leverage", 1.0)
        x.setdefault("qty_token", 0.0)
        x.setdefault("entry_px", 0.0)
        x.setdefault("stop_px", 0.0)
        x.setdefault("tp_px", 0.0)
        x.setdefault("px_ccy", "SOL")
        x.setdefault("last_px", None)
        x.setdefault("realized_pnl", 0.0)
        x.setdefault("fees", 0.0)
        x.setdefault("opened_at", _now_iso())
        x.setdefault("updated_at", _now_iso())
        x.setdefault("fills", [])
        x.setdefault("fill_ids", [])
        x.setdefault("meta", {})
        x.setdefault("schema_version", x.get("schema_version", 1))

        # sanitize types lightly
        x["mint"] = _s(x.get("mint", ""), "")
        x["symbol"] = _s(x.get("symbol", ""), "")
        x["side"] = _s(x.get("side", "long"), "long").lower().strip()
        if x["side"] not in ("long", "short"):
            x["side"] = "long"

        x["leverage"] = max(1.0, _f(x.get("leverage", 1.0), 1.0))
        x["qty_token"] = max(0.0, _f(x.get("qty_token", 0.0), 0.0))
        x["entry_px"] = max(0.0, _f(x.get("entry_px", 0.0), 0.0))
        x["stop_px"] = max(0.0, _f(x.get("stop_px", 0.0), 0.0))
        x["tp_px"] = max(0.0, _f(x.get("tp_px", 0.0), 0.0))
        x["realized_pnl"] = _f(x.get("realized_pnl", 0.0), 0.0)
        x["fees"] = _f(x.get("fees", 0.0), 0.0)
        x["px_ccy"] = _s(x.get("px_ccy", "SOL"), "SOL").upper().strip() or "SOL"

        # last_px may be None
        lp = x.get("last_px")
        x["last_px"] = None if lp is None else _f(lp, 0.0)

        # coerce lists
        if not isinstance(x.get("fills", []), list):
            x["fills"] = []
        if not isinstance(x.get("fill_ids", []), list):
            x["fill_ids"] = []
        if not isinstance(x.get("meta", {}), dict):
            x["meta"] = {}

        return Position(**x)

    # --------------------------- Marking & PnL --------------------------------

    def mark(self, px: float) -> None:
        px = _f(px, 0.0)
        if px <= 0:
            return
        self.last_px = px
        self.updated_at = _now_iso()

    def unrealized_pnl(self) -> float | None:
        if self.is_flat or self.last_px is None or self.entry_px <= 0:
            return None
        return (self.last_px - self.entry_px) * self.qty_token * self.direction

    def total_pnl(self) -> float:
        u = self.unrealized_pnl()
        return float(self.realized_pnl + (u or 0.0))

    # --------------------------- Risk / R multiple -----------------------------

    def risk_per_token(self) -> float:
        if self.stop_px <= 0 or self.entry_px <= 0:
            return 0.0
        # distance should be positive
        dist = (
            (self.entry_px - self.stop_px)
            if self.side == "long"
            else (self.stop_px - self.entry_px)
        )
        return max(0.0, float(dist))

    def position_risk(self) -> float:
        return float(self.qty_token * self.risk_per_token())

    def r_multiple(self) -> float | None:
        risk = self.risk_per_token()
        if risk <= 0 or self.last_px is None or self.entry_px <= 0:
            return None
        move = (self.last_px - self.entry_px) * self.direction
        return float(move / risk) if risk > 0 else None

    # --------------------------- Trailing stop --------------------------------

    def trail_stop_pct(self, pct: float, new_px: float) -> bool:
        pct = _f(pct, 0.0)
        new_px = _f(new_px, 0.0)
        if pct <= 0 or new_px <= 0:
            return False

        if self.side == "long":
            candidate = new_px * (1.0 - pct)
            if candidate > (self.stop_px or 0.0):
                self.stop_px = candidate
                self.updated_at = _now_iso()
                return True
        else:
            # short: stop trails above price
            candidate = new_px * (1.0 + pct)
            if self.stop_px <= 0 or candidate < self.stop_px:
                self.stop_px = candidate
                self.updated_at = _now_iso()
                return True
        return False

    # --------------------------- Fills / VWAP accounting -----------------------

    def _has_fill_id(self, fill_id: str | None) -> bool:
        if not fill_id:
            return False
        fid = str(fill_id)
        # fill_ids is kept small; linear scan is fine
        return fid in self.fill_ids

    def add_fill(
        self,
        side: str,
        qty_token: float,
        px: float,
        fee: float = 0.0,
        *,
        fill_id: str | None = None,
        tx_sig: str | None = None,
        note: str | None = None,
        ts_iso: str | None = None,
    ) -> bool:
        """
        Apply a trade fill to this position.

        Args:
          side     : "buy" or "sell" (spot semantics)
          qty_token: positive qty in token units
          px       : execution price in quote units (px_ccy per token)
          fee      : fee in quote units (same as px_ccy)
          fill_id  : optional unique id for dedupe (highly recommended)
          tx_sig   : optional onchain signature
          note     : optional freeform note
          ts_iso   : override timestamp (UTC ISO)

        Returns:
          True if applied, False if rejected or deduped.
        """
        s = str(side or "").lower().strip()
        if s not in ("buy", "sell"):
            return False

        qty = _f(qty_token, 0.0)
        px = _f(px, 0.0)
        fee = max(0.0, _f(fee, 0.0))

        if qty <= 0 or px <= 0:
            return False

        if fill_id and self._has_fill_id(fill_id):
            return False

        ts = ts_iso or _now_iso()
        self.updated_at = ts

        # record
        rec: dict[str, Any] = {"ts": ts, "side": s, "qty": qty, "px": px, "fee": fee}
        if fill_id:
            rec["id"] = str(fill_id)
            self.fill_ids.append(str(fill_id))
        if tx_sig:
            rec["sig"] = str(tx_sig)
        if note:
            rec["note"] = str(note)
        self.fills.append(rec)

        self.fees += fee
        self.last_px = px

        # If flat, open in the direction implied by fill
        if self.is_flat:
            self.side = "long" if s == "buy" else "short"
            self.qty_token = qty
            self.entry_px = px
            return True

        # Increasing if:
        # - long + buy
        # - short + sell
        increasing = (self.side == "long" and s == "buy") or (self.side == "short" and s == "sell")

        if increasing:
            # DCA VWAP
            old_qty = self.qty_token
            new_qty = old_qty + qty
            if new_qty <= 0:
                return False
            old_cost = self.entry_px * old_qty
            new_cost = px * qty
            self.qty_token = new_qty
            self.entry_px = (old_cost + new_cost) / new_qty
            return True

        # Decreasing / closing
        close_qty = min(qty, self.qty_token)
        pnl_per_token = (px - self.entry_px) * self.direction
        self.realized_pnl += pnl_per_token * close_qty
        self.qty_token -= close_qty

        remain = qty - close_qty
        if self.qty_token <= 1e-18:
            # fully closed
            self.qty_token = 0.0
            # flip if remainder exists
            if remain > 1e-18:
                self.side = "short" if self.side == "long" else "long"
                self.qty_token = remain
                self.entry_px = px
                # targets invalid after flip; caller should set new ones
                self.stop_px = 0.0
                self.tp_px = 0.0
        # partial close keeps entry_px unchanged
        return True

    # --------------------------- Convenience ----------------------------------

    def set_targets(self, stop_px: float, tp_px: float) -> None:
        sp = max(0.0, _f(stop_px, 0.0))
        tp = max(0.0, _f(tp_px, 0.0))
        self.stop_px = sp
        self.tp_px = tp
        self.updated_at = _now_iso()

    def size_value_quote(self) -> float | None:
        if self.last_px is None or self.last_px <= 0 or self.is_flat:
            return None
        return float(self.qty_token * self.last_px)

    # Back-compat: tuple in the old field order
    def to_tuple(self) -> tuple[str, str, str, float, float, float, float, float, float]:
        return (
            self.mint,
            self.symbol,
            self.side,
            float(self.qty_token),
            float(self.entry_px),
            float(self.stop_px),
            float(self.tp_px),
            float(self.last_px or 0.0),
            float(self.realized_pnl),
        )


# ----------------------------- Store ------------------------------------------


class PositionStore:
    """
    Single-writer friendly store. Uses lock + atomic writes.

    Internal format:
      {
        "__meta__": {"schema_version": 2, "updated_at": "..."},
        "<mint>": {Position dict...},
        ...
      }
    """

    def __init__(self):
        self.lock = _FileLock(_LOCK_FILE, DT_LOCK_TIMEOUT_SEC, DT_LOCK_POLL_MS, DT_LOCK_STALE_SEC)

    def load_all(self) -> dict[str, Any]:
        blob = _load_all()
        if not isinstance(blob, dict):
            return {}
        return blob

    def save_all(self, blob: dict[str, Any]) -> None:
        if not isinstance(blob, dict):
            return
        blob.setdefault("__meta__", {})
        if isinstance(blob["__meta__"], dict):
            blob["__meta__"]["schema_version"] = _SCHEMA_VERSION
            blob["__meta__"]["updated_at"] = _now_iso()
        _save_all(blob)

    def load_position(self, mint: str) -> Position | None:
        m = str(mint or "").strip()
        if not m:
            return None
        with self.lock:
            blob = self.load_all()
            d = blob.get(m)
            if not isinstance(d, dict):
                return None
            try:
                return Position.from_dict(d)
            except Exception:
                return None

    def save_position(self, pos: Position) -> None:
        if not pos or not pos.mint:
            return
        with self.lock:
            blob = self.load_all()
            blob[pos.mint] = pos.to_dict()
            self.save_all(blob)

    def list_positions(self) -> list[Position]:
        out: list[Position] = []
        with self.lock:
            blob = self.load_all()
        for k, v in blob.items():
            if k == "__meta__":
                continue
            if isinstance(v, dict):
                try:
                    p = Position.from_dict(v)
                    out.append(p)
                except Exception:
                    continue
        return out

    def close_position(self, mint: str) -> None:
        m = str(mint or "").strip()
        if not m:
            return
        with self.lock:
            blob = self.load_all()
            if m in blob:
                del blob[m]
                self.save_all(blob)

    def close_all(self) -> None:
        with self.lock:
            self.save_all(
                {"__meta__": {"schema_version": _SCHEMA_VERSION, "updated_at": _now_iso()}}
            )


_STORE = PositionStore()

# ----------------------------- Module-level API (back-compat) -----------------


def load_position(mint: str) -> Position | None:
    return _STORE.load_position(mint)


def save_position(pos: Position) -> None:
    _STORE.save_position(pos)


def list_positions() -> list[Position]:
    return _STORE.list_positions()


def close_position(mint: str) -> None:
    _STORE.close_position(mint)


def close_all_positions() -> None:
    _STORE.close_all()


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
    Backward-compatible factory (keeps legacy arg names).
    Interprets entry_sol/stop_sol/tp_sol as entry_px/stop_px/tp_px with px_ccy="SOL".

    If your daytrader is quoting in USDC/USD instead, set pos.px_ccy manually after creating,
    or create via Position(...) directly.
    """
    s = (side or "long").lower().strip()
    if s not in ("long", "short"):
        s = "long"
    pos = Position(
        mint=str(mint),
        symbol=str(symbol),
        side=s,
        leverage=max(1.0, _f(leverage, 1.0)),
        qty_token=max(0.0, _f(qty_token, 0.0)),
        entry_px=max(0.0, _f(entry_sol, 0.0)),
        stop_px=max(0.0, _f(stop_sol, 0.0)),
        tp_px=max(0.0, _f(tp_sol, 0.0)),
        px_ccy="SOL",
        opened_at=_now_iso(),
        updated_at=_now_iso(),
        last_px=max(0.0, _f(entry_sol, 0.0)) if _f(entry_sol, 0.0) > 0 else None,
    )
    return pos


__all__ = [
    "Position",
    "PositionStore",
    "close_all_positions",
    "close_position",
    "list_positions",
    "load_position",
    "new_position",
    "save_position",
]
