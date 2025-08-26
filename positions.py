# positions.py
"""
Centralized positions store with Redis-first backend and atomic JSON fallback.

Features
- Active positions CRUD: save/get/update/remove/list/load
- Closed trade logging to CSV/JSON (append-only)
- Safe, atomic local file writes (Windows-friendly)
- Ready for future Postgres migration (schema kept simple)
- Async-first API with tiny sync shims (opt-in)
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Optional import of shared Redis client from your utils
# (init_resources() in utils_pkg.core_utils sets this)
try:
    from utils_pkg.core_utils import redis as _shared_redis  # type: ignore
except Exception:
    _shared_redis = None  # falls back to file backend


# ---------- config ----------

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

POSITIONS_JSON = DATA_DIR / "positions.json"          # active positions (fallback)
TRADES_JSONL = DATA_DIR / "trades_closed.jsonl"       # append-only closed trades log
TRADES_CSV = DATA_DIR / "trades_closed.csv"           # simple CSV mirror (optional)

# Redis keys
REDIS_INDEX_KEY = "positions:index"                   # Set of token_addresses
REDIS_POS_PREFIX = "pos:"                             # pos:{token_address}
REDIS_TRADES_LIST = "trades:closed"                   # capped list (optional)

# Cap for Redis trades list
REDIS_TRADES_MAX = 2000

# In-process lock for file operations
_FILE_LOCK = asyncio.Lock()


# ---------- schema ----------

@dataclass
class Position:
    token_address: str                 # SPL mint
    buy_price: float                   # in SOL (or quote unit you use)
    amount_invested: float             # in SOL (capital risked)
    timestamp: str                     # ISO (UTC) buy time
    trailing_stop: Optional[float] = None
    symbol: Optional[str] = None
    entry_tx: Optional[str] = None
    source: Optional[str] = None       # e.g., 'dexscreener', 'pumpfun', etc.
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.setdefault("meta", {})
        return d


@dataclass
class ClosedTrade:
    token_address: str
    symbol: Optional[str]
    buy_price: float
    sell_price: float
    amount: float                      # amount of token or SOL invested (whichever you track)
    profit_sol: float                  # profit in SOL (can be negative)
    profit_pct: float                  # ((sell - buy)/buy)*100
    buy_time: str                      # ISO
    sell_time: str                     # ISO
    reason: str                        # exit reason (stop-loss, TP, AI, time, manual, etc.)
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.setdefault("meta", {})
        return d


# ---------- backend helpers ----------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _redis_available() -> bool:
    return _shared_redis is not None

async def _file_read_positions() -> Dict[str, Dict[str, Any]]:
    async with _FILE_LOCK:
        if not POSITIONS_JSON.exists():
            return {}
        text = POSITIONS_JSON.read_text(encoding="utf-8") or "{}"
        try:
            return json.loads(text)
        except Exception:
            # Corrupted file — rotate & start fresh
            POSITIONS_JSON.rename(POSITIONS_JSON.with_suffix(".corrupt.json"))
            return {}

async def _file_write_positions(obj: Dict[str, Dict[str, Any]]) -> None:
    async with _FILE_LOCK:
        tmp = POSITIONS_JSON.with_suffix(".tmp")
        tmp.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(POSITIONS_JSON)

async def _file_append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    async with _FILE_LOCK:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

async def _file_append_csv(row: Dict[str, Any]) -> None:
    # very light CSV writer (header if missing)
    HEAD = [
        "token_address","symbol","buy_price","sell_price","amount",
        "profit_sol","profit_pct","buy_time","sell_time","reason"
    ]
    async with _FILE_LOCK:
        write_header = not TRADES_CSV.exists()
        with TRADES_CSV.open("a", encoding="utf-8", newline="") as f:
            if write_header:
                f.write(",".join(HEAD) + "\n")
            vals = [
                str(row.get("token_address","")),
                str(row.get("symbol","") or "")),
                str(row.get("buy_price","")),
                str(row.get("sell_price","")),
                str(row.get("amount","")),
                str(row.get("profit_sol","")),
                str(row.get("profit_pct","")),
                str(row.get("buy_time","")),
                str(row.get("sell_time","")),
                str(row.get("reason","")),
            ]
            f.write(",".join(vals) + "\n")


# ---------- public API (async) ----------

async def load_positions() -> Dict[str, Dict[str, Any]]:
    """
    Return mapping token_address -> position dict.
    Uses Redis (preferred). Falls back to JSON file.
    """
    if _redis_available():
        # return all keys in index
        try:
            members = await _shared_redis.smembers(REDIS_INDEX_KEY)  # type: ignore
            res: Dict[str, Dict[str, Any]] = {}
            for token in members:
                raw = await _shared_redis.get(f"{REDIS_POS_PREFIX}{token}")  # type: ignore
                if raw:
                    try:
                        res[token] = json.loads(raw)
                    except Exception:
                        pass
            return res
        except Exception:
            # fallback to file
            pass
    return await _file_read_positions()


async def list_active_tokens() -> List[str]:
    if _redis_available():
        try:
            members = await _shared_redis.smembers(REDIS_INDEX_KEY)  # type: ignore
            # normalize to str list
            return [str(m) for m in members]
        except Exception:
            pass
    pos = await _file_read_positions()
    return list(pos.keys())


async def get_position(token_address: str) -> Optional[Dict[str, Any]]:
    if _redis_available():
        try:
            raw = await _shared_redis.get(f"{REDIS_POS_PREFIX}{token_address}")  # type: ignore
            if raw:
                return json.loads(raw)
        except Exception:
            pass
    pos = await _file_read_positions()
    return pos.get(token_address)


async def save_position(token_address: str, data: Dict[str, Any]) -> None:
    """
    Insert or replace a position for token_address.
    """
    data = dict(data)
    data.setdefault("token_address", token_address)
    data.setdefault("timestamp", _now_iso())

    if _redis_available():
        try:
            await _shared_redis.set(f"{REDIS_POS_PREFIX}{token_address}", json.dumps(data, separators=(",", ":")))  # type: ignore
            await _shared_redis.sadd(REDIS_INDEX_KEY, token_address)  # type: ignore
            return
        except Exception:
            # fallback to file
            pass

    obj = await _file_read_positions()
    obj[token_address] = data
    await _file_write_positions(obj)


async def update_position(token_address: str, patch: Dict[str, Any]) -> None:
    """
    Merge patch into existing position.
    """
    cur = await get_position(token_address) or {}
    cur.update(patch)
    await save_position(token_address, cur)


async def remove_position(token_address: str) -> None:
    """
    Remove a position from the active store.
    """
    if _redis_available():
        try:
            await _shared_redis.delete(f"{REDIS_POS_PREFIX}{token_address}")  # type: ignore
            await _shared_redis.srem(REDIS_INDEX_KEY, token_address)  # type: ignore
            # continue to file removal to keep parity
        except Exception:
            pass

    obj = await _file_read_positions()
    if token_address in obj:
        del obj[token_address]
        await _file_write_positions(obj)


async def record_closed_trade(trade: ClosedTrade, mirror_csv: bool = True, mirror_redis: bool = True) -> None:
    """
    Append a closed trade to JSONL (and CSV mirror).
    Optionally push into Redis capped list.
    """
    row = trade.to_dict()
    row.setdefault("logged_at", _now_iso())

    await _file_append_jsonl(TRADES_JSONL, row)
    if mirror_csv:
        await _file_append_csv(row)

    if mirror_redis and _redis_available():
        try:
            await _shared_redis.lpush(REDIS_TRADES_LIST, json.dumps(row, separators=(",", ":")))  # type: ignore
            await _shared_redis.ltrim(REDIS_TRADES_LIST, 0, REDIS_TRADES_MAX - 1)  # type: ignore
        except Exception:
            pass


# ---------- convenience helpers ----------

async def ensure_position(
    token_address: str,
    buy_price: float,
    amount_invested: float,
    symbol: Optional[str] = None,
    trailing_stop: Optional[float] = None,
    entry_tx: Optional[str] = None,
    source: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Create if missing; otherwise no-op (does not overwrite existing fields).
    """
    cur = await get_position(token_address)
    if cur:
        return
    pos = Position(
        token_address=token_address,
        buy_price=buy_price,
        amount_invested=amount_invested,
        timestamp=_now_iso(),
        trailing_stop=trailing_stop,
        symbol=symbol,
        entry_tx=entry_tx,
        source=source,
        meta=meta or {},
    )
    await save_position(token_address, pos.to_dict())


async def iter_positions() -> List[Position]:
    """
    Return a list of Position dataclass objects for convenience.
    """
    obj = await load_positions()
    out: List[Position] = []
    for k, v in obj.items():
        try:
            out.append(Position(**v))
        except Exception:
            # tolerate schema drift
            v = dict(v)
            v.setdefault("token_address", k)
            v.setdefault("timestamp", _now_iso())
            out.append(Position(**v))
    return out


# ---------- sync shims (optional) ----------

def save_position_sync(token_address: str, data: Dict[str, Any]) -> None:
    asyncio.run(save_position(token_address, data))

def load_positions_sync() -> Dict[str, Dict[str, Any]]:
    return asyncio.run(load_positions())

def remove_position_sync(token_address: str) -> None:
    asyncio.run(remove_position(token_address))

def record_closed_trade_sync(trade: ClosedTrade, mirror_csv: bool = True, mirror_redis: bool = True) -> None:
    asyncio.run(record_closed_trade(trade, mirror_csv=mirror_csv, mirror_redis=mirror_redis))


# ---------- (optional) portfolio snapshot ----------

async def snapshot_summary() -> Dict[str, Any]:
    """
    Lightweight snapshot (counts & capital at risk).
    NOTE: If you want live PnL, combine with your price fetcher in caller code.
    """
    pos = await load_positions()
    n = len(pos)
    cap = sum(float(p.get("amount_invested", 0.0)) for p in pos.values())
    return {
        "count": n,
        "capital_at_risk_sol": round(cap, 6),
        "tokens": list(pos.keys()),
        "generated_at": _now_iso(),
    }


# ---------- handy factory for ClosedTrade ----------

def make_closed_trade(
    *,
    token_address: str,
    symbol: Optional[str],
    entry_price: float,
    sell_price: float,
    amount: float,
    buy_time: datetime,
    sell_time: Optional[datetime] = None,
    reason: str,
    extra_meta: Optional[Dict[str, Any]] = None,
) -> ClosedTrade:
    pct = 0.0
    if entry_price:
        pct = ((sell_price - entry_price) / entry_price) * 100.0
    profit_sol = (sell_price - entry_price) * amount
    return ClosedTrade(
        token_address=token_address,
        symbol=symbol,
        buy_price=entry_price,
        sell_price=sell_price,
        amount=amount,
        profit_sol=round(profit_sol, 9),
        profit_pct=round(pct, 4),
        buy_time=buy_time.replace(tzinfo=timezone.utc).isoformat(),
        sell_time=(sell_time or datetime.now(timezone.utc)).isoformat(),
        reason=reason,
        meta=extra_meta or {},
    )
