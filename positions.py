# positions.py
"""
Positions store (Redis-first, file fallback) for open trades.

- Stores/loads PositionState objects used by execution/exits.py
- Async API with tiny sync shims
- Resilient to schema drift (old/new fields)
- Safe atomic file writes for fallback

Public API (async):
  save_position(mint, pos_or_dict)
  load_position(mint) -> PositionState | None
  list_open_positions() -> list[PositionState]
  update_position(mint, patch_dict)
  close_position(mint)
  snapshot_summary() -> dict

Optional sync shims:
  save_position_sync(...)
  load_positions_sync()
  remove_position_sync(mint)
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from typing import Any, Dict

def load_positions() -> Dict[str, Any]:
    return {}

def save_positions(data: Dict[str, Any]) -> None:
    pass

# --- execution data model (no circular import from exits) ---
try:
    # PositionState(mint, entry_px, size_usd); has .peak_px, .tp1_done (bool), optional .symbol
    from execution.exits import PositionState  # type: ignore
except Exception:
    # Minimal stand-in so editors don’t complain; real class comes from execution.exits
    class PositionState:  # type: ignore
        def __init__(self, mint: str, entry_px: float, size_usd: float):
            self.mint = mint
            self.entry_px = float(entry_px)
            self.size_usd = float(size_usd)
            self.peak_px = float(entry_px)
            self.tp1_done = False
            self.symbol: Optional[str] = None

# --- Redis client (lazy) ---
def _get_redis_client():
    """
    Try both shapes used in your stack:
      - utils_pkg.core_utils.get_redis()
      - utils_pkg.core_utils.redis (object)
    Return None if not available.
    """
    try:
        from utils_pkg.core_utils import get_redis  # type: ignore
        return get_redis()
    except Exception:
        pass
    try:
        from utils_pkg.core_utils import redis as _redis_obj  # type: ignore
        return _redis_obj
    except Exception:
        return None

# --- config / file fallback ---
DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

POSITIONS_JSON = DATA_DIR / "positions.json"
_FILE_LOCK = asyncio.Lock()

# Redis keys
OPEN_SET = "pos:open"              # set of mints
POS_KEY  = "pos:{mint}"            # JSON blob per mint


# ---------- utilities ----------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _coerce_str(x: Any) -> str:
    return x.decode("utf-8") if isinstance(x, (bytes, bytearray)) else str(x)

def _dataclass_to_dict(obj: Any) -> Dict[str, Any]:
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, dict):
        return dict(obj)
    # Fallback: reflect known attributes used by PositionState
    baseline = {}
    for k in ("mint", "entry_px", "size_usd", "peak_px", "tp1_done", "symbol"):
        if hasattr(obj, k):
            baseline[k] = getattr(obj, k)
    return baseline

def _dict_to_position(d: Dict[str, Any]) -> PositionState:
    """
    Tolerate schema drift; fill missing fields from reasonable defaults.
    """
    mint = d.get("mint") or d.get("token_address") or d.get("token") or "UNKNOWN"
    entry = float(d.get("entry_px", 0) or d.get("buy_price", 0) or 0.0)
    size_usd = float(d.get("size_usd", 0.0))
    pos = PositionState(mint=mint, entry_px=entry, size_usd=size_usd)
    # Optional fields
    try:
        if "peak_px" in d and d["peak_px"] is not None:
            pos.peak_px = float(d["peak_px"])
        if "tp1_done" in d and d["tp1_done"] is not None:
            pos.tp1_done = bool(d["tp1_done"])
        if "symbol" in d and d["symbol"] is not None:
            pos.symbol = d["symbol"]
    except Exception:
        pass
    return pos


# ---------- file backend ----------

async def _file_read_positions() -> Dict[str, Dict[str, Any]]:
    async with _FILE_LOCK:
        if not POSITIONS_JSON.exists():
            return {}
        try:
            txt = POSITIONS_JSON.read_text(encoding="utf-8") or "{}"
            obj = json.loads(txt)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            # rotate corrupt file
            try:
                POSITIONS_JSON.rename(POSITIONS_JSON.with_suffix(".corrupt.json"))
            except Exception:
                pass
            return {}

async def _file_write_positions(all_pos: Dict[str, Dict[str, Any]]) -> None:
    async with _FILE_LOCK:
        tmp = POSITIONS_JSON.with_suffix(".tmp")
        tmp.write_text(json.dumps(all_pos, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(POSITIONS_JSON)


# ---------- core API ----------

async def save_position(arg1: Union[str, Dict[str, Any], PositionState], data: Optional[Dict[str, Any]] = None) -> None:
    """
    Save (create or replace) a position.

    Usage:
      await save_position(mint, PositionState(...))
      await save_position(mint, { ... })
      await save_position({ ... 'mint': '...' ... })  # single-arg dict form
    """
    # Normalize inputs
    if isinstance(arg1, str):
        mint = arg1
        payload = _dataclass_to_dict(data if data is not None else {})
        payload["mint"] = mint
    else:
        payload = _dataclass_to_dict(arg1)
        mint = str(payload.get("mint") or payload.get("token_address") or payload.get("token") or "")
        if not mint:
            raise ValueError("save_position: mint not provided in position payload")

    # Ensure required fields exist
    payload.setdefault("peak_px", float(payload.get("entry_px", 0.0)))
    payload.setdefault("tp1_done", False)
    payload.setdefault("updated_at", _now_iso())

    r = _get_redis_client()
    if r:
        try:
            await r.set(POS_KEY.format(mint=mint), json.dumps(payload, separators=(",", ":")))
            await r.sadd(OPEN_SET, mint)
            return
        except Exception:
            # fall through to file backend
            pass

    # file fallback
    all_pos = await _file_read_positions()
    all_pos[mint] = payload
    await _file_write_positions(all_pos)


async def update_position(mint: str, patch: Dict[str, Any]) -> None:
    """
    Merge `patch` into existing position (no-op if not found).
    """
    cur = await load_position(mint)
    if not cur:
        return
    obj = _dataclass_to_dict(cur)
    obj.update(dict(patch))
    obj["updated_at"] = _now_iso()
    await save_position(mint, obj)


async def load_position(mint: str) -> Optional[PositionState]:
    """
    Load a single position as PositionState, or None if missing.
    """
    r = _get_redis_client()
    raw: Optional[Any] = None
    if r:
        try:
            raw = await r.get(POS_KEY.format(mint=mint))
            if raw:
                d = json.loads(_coerce_str(raw))
                return _dict_to_position(d)
        except Exception:
            pass

    # file fallback
    all_pos = await _file_read_positions()
    if mint in all_pos:
        return _dict_to_position(all_pos[mint])
    return None


async def list_open_positions() -> List[PositionState]:
    """
    Return all open positions as a list of PositionState.
    """
    r = _get_redis_client()
    out: List[PositionState] = []
    if r:
        try:
            members = await r.smembers(OPEN_SET) or []
            # smembers may return bytes; normalize to str
            mints = [_coerce_str(m) for m in members]
            for m in mints:
                raw = await r.get(POS_KEY.format(mint=m))
                if raw:
                    try:
                        d = json.loads(_coerce_str(raw))
                        out.append(_dict_to_position(d))
                    except Exception:
                        pass
                else:
                    # cleanup dangling index entries
                    try:
                        await r.srem(OPEN_SET, m)
                    except Exception:
                        pass
            return out
        except Exception:
            pass

    # file fallback
    all_pos = await _file_read_positions()
    for m, d in all_pos.items():
        try:
            out.append(_dict_to_position(d))
        except Exception:
            # tolerate drift
            out.append(_dict_to_position({"mint": m, **(d or {})}))
    return out


async def close_position(mint: str) -> None:
    """
    Remove a position from the active store.
    """
    r = _get_redis_client()
    if r:
        try:
            await r.delete(POS_KEY.format(mint=mint))
            await r.srem(OPEN_SET, mint)
        except Exception:
            # ignore
            pass

    # file fallback cleanup
    all_pos = await _file_read_positions()
    if mint in all_pos:
        del all_pos[mint]
        await _file_write_positions(all_pos)


# ---------- snapshot / helpers ----------

async def load_positions_map() -> Dict[str, Dict[str, Any]]:
    """
    Return raw mapping mint -> dict (for diagnostics/UI).
    """
    r = _get_redis_client()
    if r:
        try:
            members = await r.smembers(OPEN_SET) or []
            mints = [_coerce_str(m) for m in members]
            res: Dict[str, Dict[str, Any]] = {}
            for m in mints:
                raw = await r.get(POS_KEY.format(mint=m))
                if raw:
                    try:
                        res[m] = json.loads(_coerce_str(raw))
                    except Exception:
                        pass
            return res
        except Exception:
            pass
    return await _file_read_positions()


async def snapshot_summary() -> Dict[str, Any]:
    """
    Lightweight summary (count & committed USD).
    Note: live PnL requires price lookups in caller code.
    """
    mp = await load_positions_map()
    count = len(mp)
    total_usd = 0.0
    for d in mp.values():
        try:
            total_usd += float(d.get("size_usd", 0.0) or d.get("amount_invested", 0.0) or 0.0)
        except Exception:
            pass
    return {
        "count": count,
        "capital_committed_usd": round(total_usd, 2),
        "mints": list(mp.keys()),
        "generated_at": _now_iso(),
    }


# ---------- sync shims (optional) ----------

def save_position_sync(arg1, data: Optional[Dict[str, Any]] = None) -> None:
    asyncio.run(save_position(arg1, data))

def load_positions_sync() -> Dict[str, Dict[str, Any]]:
    return asyncio.run(load_positions_map())

def remove_position_sync(mint: str) -> None:
    asyncio.run(close_position(mint))
