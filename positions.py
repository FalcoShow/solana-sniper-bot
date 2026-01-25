from __future__ import annotations

"""
positions.py (ROOT)

P0 LOCK-IN: ROOT POSITIONS MUST NOT COMPETE WITH utils_pkg.position_store
=======================================================================

This root module historically stored *exit-tracking state* (PositionState) in Redis/file.
Under the new contract:

- Canonical position truth (open/closed positions, qty, entry, meta) lives in:
    utils_pkg.position_store   ✅ SINGLE SOURCE OF TRUTH

- Postgres (utils_pkg.positions_pg) is analytics/mirror/recovery only.

So what is this file now?
-------------------------
A **thin compatibility adapter** for legacy call-sites (mostly execution/exits.py)
that still expect:

  - PositionState objects (entry_px, size_usd, peak_px, tp1_done, symbol)
  - async CRUD helpers + tiny sync shims

Where does it store that state?
------------------------------
Inside PositionStore meta under a single key:

    meta["exit_state"] = { entry_px, size_usd, peak_px, tp1_done, symbol, updated_at }

That means:
- No competing “open-set” indexes at root.
- No separate root schema drift.
- One place to debug positions: PositionStore.

Emergency fallback (optional)
-----------------------------
If PositionStore/Redis is unavailable, we can optionally mirror to a local file for
crash-only recovery. This is NOT a competing truth; it’s a last-resort mirror.

Env knobs:
- POSITIONS_DEFAULT_WALLET: overrides wallet selection
- POSITIONS_FILE_FALLBACK: "1" enables file mirror (default "1")
- POSITIONS_DATA_DIR: default "data"
- POSITIONS_FILE_NAME: default "positions_exit_state.json"

Public async API:
  save_position(mint, pos_or_dict)   OR save_position(pos_dict_or_posstate)
  load_position(mint) -> PositionState | None
  list_open_positions() -> list[PositionState]
  update_position(mint, patch_dict)
  close_position(mint)
  load_positions_map() -> dict[mint, dict]
  snapshot_summary() -> dict

Optional sync shims:
  save_position_sync(...)
  load_positions_sync()
  remove_position_sync(mint)
"""

import asyncio
import contextlib
import json
import os
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Logger (safe import)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore
except Exception:  # pragma: no cover
    import logging

    def get_logger(name: str | None = None):  # type: ignore
        return logging.getLogger(name or "positions")


log = get_logger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# PositionState import (avoid circulars)
# ──────────────────────────────────────────────────────────────────────────────
try:
    # Expected in your stack:
    # PositionState(mint, entry_px, size_usd); has .peak_px, .tp1_done, optional .symbol
    from execution.exits import PositionState  # type: ignore
except Exception:

    class PositionState:  # type: ignore
        def __init__(self, mint: str, entry_px: float, size_usd: float):
            self.mint = str(mint)
            self.entry_px = float(entry_px)
            self.size_usd = float(size_usd)
            self.peak_px = float(entry_px)
            self.tp1_done = False
            self.symbol: str | None = None


# ──────────────────────────────────────────────────────────────────────────────
# Config helpers
# ──────────────────────────────────────────────────────────────────────────────
def _truthy(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _file_fallback_enabled() -> bool:
    # Default ON for crash-only recovery (mirror), but tests can disable via env.
    return _truthy(os.getenv("POSITIONS_FILE_FALLBACK", "1"))


def _data_dir() -> Path:
    return Path(os.getenv("POSITIONS_DATA_DIR", "data") or "data")


def _fallback_file() -> Path:
    name = (
        os.getenv("POSITIONS_FILE_NAME", "positions_exit_state.json") or "positions_exit_state.json"
    )
    return _data_dir() / name


def _ensure_data_dir() -> None:
    # Only create on-demand (avoid import-time FS side effects).
    with contextlib.suppress(Exception):
        _data_dir().mkdir(parents=True, exist_ok=True)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:  # NaN
            return default
        return v
    except Exception:
        return default


def _safe_bool(x: Any, default: bool = False) -> bool:
    try:
        if isinstance(x, bool):
            return x
        s = str(x).strip().lower()
        if s in {"1", "true", "yes", "y", "on"}:
            return True
        if s in {"0", "false", "no", "n", "off"}:
            return False
        return default
    except Exception:
        return default


def _get_config_attr(name: str) -> Any:
    try:
        import config as C  # type: ignore

        return getattr(C, name)
    except Exception:
        return None


def _default_wallet() -> str:
    return (
        (os.getenv("POSITIONS_DEFAULT_WALLET") or "").strip()
        or str(_get_config_attr("WALLET_ADDRESS") or "").strip()
        or str(_get_config_attr("WALLET_PUBKEY") or "").strip()
        or str(_get_config_attr("PUBLIC_KEY") or "").strip()
        or (os.getenv("WALLET_ADDRESS") or "").strip()
        or (os.getenv("WALLET_PUBKEY") or "").strip()
        or (os.getenv("PUBLIC_KEY") or "").strip()
        or "unknown_wallet"
    )


_FILE_LOCK = asyncio.Lock()


# ──────────────────────────────────────────────────────────────────────────────
# Utility conversions
# ──────────────────────────────────────────────────────────────────────────────
def _dataclass_to_dict(obj: Any) -> dict[str, Any]:
    """
    Extract a minimal dict from a dataclass/instance/dict.
    This is intentionally forgiving to support legacy call-sites.
    """
    if obj is None:
        return {}
    if is_dataclass(obj):
        return asdict(obj)  # type: ignore[arg-type]
    if isinstance(obj, dict):
        return dict(obj)
    out: dict[str, Any] = {}
    for k in ("mint", "entry_px", "size_usd", "peak_px", "tp1_done", "symbol", "pool", "wallet"):
        if hasattr(obj, k):
            out[k] = getattr(obj, k)
    return out


def _dict_to_position(d: dict[str, Any]) -> PositionState:
    mint = str(d.get("mint") or d.get("token") or d.get("token_address") or "").strip() or "UNKNOWN"
    entry = _safe_float(d.get("entry_px", d.get("buy_price", 0.0)), 0.0)
    size_usd = _safe_float(d.get("size_usd", d.get("amount_invested", 0.0)), 0.0)
    pos = PositionState(mint=mint, entry_px=entry, size_usd=size_usd)
    with contextlib.suppress(Exception):
        if d.get("peak_px") is not None:
            pos.peak_px = _safe_float(d.get("peak_px"), entry)
        if d.get("tp1_done") is not None:
            pos.tp1_done = _safe_bool(d.get("tp1_done"), False)
        if d.get("symbol") is not None:
            sym = str(d.get("symbol") or "").strip()
            pos.symbol = sym or None
    return pos


def _pos_to_exit_dict(p: PositionState) -> dict[str, Any]:
    d = _dataclass_to_dict(p)
    d["mint"] = str(getattr(p, "mint", "UNKNOWN"))
    d["entry_px"] = _safe_float(getattr(p, "entry_px", 0.0), 0.0)
    d["size_usd"] = _safe_float(getattr(p, "size_usd", 0.0), 0.0)
    d["peak_px"] = _safe_float(getattr(p, "peak_px", d["entry_px"]), d["entry_px"])
    d["tp1_done"] = bool(getattr(p, "tp1_done", False))
    d["symbol"] = getattr(p, "symbol", None)
    d["updated_at"] = _now_iso()
    return d


def _merge_exit_state(existing_meta: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    meta = dict(existing_meta or {})
    cur = meta.get("exit_state")
    if not isinstance(cur, dict):
        cur = {}
    nxt = dict(cur)
    nxt.update({k: v for k, v in (patch or {}).items() if v is not None})
    nxt["updated_at"] = _now_iso()
    meta["exit_state"] = nxt
    return meta


# ──────────────────────────────────────────────────────────────────────────────
# File fallback (mirror-only; never a competing truth)
# ──────────────────────────────────────────────────────────────────────────────
async def _file_read_all() -> dict[str, dict[str, Any]]:
    if not _file_fallback_enabled():
        return {}
    path = _fallback_file()
    async with _FILE_LOCK:
        if not path.exists():
            return {}
        try:
            txt = path.read_text(encoding="utf-8") or "{}"
            obj = json.loads(txt)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            with contextlib.suppress(Exception):
                path.rename(path.with_suffix(".corrupt.json"))
            return {}


async def _file_write_all(mp: dict[str, dict[str, Any]]) -> None:
    if not _file_fallback_enabled():
        return
    _ensure_data_dir()
    path = _fallback_file()
    async with _FILE_LOCK:
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(mp, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(path)


async def _file_set(mint: str, payload: dict[str, Any]) -> None:
    if not _file_fallback_enabled():
        return
    mp = await _file_read_all()
    mp[str(mint)] = dict(payload)
    await _file_write_all(mp)


async def _file_del(mint: str) -> None:
    if not _file_fallback_enabled():
        return
    mp = await _file_read_all()
    if str(mint) in mp:
        del mp[str(mint)]
        await _file_write_all(mp)


# ──────────────────────────────────────────────────────────────────────────────
# PositionStore bridge
# ──────────────────────────────────────────────────────────────────────────────
async def _find_open_pool(wallet: str, mint: str) -> str | None:
    """If pool not specified, pick newest open position for mint."""
    try:
        from utils_pkg import position_store as ps  # type: ignore

        opens = await ps.list_open_positions(wallet)
        cand = [p for p in opens if getattr(p, "mint", "") == mint]
        if not cand:
            return None
        cand.sort(
            key=lambda x: int(getattr(x, "opened_ts", 0) or getattr(x, "created_ts", 0) or 0),
            reverse=True,
        )
        pool = str(getattr(cand[0], "pool", "") or "").strip()
        return pool or None
    except Exception:
        return None


async def _upsert_exit_state_into_store(
    *,
    wallet: str,
    mint: str,
    pool: str,
    patch_exit_state: dict[str, Any],
) -> bool:
    """Return True if write succeeded."""
    try:
        from utils_pkg import position_store as ps  # type: ignore

        with contextlib.suppress(Exception):
            await ps.register_new(
                wallet=wallet,
                mint=mint,
                pool=pool,
                symbol=str(patch_exit_state.get("symbol") or ""),
                reason="EXIT_STATE",
                meta={},
            )

        pos = await ps.get_position(wallet, mint, pool)
        if pos is None:
            return False

        try:
            meta = pos.meta if isinstance(getattr(pos, "meta", None), dict) else {}
        except Exception:
            meta = {}
        meta = _merge_exit_state(meta, patch_exit_state)

        with contextlib.suppress(Exception):
            pos.set_meta(meta)  # type: ignore[attr-defined]
        with contextlib.suppress(Exception):
            pos.meta = meta  # type: ignore[attr-defined]

        s = await ps.get_store()
        await s.upsert(pos, ttl_sec=None)
        return True
    except Exception:
        return False


async def _read_exit_state_from_store(
    *, wallet: str, mint: str
) -> tuple[str, dict[str, Any]] | None:
    """
    Returns (pool, exit_state_dict) for the newest open position that has exit_state, else None.
    """
    try:
        from utils_pkg import position_store as ps  # type: ignore

        opens = await ps.list_open_positions(wallet)
        cand = [p for p in opens if getattr(p, "mint", "") == mint]
        if not cand:
            return None
        cand.sort(
            key=lambda x: int(getattr(x, "opened_ts", 0) or getattr(x, "created_ts", 0) or 0),
            reverse=True,
        )
        for p in cand:
            try:
                meta = p.meta if isinstance(getattr(p, "meta", None), dict) else {}
            except Exception:
                meta = {}
            es = meta.get("exit_state")
            if isinstance(es, dict):
                pool = str(getattr(p, "pool", "") or "unknown_pool")
                return (pool, dict(es))
        return None
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Public async API
# ──────────────────────────────────────────────────────────────────────────────
async def save_position(
    arg1: str | dict[str, Any] | PositionState,
    data: dict[str, Any] | PositionState | None = None,
) -> None:
    """
    Save (create/replace) exit state for a token.

    Supported call shapes:
      await save_position(mint, PositionState(...))
      await save_position(mint, { ... })
      await save_position({ ... 'mint': '...' ... })  # single-arg dict form
      await save_position(PositionState(...))         # single-arg PositionState form
    """
    payload: dict[str, Any]
    if isinstance(arg1, str):
        mint = str(arg1).strip()
        payload = _dataclass_to_dict(data)
        payload["mint"] = mint
    else:
        payload = _dataclass_to_dict(arg1)
        mint = str(
            payload.get("mint") or payload.get("token") or payload.get("token_address") or ""
        ).strip()

    if not mint:
        raise ValueError("save_position: mint not provided")

    pos_obj = arg1 if isinstance(arg1, PositionState) else _dict_to_position(payload)
    exit_dict = _pos_to_exit_dict(pos_obj)

    wallet = str(payload.get("wallet") or _default_wallet()).strip()
    pool = str(payload.get("pool") or "").strip()
    if not pool:
        pool = (await _find_open_pool(wallet, mint)) or "unknown_pool"

    ok = await _upsert_exit_state_into_store(
        wallet=wallet, mint=mint, pool=pool, patch_exit_state=exit_dict
    )

    if _file_fallback_enabled():
        await _file_set(
            mint, {**exit_dict, "wallet": wallet, "pool": pool, "mirrored_at": _now_iso()}
        )

    if not ok:
        with contextlib.suppress(Exception):
            log.warning(
                "EXIT_STATE_SAVE_STORE_FAIL", extra={"mint": mint, "wallet": wallet, "pool": pool}
            )


async def update_position(mint: str, patch: dict[str, Any]) -> None:
    """Merge patch into existing exit_state (no-op if missing)."""
    m = str(mint or "").strip()
    if not m:
        return
    cur = await load_position(m)
    if not cur:
        return

    d = _pos_to_exit_dict(cur)
    d.update({k: v for k, v in (patch or {}).items() if v is not None})
    d["mint"] = m
    await save_position(m, d)


async def load_position(mint: str) -> PositionState | None:
    """Load exit_state as PositionState, or None if not present."""
    m = str(mint or "").strip()
    if not m:
        return None

    wallet = _default_wallet()

    got = await _read_exit_state_from_store(wallet=wallet, mint=m)
    if got:
        _pool, es = got
        dd = dict(es)
        dd["mint"] = m
        return _dict_to_position(dd)

    if _file_fallback_enabled():
        mp = await _file_read_all()
        if m in mp and isinstance(mp[m], dict):
            dd = dict(mp[m])
            dd["mint"] = m
            return _dict_to_position(dd)

    return None


async def list_open_positions() -> list[PositionState]:
    """Return PositionState list for open positions that have exit_state stored in meta."""
    wallet = _default_wallet()
    out: list[PositionState] = []

    with contextlib.suppress(Exception):
        from utils_pkg import position_store as ps  # type: ignore

        opens = await ps.list_open_positions(wallet)
        for p in opens:
            try:
                meta = p.meta if isinstance(getattr(p, "meta", None), dict) else {}
            except Exception:
                meta = {}
            es = meta.get("exit_state")
            if isinstance(es, dict):
                dd = dict(es)
                dd["mint"] = getattr(p, "mint", dd.get("mint", "UNKNOWN"))
                if dd.get("symbol") is None:
                    dd["symbol"] = getattr(p, "symbol", None)
                out.append(_dict_to_position(dd))
        return out

    if _file_fallback_enabled():
        mp = await _file_read_all()
        for m, dd in mp.items():
            if isinstance(dd, dict):
                d2 = dict(dd)
                d2["mint"] = m
                out.append(_dict_to_position(d2))
    return out


async def close_position(mint: str) -> None:
    """
    Remove exit_state for a mint (does NOT close the trade itself).
    This just clears the legacy exit-tracking metadata.
    """
    m = str(mint or "").strip()
    if not m:
        return

    wallet = _default_wallet()

    with contextlib.suppress(Exception):
        from utils_pkg import position_store as ps  # type: ignore

        pool = (await _find_open_pool(wallet, m)) or "unknown_pool"
        pos = await ps.get_position(wallet, m, pool)
        if pos is not None:
            try:
                meta = pos.meta if isinstance(getattr(pos, "meta", None), dict) else {}
            except Exception:
                meta = {}
            if isinstance(meta, dict) and "exit_state" in meta:
                meta2 = dict(meta)
                meta2.pop("exit_state", None)

                with contextlib.suppress(Exception):
                    pos.set_meta(meta2)  # type: ignore[attr-defined]
                with contextlib.suppress(Exception):
                    pos.meta = meta2  # type: ignore[attr-defined]

                s = await ps.get_store()
                await s.upsert(pos, ttl_sec=None)

    await _file_del(m)


async def load_positions_map() -> dict[str, dict[str, Any]]:
    """Raw mapping mint -> exit_state dict (diagnostics/UI)."""
    wallet = _default_wallet()
    res: dict[str, dict[str, Any]] = {}

    with contextlib.suppress(Exception):
        from utils_pkg import position_store as ps  # type: ignore

        opens = await ps.list_open_positions(wallet)
        for p in opens:
            try:
                meta = p.meta if isinstance(getattr(p, "meta", None), dict) else {}
            except Exception:
                meta = {}
            es = meta.get("exit_state")
            if isinstance(es, dict):
                mint = str(getattr(p, "mint", "") or es.get("mint") or "").strip()
                if mint:
                    d = dict(es)
                    d.setdefault("mint", mint)
                    d.setdefault("symbol", getattr(p, "symbol", None))
                    res[mint] = d
        return res

    if _file_fallback_enabled():
        mp = await _file_read_all()
        for m, d in mp.items():
            if isinstance(d, dict):
                d2 = dict(d)
                d2.setdefault("mint", m)
                res[m] = d2
    return res


async def snapshot_summary() -> dict[str, Any]:
    """Lightweight summary of exit-state tracking."""
    mp = await load_positions_map()
    total_usd = 0.0
    for d in mp.values():
        total_usd += _safe_float(d.get("size_usd", 0.0), 0.0)
    return {
        "count": len(mp),
        "capital_committed_usd": round(total_usd, 2),
        "mints": list(mp.keys()),
        "generated_at": _now_iso(),
        "wallet": _default_wallet(),
        "file_fallback_enabled": _file_fallback_enabled(),
        "file_path": str(_fallback_file()),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Sync shims (safe-ish)
# ──────────────────────────────────────────────────────────────────────────────
_LEGACY_TASKS: set[asyncio.Task[Any]] = set()


def _legacy_done_cb(task: asyncio.Task[Any]) -> None:
    _LEGACY_TASKS.discard(task)
    # Force exception retrieval so it doesn't get dropped silently.
    with contextlib.suppress(asyncio.CancelledError, Exception):
        _ = task.exception()


def _in_running_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _schedule(coro: Any) -> None:
    """
    Best-effort "fire-and-forget" scheduling for legacy sync shims when already
    inside an event loop. Prefer tracked helper if present; otherwise track locally.
    """
    with contextlib.suppress(Exception):
        from utils_pkg.task_spawn import spawn  # type: ignore

        spawn(coro)
        return

    with contextlib.suppress(Exception):
        task = asyncio.get_running_loop().create_task(coro)
        _LEGACY_TASKS.add(task)
        task.add_done_callback(_legacy_done_cb)


def _run_sync(coro: Any) -> Any:
    if _in_running_loop():
        _schedule(coro)
        return None
    return asyncio.run(coro)


def save_position_sync(arg1: Any, data: dict[str, Any] | None = None) -> None:
    _run_sync(save_position(arg1, data))


def load_positions_sync() -> dict[str, dict[str, Any]]:
    out = _run_sync(load_positions_map())
    return out if isinstance(out, dict) else {}


def remove_position_sync(mint: str) -> None:
    _run_sync(close_position(mint))


__all__ = [
    "close_position",
    "list_open_positions",
    "load_position",
    "load_positions_map",
    "load_positions_sync",
    "remove_position_sync",
    "save_position",
    "save_position_sync",
    "snapshot_summary",
    "update_position",
]
