from __future__ import annotations

"""
positions.py (ROOT)

P0 LOCK-IN: ROOT POSITIONS MUST NOT COMPETE WITH utils_pkg.position_store
=======================================================================

This root module historically stored *exit-tracking state* in Redis/file.
Under the new contract:

- Canonical position truth (open/closed positions, qty, entry, meta) lives in:
    utils_pkg.position_store   ✅ SINGLE SOURCE OF TRUTH

- Postgres (utils_pkg.positions_pg) is analytics/mirror/recovery only.

So what is this file now?
-------------------------
A **thin compatibility adapter** for legacy call-sites that still expect:

  - PositionState objects (entry_px, size_usd, peak_px, tp1_done, symbol)
  - async CRUD helpers + tiny sync shims

Where does it store that state?
------------------------------
Inside PositionStore meta under a single key:

    meta["exit_state"] = { entry_px, size_usd, peak_px, tp1_done, symbol, updated_at, ... }

That means:
- No competing “open-set” indexes at root.
- No separate root schema drift.
- One place to debug positions: PositionStore.

Crash-only file mirror (optional)
---------------------------------
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
import inspect
import json
import logging
import os
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Coroutine, Protocol, cast, overload

# =============================================================================
# Logging (Phase A: eliminate Logger ↔ EventLogger typing fallout)
# =============================================================================


class LoggerLike(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> None: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...

    def event(self, name: str, **kw: Any) -> None: ...
    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None: ...


class _LogAdapter:
    """
    Normalizes Logger/EventLogger differences *without mutating* the underlying object.

    - Guarantees .debug/.info/.warning/.error/.exception
    - Guarantees .event/.every (fallback implementations)
    - Defends against signature mismatches (eg, log.warning(..., extra=...) not accepted)
    """

    __slots__ = ("_base", "_every_store")

    def __init__(self, base: Any) -> None:
        self._base = base
        self._every_store: dict[str, float] = {}

    def _call(self, method: str, msg: str, *args: Any, **kwargs: Any) -> None:
        fn = getattr(self._base, method, None)
        if callable(fn):
            try:
                fn(msg, *args, **kwargs)
                return
            except TypeError:
                # Some custom loggers reject kwargs like "extra"
                with contextlib.suppress(Exception):
                    fn(msg, *args)
                    return
            except Exception:
                # Never let logging crash the bot
                return

        # fallback to stdlib logger
        std = logging.getLogger(__name__)
        getattr(std, method, std.info)(msg, *args, **kwargs)

    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._call("debug", msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._call("info", msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._call("warning", msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._call("error", msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        # Prefer underlying .exception if present; else emulate via error + exc_info
        fn = getattr(self._base, "exception", None)
        if callable(fn):
            with contextlib.suppress(TypeError, Exception):
                fn(msg, *args, **kwargs)
                return
        self._call("error", msg, *args, exc_info=True, **kwargs)

    def event(self, name: str, **kw: Any) -> None:
        fn = getattr(self._base, "event", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(name, **kw)
                return
        # Fallback: structured-ish one-liner
        self.info("%s %s", name, kw)

    def every(self, key: str, n: int = 10, msg: str | None = None, **kw: Any) -> None:
        fn = getattr(self._base, "every", None)
        if callable(fn):
            with contextlib.suppress(Exception):
                fn(key, n=n, msg=msg, **kw)
                return

        # fallback: log at most once per `n` seconds per key
        try:
            now = float(asyncio.get_running_loop().time())
        except Exception:
            now = float(datetime.now(UTC).timestamp())

        last = float(self._every_store.get(key, 0.0))
        interval = float(max(1, int(n)))
        if (now - last) >= interval:
            self._every_store[key] = now
            self.event(msg or key, **kw)


def _fallback_get_logger(name: str | None = None) -> logging.Logger:
    return logging.getLogger(name or "positions")


def _resolve_get_logger() -> Callable[..., Any]:
    try:  # pragma: no cover
        from utils_pkg.log_compat import get_logger as gl  # type: ignore

        return gl
    except Exception:  # pragma: no cover
        try:
            from logging_setup import get_logger as gl  # type: ignore

            return gl
        except Exception:  # pragma: no cover
            return _fallback_get_logger


_GET_LOGGER = _resolve_get_logger()


def _get_log(name: str) -> LoggerLike:
    base: Any = None
    with contextlib.suppress(Exception):
        base = _GET_LOGGER(name)
    if base is None:
        base = _fallback_get_logger(name)
    return cast(LoggerLike, _LogAdapter(base))


log = _get_log(__name__)

# =============================================================================
# PositionState import (avoid circulars; provide fallback)
# =============================================================================

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


# =============================================================================
# Constants / env helpers
# =============================================================================

_EXIT_META_KEY = "exit_state"
_FILE_LOCK = asyncio.Lock()


def _truthy(x: Any) -> bool:
    return str(x or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _file_fallback_enabled() -> bool:
    # Default ON for crash-only recovery mirror; tests can disable via env.
    return _truthy(os.getenv("POSITIONS_FILE_FALLBACK", "1"))


def _data_dir() -> Path:
    return Path(os.getenv("POSITIONS_DATA_DIR", "data") or "data")


def _fallback_file() -> Path:
    name = (
        os.getenv("POSITIONS_FILE_NAME", "positions_exit_state.json") or "positions_exit_state.json"
    )
    return _data_dir() / name


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
    # Avoid import-time coupling; best-effort lookup only.
    with contextlib.suppress(Exception):
        import config as C  # type: ignore

        return getattr(C, name)
    return None


def _default_wallet() -> str:
    # Priority: explicit POSITIONS_DEFAULT_WALLET -> config -> env -> placeholder.
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


# =============================================================================
# Small async helpers
# =============================================================================


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _to_thread(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    return await asyncio.to_thread(func, *args, **kwargs)


def _pos_sort_ts(p: Any) -> int:
    # Use opened_ts / created_ts if present, else 0
    with contextlib.suppress(Exception):
        v = getattr(p, "opened_ts", None)
        if v is not None:
            return int(v or 0)
    with contextlib.suppress(Exception):
        v = getattr(p, "created_ts", None)
        if v is not None:
            return int(v or 0)
    return 0


# =============================================================================
# Forgiving conversions (legacy-friendly)
# =============================================================================


def _is_posstate_like(obj: Any) -> bool:
    return (
        obj is not None
        and hasattr(obj, "mint")
        and hasattr(obj, "entry_px")
        and hasattr(obj, "size_usd")
    )


def _obj_to_dict(obj: Any) -> dict[str, Any]:
    """
    Extract a forgiving dict from a dataclass/instance/dict.
    Intentionally permissive to support legacy call-sites.
    """
    if obj is None:
        return {}
    if is_dataclass(obj):
        return asdict(obj)  # type: ignore[arg-type]
    if isinstance(obj, dict):
        return dict(obj)

    out: dict[str, Any] = {}
    for k in (
        "mint",
        "token",
        "token_address",
        "entry_px",
        "size_usd",
        "peak_px",
        "tp1_done",
        "symbol",
        "pool",
        "wallet",
    ):
        if hasattr(obj, k):
            out[k] = getattr(obj, k)
    return out


def _construct_posstate(mint: str, entry_px: float, size_usd: float) -> PositionState:
    # Try multiple construction styles safely.
    with contextlib.suppress(Exception):
        return PositionState(mint=mint, entry_px=entry_px, size_usd=size_usd)  # type: ignore[call-arg]
    with contextlib.suppress(Exception):
        return PositionState(mint, entry_px, size_usd)  # type: ignore[call-arg]
    # Last resort: create then set attrs
    pos = PositionState(mint, entry_px, size_usd)  # type: ignore[call-arg]
    with contextlib.suppress(Exception):
        pos.mint = mint
        pos.entry_px = entry_px
        pos.size_usd = size_usd
    return pos


def _dict_to_position(d: dict[str, Any]) -> PositionState:
    mint = str(d.get("mint") or d.get("token") or d.get("token_address") or "").strip() or "UNKNOWN"
    entry = _safe_float(d.get("entry_px", d.get("buy_price", 0.0)), 0.0)
    size_usd = _safe_float(d.get("size_usd", d.get("amount_invested", 0.0)), 0.0)

    pos = _construct_posstate(mint, entry, size_usd)

    with contextlib.suppress(Exception):
        if d.get("peak_px") is not None:
            pos.peak_px = _safe_float(d.get("peak_px"), entry)
    with contextlib.suppress(Exception):
        if d.get("tp1_done") is not None:
            pos.tp1_done = _safe_bool(d.get("tp1_done"), False)
    with contextlib.suppress(Exception):
        if d.get("symbol") is not None:
            sym = str(d.get("symbol") or "").strip()
            pos.symbol = sym or None

    return pos


def _pos_to_exit_dict(p: Any) -> dict[str, Any]:
    """
    Convert a PositionState-like object into the canonical exit_state dict.
    """
    base = _obj_to_dict(p)
    mint = str(getattr(p, "mint", base.get("mint", "UNKNOWN")) or "UNKNOWN")
    entry = _safe_float(getattr(p, "entry_px", base.get("entry_px", 0.0)), 0.0)
    size_usd = _safe_float(getattr(p, "size_usd", base.get("size_usd", 0.0)), 0.0)
    peak = _safe_float(getattr(p, "peak_px", base.get("peak_px", entry)), entry)
    tp1_done = bool(getattr(p, "tp1_done", base.get("tp1_done", False)))
    symbol = getattr(p, "symbol", base.get("symbol", None))

    out: dict[str, Any] = {
        "mint": mint,
        "entry_px": entry,
        "size_usd": size_usd,
        "peak_px": peak,
        "tp1_done": tp1_done,
        "symbol": symbol,
        "updated_at": _now_iso(),
    }

    # Preserve extra keys (legacy-friendly) without overriding core fields.
    for k, v in base.items():
        if k not in out:
            out[k] = v

    return out


def _merge_exit_state(
    existing_meta: dict[str, Any], patch_exit_state: dict[str, Any]
) -> dict[str, Any]:
    meta = dict(existing_meta or {})
    cur = meta.get(_EXIT_META_KEY)
    if not isinstance(cur, dict):
        cur = {}
    nxt = dict(cur)
    for k, v in (patch_exit_state or {}).items():
        if v is not None:
            nxt[k] = v
    nxt["updated_at"] = _now_iso()
    meta[_EXIT_META_KEY] = nxt
    return meta


# =============================================================================
# File mirror (crash-only fallback; never a competing truth)
# =============================================================================


def _ensure_data_dir_sync() -> None:
    with contextlib.suppress(Exception):
        _data_dir().mkdir(parents=True, exist_ok=True)


async def _file_read_all() -> dict[str, dict[str, Any]]:
    if not _file_fallback_enabled():
        return {}

    path = _fallback_file()
    async with _FILE_LOCK:
        if not path.exists():
            return {}

        try:
            txt = await _to_thread(path.read_text, encoding="utf-8")
            obj = json.loads(txt or "{}")
            return obj if isinstance(obj, dict) else {}
        except Exception:
            # Move corrupt file aside to avoid repeated parse failures (best effort)
            with contextlib.suppress(Exception):
                stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
                await _to_thread(path.replace, path.with_suffix(f".corrupt.{stamp}.json"))
            return {}


async def _file_write_all(mp: dict[str, dict[str, Any]]) -> None:
    if not _file_fallback_enabled():
        return

    _ensure_data_dir_sync()
    path = _fallback_file()
    tmp = path.with_suffix(".tmp")
    payload = json.dumps(mp, ensure_ascii=False, separators=(",", ":"))

    async with _FILE_LOCK:
        await _to_thread(tmp.write_text, payload, encoding="utf-8")
        await _to_thread(tmp.replace, path)


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
    key = str(mint)
    if key in mp:
        del mp[key]
        await _file_write_all(mp)


# =============================================================================
# PositionStore bridge (best-effort, defensive against shape changes)
# =============================================================================


def _import_position_store() -> Any:
    with contextlib.suppress(Exception):
        from utils_pkg import position_store as ps  # type: ignore

        return ps
    return None


async def _list_open_positions_store(wallet: str) -> list[Any]:
    ps = _import_position_store()
    if ps is None:
        return []
    try:
        res = await _maybe_await(ps.list_open_positions(wallet))  # type: ignore[attr-defined]
        return list(res or [])
    except Exception:
        return []


async def _find_open_pool(wallet: str, mint: str) -> str | None:
    opens = await _list_open_positions_store(wallet)
    if not opens:
        return None
    m = str(mint)
    cand = [p for p in opens if str(getattr(p, "mint", "")) == m]
    if not cand:
        return None
    cand.sort(key=_pos_sort_ts, reverse=True)
    pool = str(getattr(cand[0], "pool", "") or "").strip()
    return pool or None


async def _upsert_exit_state_into_store(
    *, wallet: str, mint: str, pool: str, exit_state: dict[str, Any]
) -> bool:
    ps = _import_position_store()
    if ps is None:
        return False

    # Optional: ensure a position exists if the store supports it
    with contextlib.suppress(Exception):
        reg = getattr(ps, "register_new", None)
        if callable(reg):
            await _maybe_await(
                reg(
                    wallet=wallet,
                    mint=mint,
                    pool=pool,
                    symbol=str(exit_state.get("symbol") or ""),
                    reason="EXIT_STATE",
                    meta={},
                )
            )

    try:
        pos = await _maybe_await(ps.get_position(wallet, mint, pool))  # type: ignore[attr-defined]
    except Exception:
        pos = None
    if pos is None:
        return False

    try:
        meta_obj = getattr(pos, "meta", None)
        meta = meta_obj if isinstance(meta_obj, dict) else {}
    except Exception:
        meta = {}

    meta = _merge_exit_state(meta, exit_state)

    with contextlib.suppress(Exception):
        setter = getattr(pos, "set_meta", None)
        if callable(setter):
            setter(meta)
        else:
            pos.meta = meta  # type: ignore[attr-defined]

    # Resolve store handle
    store: Any = None
    with contextlib.suppress(Exception):
        getter = getattr(ps, "get_store", None)
        if callable(getter):
            store = await _maybe_await(getter())
    if store is None:
        with contextlib.suppress(Exception):
            store = getattr(ps, "STORE", None)
        with contextlib.suppress(Exception):
            store = getattr(ps, "store", store)

    if store is None:
        return False

    try:
        upsert = getattr(store, "upsert", None)
        if callable(upsert):
            await _maybe_await(upsert(pos, ttl_sec=None))
            return True
    except Exception:
        return False

    return False


async def _read_exit_state_from_store(
    *, wallet: str, mint: str
) -> tuple[str, dict[str, Any]] | None:
    """
    Returns (pool, exit_state_dict) for the newest open position that has exit_state, else None.
    """
    opens = await _list_open_positions_store(wallet)
    if not opens:
        return None

    m = str(mint)
    cand = [p for p in opens if str(getattr(p, "mint", "")) == m]
    if not cand:
        return None

    cand.sort(key=_pos_sort_ts, reverse=True)
    for p in cand:
        try:
            meta_obj = getattr(p, "meta", None)
            meta = meta_obj if isinstance(meta_obj, dict) else {}
        except Exception:
            meta = {}

        es = meta.get(_EXIT_META_KEY)
        if isinstance(es, dict):
            pool = str(getattr(p, "pool", "") or "unknown_pool")
            return pool, dict(es)

    return None


# =============================================================================
# Public async API
# =============================================================================


@overload
async def save_position(mint: str, data: dict[str, Any] | PositionState) -> None: ...


@overload
async def save_position(pos_or_dict: dict[str, Any] | PositionState, /) -> None: ...


async def save_position(
    mint: str | dict[str, Any] | PositionState,
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
    mint_str: str
    pos_obj: Any

    if isinstance(mint, str):
        mint_str = mint.strip()
        payload = _obj_to_dict(data)
        payload["mint"] = mint_str
        pos_obj = data
    else:
        payload = _obj_to_dict(mint)
        mint_str = str(
            payload.get("mint") or payload.get("token") or payload.get("token_address") or ""
        ).strip()
        pos_obj = mint

    if not mint_str:
        raise ValueError("save_position: mint not provided")

    pos_state = pos_obj if _is_posstate_like(pos_obj) else _dict_to_position(payload)
    exit_dict = _pos_to_exit_dict(pos_state)

    wallet = str(payload.get("wallet") or _default_wallet()).strip()
    pool = (
        str(payload.get("pool") or "").strip()
        or (await _find_open_pool(wallet, mint_str))
        or "unknown_pool"
    )

    ok = await _upsert_exit_state_into_store(
        wallet=wallet, mint=mint_str, pool=pool, exit_state=exit_dict
    )

    # Crash-only file mirror
    if _file_fallback_enabled():
        await _file_set(
            mint_str,
            {**exit_dict, "wallet": wallet, "pool": pool, "mirrored_at": _now_iso()},
        )

    if not ok:
        log.warning(
            "EXIT_STATE_SAVE_STORE_FAIL", extra={"mint": mint_str, "wallet": wallet, "pool": pool}
        )


async def load_position(mint: str) -> PositionState | None:
    m = str(mint or "").strip()
    if not m:
        return None

    wallet = _default_wallet()

    got = await _read_exit_state_from_store(wallet=wallet, mint=m)
    if got:
        _pool, es = got
        d = dict(es)
        d["mint"] = m
        return _dict_to_position(d)

    if _file_fallback_enabled():
        mp = await _file_read_all()
        if m in mp and isinstance(mp[m], dict):
            d = dict(mp[m])
            d["mint"] = m
            return _dict_to_position(d)

    return None


async def list_open_positions() -> list[PositionState]:
    wallet = _default_wallet()
    out: list[PositionState] = []

    opens = await _list_open_positions_store(wallet)
    if opens:
        for p in opens:
            try:
                meta_obj = getattr(p, "meta", None)
                meta = meta_obj if isinstance(meta_obj, dict) else {}
            except Exception:
                meta = {}

            es = meta.get(_EXIT_META_KEY)
            if not isinstance(es, dict):
                continue

            d = dict(es)
            d["mint"] = getattr(p, "mint", d.get("mint", "UNKNOWN"))
            if d.get("symbol") is None:
                d["symbol"] = getattr(p, "symbol", None)

            out.append(_dict_to_position(d))
        return out

    if _file_fallback_enabled():
        mp = await _file_read_all()
        for m, d in mp.items():
            if isinstance(d, dict):
                d2 = dict(d)
                d2["mint"] = m
                out.append(_dict_to_position(d2))

    return out


async def load_positions_map() -> dict[str, dict[str, Any]]:
    wallet = _default_wallet()
    res: dict[str, dict[str, Any]] = {}

    opens = await _list_open_positions_store(wallet)
    if opens:
        opens.sort(key=_pos_sort_ts, reverse=True)

        for p in opens:
            try:
                meta_obj = getattr(p, "meta", None)
                meta = meta_obj if isinstance(meta_obj, dict) else {}
            except Exception:
                meta = {}

            es = meta.get(_EXIT_META_KEY)
            if not isinstance(es, dict):
                continue

            mint = str(getattr(p, "mint", "") or es.get("mint") or "").strip()
            if not mint or mint in res:
                continue

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


async def update_position(mint: str, patch: dict[str, Any]) -> None:
    m = str(mint or "").strip()
    if not m:
        return

    patch = dict(patch or {})
    wallet = str(patch.pop("wallet", "") or "").strip() or _default_wallet()
    pool = str(patch.pop("pool", "") or "").strip()

    current = await _read_exit_state_from_store(wallet=wallet, mint=m)
    cur_pool: str | None = None
    cur_dict: dict[str, Any] | None = None

    if current:
        cur_pool, cur_dict = current
    elif _file_fallback_enabled():
        mp = await _file_read_all()
        if m in mp and isinstance(mp[m], dict):
            cur_dict = dict(mp[m])

    if not cur_dict:
        return

    merged = dict(cur_dict)
    for k, v in patch.items():
        if v is not None:
            merged[k] = v
    merged["mint"] = m
    merged["updated_at"] = _now_iso()

    if not pool:
        pool = cur_pool or (await _find_open_pool(wallet, m)) or "unknown_pool"

    ok = await _upsert_exit_state_into_store(wallet=wallet, mint=m, pool=pool, exit_state=merged)

    if _file_fallback_enabled():
        await _file_set(m, {**merged, "wallet": wallet, "pool": pool, "mirrored_at": _now_iso()})

    if not ok:
        log.warning(
            "EXIT_STATE_UPDATE_STORE_FAIL", extra={"mint": m, "wallet": wallet, "pool": pool}
        )


async def close_position(mint: str) -> None:
    """
    Remove exit_state for a mint (does NOT close the trade itself).
    This just clears the legacy exit-tracking metadata.
    """
    m = str(mint or "").strip()
    if not m:
        return

    wallet = _default_wallet()
    ps = _import_position_store()

    if ps is not None:
        opens = await _list_open_positions_store(wallet)
        for p in opens:
            if str(getattr(p, "mint", "")) != m:
                continue

            try:
                meta_obj = getattr(p, "meta", None)
                meta = meta_obj if isinstance(meta_obj, dict) else {}
            except Exception:
                meta = {}

            if not isinstance(meta, dict) or _EXIT_META_KEY not in meta:
                continue

            meta2 = dict(meta)
            meta2.pop(_EXIT_META_KEY, None)

            with contextlib.suppress(Exception):
                setter = getattr(p, "set_meta", None)
                if callable(setter):
                    setter(meta2)
                else:
                    p.meta = meta2  # type: ignore[attr-defined]

            # Persist
            store: Any = None
            with contextlib.suppress(Exception):
                getter = getattr(ps, "get_store", None)
                if callable(getter):
                    store = await _maybe_await(getter())
            if store is None:
                with contextlib.suppress(Exception):
                    store = getattr(ps, "STORE", None)
                with contextlib.suppress(Exception):
                    store = getattr(ps, "store", store)

            if store is not None:
                with contextlib.suppress(Exception):
                    upsert = getattr(store, "upsert", None)
                    if callable(upsert):
                        await _maybe_await(upsert(p, ttl_sec=None))

    await _file_del(m)


async def snapshot_summary() -> dict[str, Any]:
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


# =============================================================================
# Sync shims (legacy)
# =============================================================================

_LEGACY_TASKS: set[asyncio.Task[Any]] = set()


def _legacy_done_cb(task: asyncio.Task[Any]) -> None:
    _LEGACY_TASKS.discard(task)
    with contextlib.suppress(asyncio.CancelledError, Exception):
        _ = task.exception()


def _in_running_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _schedule(coro: Coroutine[Any, Any, Any]) -> None:
    """
    Best-effort fire-and-forget scheduling for legacy sync shims when already
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


def _run_sync(coro: Coroutine[Any, Any, Any]) -> Any:
    if _in_running_loop():
        _schedule(coro)
        return None
    return asyncio.run(coro)


def save_position_sync(arg1: Any, data: dict[str, Any] | None = None) -> None:
    _run_sync(save_position(arg1, data))  # type: ignore[arg-type]


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
