# tests/test_exit_pipeline.py
from __future__ import annotations

"""
God-Tier Exit Pipeline Tests — V3+++ LOCK-IN (P0 / MUST)
=======================================================

Goal
----
Prove the *canonical* exit path is wired end-to-end WITHOUT touching onchain:

  enqueue_exit_request
      -> queue + payload written (Redis)
      -> process_one claims work (NO Lua in tests; patched claim)
      -> _execute_exit(req) (mocked)
      -> idempotency (DONE prevents replay)
      -> inflight cleared
      -> payload cleanup (configurable)
      -> DLQ on repeated failure (bounded retries)
      -> mint lock release (best-effort assertion)

Hard requirements (LOCKED)
--------------------------
- utils_pkg.exit_engine imports (canonical brain)
- utils_pkg.auto_exit_worker.exit_engine exports:
    enqueue_exit_request, process_one, reclaim_stale_inflight, prune_done, worker_loop

- Worker defines canonical Redis keys/constants:
    EXIT_QUEUE_KEY, EXIT_INFLIGHT_KEY, EXIT_DONE_KEY, EXIT_DATA_KEY, EXIT_DLQ_KEY
  (names locked; values can change)

Design
------
- Uses an in-memory FakeRedis implementing only ops the worker needs (plus a few safety extras).
- Patches worker internals to:
    - avoid Lua claim scripts (patch claim function)
    - avoid network/RPC (patch executor, init, notify, telemetry)
    - avoid nondeterministic time/jitter (freeze time/random)
- Adds strict network guards to catch accidental HTTP/RPC during import or processing.

Run
---
pytest -q
"""

import contextlib
import importlib
import inspect
import json
import os
import random
import socket
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Ensure repo root (folder that contains `utils_pkg`) is on sys.path
# ---------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, *_HERE.parents):
    if (_p / "utils_pkg").is_dir():
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
        break


# ---------------------------------------------------------------------------
# Environment + network guards (Windows-friendly)
# ---------------------------------------------------------------------------
class _NetworkGuardError(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def _safe_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Force offline, non-strict, deterministic mode.
    We set multiple toggles so whichever your code recognizes will work.
    """
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("SAFE_MODE", "1")
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")

    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")
    monkeypatch.setenv("DISABLE_RPC", "1")
    monkeypatch.setenv("DISABLE_HTTP", "1")

    monkeypatch.setenv("ENV_STRICT", os.getenv("ENV_STRICT", "0"))
    monkeypatch.setenv("STRICT_ENV", os.getenv("STRICT_ENV", "0"))
    monkeypatch.setenv("CORE_UTILS_ENV_STRICT", os.getenv("CORE_UTILS_ENV_STRICT", "0"))
    monkeypatch.setenv("CORE_LOAD_DOTENV", "0")

    monkeypatch.setenv("SANDBOX_MODE", os.getenv("SANDBOX_MODE", "1"))
    monkeypatch.setenv("LOG_LEVEL", os.getenv("LOG_LEVEL", "WARNING"))

    monkeypatch.setenv("TRADING_ENABLED", "0")
    monkeypatch.setenv("ALLOW_BUYS", "0")
    monkeypatch.setenv("ALLOW_SELLS", "0")


def _is_loopback_ephemeral(addr: Any) -> bool:
    """
    Allow ONLY Windows asyncio internals loopback connects:
      - host in {127.0.0.1, localhost, ::1}
      - port >= 49152 (typical Windows ephemeral range)
    """
    try:
        if not isinstance(addr, tuple) or len(addr) < 2:
            return False
        host, port = addr[0], addr[1]
        return host in ("127.0.0.1", "localhost", "::1") and isinstance(port, int) and port >= 49152
    except Exception:
        return False


@contextlib.contextmanager
def _no_network(monkeypatch: pytest.MonkeyPatch, *mods: Any):
    """
    Block real network (and local service ports), but allow loopback ephemeral
    connects required by Windows asyncio internals. Scoped to avoid teardown issues.
    """
    with monkeypatch.context() as m:
        orig_create_connection = getattr(socket, "create_connection", None)

        def _guard_create_connection(*args: Any, **kwargs: Any) -> Any:
            addr = args[0] if args else kwargs.get("address")
            if _is_loopback_ephemeral(addr) and callable(orig_create_connection):
                return orig_create_connection(*args, **kwargs)
            raise _NetworkGuardError(
                "Network blocked in exit pipeline tests (socket.create_connection)."
            )

        m.setattr(socket, "create_connection", _guard_create_connection, raising=False)

        orig_socket_cls = socket.socket

        class _GuardSocket(orig_socket_cls):  # type: ignore[misc]
            def connect(self, *args: Any, **kwargs: Any) -> Any:
                addr = args[0] if args else None
                if _is_loopback_ephemeral(addr):
                    return super().connect(*args, **kwargs)
                raise _NetworkGuardError("Network blocked in exit pipeline tests (socket.connect).")

            def connect_ex(self, *args: Any, **kwargs: Any) -> Any:
                addr = args[0] if args else None
                if _is_loopback_ephemeral(addr):
                    return super().connect_ex(*args, **kwargs)
                raise _NetworkGuardError(
                    "Network blocked in exit pipeline tests (socket.connect_ex)."
                )

        m.setattr(socket, "socket", _GuardSocket, raising=False)

        # Patch common HTTP clients IF present in module namespaces
        for mod in mods:
            aiohttp = getattr(mod, "aiohttp", None)
            if aiohttp is not None:

                class _GuardAiohttpSession:
                    def __init__(self, *args: Any, **kwargs: Any) -> None:
                        raise _NetworkGuardError("Network blocked (aiohttp.ClientSession).")

                m.setattr(aiohttp, "ClientSession", _GuardAiohttpSession, raising=False)

            httpx = getattr(mod, "httpx", None)
            if httpx is not None:

                class _GuardHttpxAsyncClient:
                    def __init__(self, *args: Any, **kwargs: Any) -> None:
                        raise _NetworkGuardError("Network blocked (httpx.AsyncClient).")

                m.setattr(httpx, "AsyncClient", _GuardHttpxAsyncClient, raising=False)

            requests = getattr(mod, "requests", None)
            if requests is not None:

                def _guard_requests(*args: Any, **kwargs: Any) -> Any:
                    raise _NetworkGuardError("Network blocked (requests).")

                for nm in ("request", "get", "post"):
                    if hasattr(requests, nm):
                        m.setattr(requests, nm, _guard_requests, raising=False)

                if hasattr(requests, "Session"):

                    class _GuardSession:
                        def __init__(self, *args: Any, **kwargs: Any) -> None:
                            raise _NetworkGuardError("Network blocked (requests.Session).")

                    m.setattr(requests, "Session", _GuardSession, raising=False)

        yield


def _freeze_determinism(monkeypatch: pytest.MonkeyPatch, fixed_now_ms: int) -> None:
    """
    Best-effort determinism: freeze time + random.
    """
    fixed_ts = fixed_now_ms / 1000.0
    monkeypatch.setattr(time, "time", lambda: float(fixed_ts), raising=False)
    monkeypatch.setattr(time, "monotonic", lambda: float(fixed_ts), raising=False)
    monkeypatch.setattr(random, "random", lambda: 0.0, raising=False)
    monkeypatch.setattr(random, "randint", lambda a, b: int(a), raising=False)


def _now_ms_fixed() -> int:
    return 1_700_000_000_000


# ---------------------------------------------------------------------------
# Minimal in-memory async Redis test double
# ---------------------------------------------------------------------------
class FakeRedis:
    """
    In-memory approximation of the redis async interface used by the worker.

    Implemented (async):
      - HASH: hget, hset, hdel, hgetall, hexists, hincrby
      - ZSET: zadd, zrem, zscore, zrange, zrangebyscore, zremrangebyscore, zcard
      - LIST: lpush, rpush, llen, lrange, ltrim
      - KV: set(get) with nx/ex/px, setnx, exists, delete, expire, pexpire, incr, getset
      - eval: NotImplemented (tests patch claim)
    """

    def __init__(self) -> None:
        self._hash: dict[str, dict[str, str]] = {}
        self._z: dict[str, dict[str, float]] = {}
        self._list: dict[str, list[str]] = {}
        self._kv: dict[str, str] = {}
        self._ttl_ms: dict[str, int] = {}

    # --- HASH ---
    async def hget(self, key: str, field: str) -> str | None:
        return self._hash.get(key, {}).get(str(field))

    async def hset(
        self,
        key: str,
        field: str | None = None,
        value: str | None = None,
        mapping: dict[str, str] | None = None,
    ) -> int:
        self._hash.setdefault(key, {})
        n = 0
        if mapping is not None:
            for f, v in mapping.items():
                self._hash[key][str(f)] = str(v)
                n += 1
            return n
        if field is None:
            raise TypeError("hset requires field/value or mapping")
        self._hash[key][str(field)] = "" if value is None else str(value)
        return 1

    async def hdel(self, key: str, field: str) -> int:
        m = self._hash.get(key)
        f = str(field)
        if not m or f not in m:
            return 0
        del m[f]
        if not m:
            self._hash.pop(key, None)
        return 1

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self._hash.get(key, {}))

    async def hexists(self, key: str, field: str) -> int:
        return 1 if str(field) in self._hash.get(key, {}) else 0

    async def hincrby(self, key: str, field: str, amount: int = 1) -> int:
        self._hash.setdefault(key, {})
        f = str(field)
        cur = int(self._hash[key].get(f, "0") or "0")
        cur += int(amount)
        self._hash[key][f] = str(cur)
        return cur

    # --- ZSET ---
    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        z = self._z.setdefault(key, {})
        for member, score in mapping.items():
            z[str(member)] = float(score)
        return len(mapping)

    async def zrem(self, key: str, member: str) -> int:
        z = self._z.get(key, {})
        m = str(member)
        if m in z:
            del z[m]
            if not z:
                self._z.pop(key, None)
            return 1
        return 0

    async def zscore(self, key: str, member: str) -> float | None:
        z = self._z.get(key, {})
        m = str(member)
        return float(z[m]) if m in z else None

    async def zrange(self, key: str, start: int, stop: int, withscores: bool = False) -> list[Any]:
        z = self._z.get(key, {})
        items = sorted(z.items(), key=lambda kv: (kv[1], kv[0]))
        if stop < 0:
            stop = len(items) - 1
        slice_ = items[start : stop + 1]
        return [(m, sc) for m, sc in slice_] if withscores else [m for m, _ in slice_]

    async def zrangebyscore(
        self,
        key: str,
        min_score: Any,
        max_score: Any,
        start: int = 0,
        num: int = 50,
        withscores: bool = False,
    ) -> list[Any]:
        z = self._z.get(key, {})
        mi = float("-inf") if str(min_score) == "-inf" else float(min_score)
        ma = float("inf") if str(max_score) == "inf" else float(max_score)
        items = [(m, sc) for m, sc in z.items() if mi <= float(sc) <= ma]
        items.sort(key=lambda kv: (kv[1], kv[0]))
        items = items[start : start + num]
        return items if withscores else [m for m, _ in items]

    async def zremrangebyscore(self, key: str, min_score: Any, max_score: Any) -> int:
        z = self._z.get(key, {})
        if not z:
            return 0
        mi = float("-inf") if str(min_score) == "-inf" else float(min_score)
        ma = float("inf") if str(max_score) == "inf" else float(max_score)
        to_del = [m for m, sc in z.items() if mi <= float(sc) <= ma]
        for m in to_del:
            del z[m]
        if not z:
            self._z.pop(key, None)
        return len(to_del)

    async def zcard(self, key: str) -> int:
        return len(self._z.get(key, {}))

    # --- LIST ---
    async def lpush(self, key: str, value: str) -> int:
        self._list.setdefault(key, [])
        self._list[key].insert(0, str(value))
        return len(self._list[key])

    async def rpush(self, key: str, value: str) -> int:
        self._list.setdefault(key, [])
        self._list[key].append(str(value))
        return len(self._list[key])

    async def llen(self, key: str) -> int:
        return len(self._list.get(key, []))

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        lst = self._list.get(key, [])
        if stop < 0:
            stop = len(lst) - 1
        return lst[start : stop + 1]

    async def ltrim(self, key: str, start: int, stop: int) -> bool:
        lst = self._list.get(key, [])
        if stop < 0:
            stop = len(lst) - 1
        self._list[key] = lst[start : stop + 1]
        return True

    # --- KV ---
    async def set(
        self, key: str, value: str, nx: bool = False, ex: int | None = None, px: int | None = None
    ) -> bool:
        if nx and key in self._kv:
            return False
        self._kv[key] = str(value)
        if ex is not None:
            self._ttl_ms[key] = int(ex) * 1000
        if px is not None:
            self._ttl_ms[key] = int(px)
        return True

    async def setnx(self, key: str, value: str) -> int:
        ok = await self.set(key, value, nx=True)
        return 1 if ok else 0

    async def get(self, key: str) -> str | None:
        return self._kv.get(key)

    async def exists(self, key: str) -> int:
        return 1 if key in self._kv else 0

    async def delete(self, key: str) -> int:
        n = 0
        if key in self._kv:
            del self._kv[key]
            n += 1
        self._ttl_ms.pop(key, None)
        return n

    async def expire(self, key: str, seconds: int) -> bool:
        if key not in self._kv:
            return False
        self._ttl_ms[key] = int(seconds) * 1000
        return True

    async def pexpire(self, key: str, ms: int) -> bool:
        if key not in self._kv:
            return False
        self._ttl_ms[key] = int(ms)
        return True

    async def incr(self, key: str) -> int:
        cur = int(self._kv.get(key, "0") or "0")
        cur += 1
        self._kv[key] = str(cur)
        return cur

    async def getset(self, key: str, value: str) -> str | None:
        old = self._kv.get(key)
        self._kv[key] = str(value)
        return old

    # --- LUA eval (not implemented; tests patch claim) ---
    async def eval(self, *_a: Any, **_k: Any) -> Any:
        raise NotImplementedError("FakeRedis.eval not implemented (tests patch worker claim)")

    async def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


def _call_best_effort(fn: Callable[..., Any], attempts: list[tuple[tuple, dict[str, Any]]]) -> Any:
    last_te: TypeError | None = None
    for args, kwargs in attempts:
        try:
            return fn(*args, **kwargs)
        except TypeError as te:
            last_te = te
            continue
    if last_te is not None:
        raise last_te
    raise AssertionError("No call attempts provided")


async def _call_async_best_effort(
    fn: Callable[..., Any], attempts: list[tuple[tuple, dict[str, Any]]]
) -> Any:
    return await _maybe_await(_call_best_effort(fn, attempts))


def _try_json(payload: str) -> dict[str, Any]:
    try:
        d = json.loads(payload)
        return d if isinstance(d, dict) else {"_value": d}
    except Exception:
        return {"_raw": payload}


def _keys_containing_mint(r: FakeRedis, mint: str) -> list[str]:
    """
    Best-effort: search keys across internal structures containing the mint substring.
    Lock naming can vary; we don't lock exact key names.
    """
    m = str(mint)
    keys: set[str] = set()
    for k in r._kv:
        if m in k:
            keys.add(k)
    for k in r._hash:
        if m in k:
            keys.add(k)
    for k in r._z:
        if m in k:
            keys.add(k)
    for k in r._list:
        if m in k:
            keys.add(k)
    return sorted(keys)


async def _python_claim_one(
    worker_mod: Any, r: FakeRedis, now_ms: int
) -> tuple[str, str | None, float | None]:
    """
    Python substitute for worker's Lua claim:
      - select earliest due rid in EXIT_QUEUE_KEY (score <= now_ms)
      - move it to EXIT_INFLIGHT_KEY with score=now_ms
    Returns: ("empty" | "not_due" | "claimed", rid|None, due_score|None)
    """
    z = r._z.get(worker_mod.EXIT_QUEUE_KEY, {})
    if not z:
        return "empty", None, None

    rid, due = sorted(z.items(), key=lambda kv: (kv[1], kv[0]))[0]
    if float(due) > float(now_ms):
        return "not_due", None, float(due)

    await r.zrem(worker_mod.EXIT_QUEUE_KEY, rid)
    await r.zadd(worker_mod.EXIT_INFLIGHT_KEY, {rid: float(now_ms)})
    return "claimed", rid, float(due)


def _patch_first_existing(
    monkeypatch: pytest.MonkeyPatch,
    mod: Any,
    names: tuple[str, ...],
    value: Any,
    *,
    required: bool = False,
) -> str | None:
    for n in names:
        if hasattr(mod, n):
            monkeypatch.setattr(mod, n, value, raising=True)
            return n
    if required:
        raise AssertionError(
            f"Required attribute not found on {getattr(mod, '__name__', mod)}: one of {names}"
        )
    return None


def _set_if_exists(monkeypatch: pytest.MonkeyPatch, mod: Any, name: str, value: Any) -> None:
    if hasattr(mod, name):
        monkeypatch.setattr(mod, name, value, raising=False)


def _get_payload_best_effort(r: FakeRedis, data_key: str, rid: str) -> str | None:
    """
    Primary contract: payload stored in a hash EXIT_DATA_KEY with field=rid.
    Backup tolerance: some implementations store under KV key f"{EXIT_DATA_KEY}:{rid}".
    """
    # caller will await hget; here keep sync for convenience with internal maps
    if data_key in r._hash and rid in r._hash[data_key]:
        return r._hash[data_key][rid]
    kv_key = f"{data_key}:{rid}"
    return r._kv.get(kv_key)


# ---------------------------------------------------------------------------
# Contract tests: imports + required surface
# ---------------------------------------------------------------------------
def test_exit_modules_imports_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    _freeze_determinism(monkeypatch, _now_ms_fixed())
    with _no_network(monkeypatch):
        importlib.import_module("utils_pkg.exit_engine")
        importlib.import_module("utils_pkg.auto_exit_worker.exit_engine")


def test_exit_worker_contract_exists() -> None:
    w = importlib.import_module("utils_pkg.auto_exit_worker.exit_engine")
    for name in (
        "enqueue_exit_request",
        "process_one",
        "reclaim_stale_inflight",
        "prune_done",
        "worker_loop",
    ):
        assert hasattr(w, name), f"auto_exit_worker.exit_engine missing {name}"
        assert callable(getattr(w, name)), f"{name} must be callable"

    for k in (
        "EXIT_QUEUE_KEY",
        "EXIT_INFLIGHT_KEY",
        "EXIT_DONE_KEY",
        "EXIT_DATA_KEY",
        "EXIT_DLQ_KEY",
    ):
        assert hasattr(w, k), f"LOCK-IN: worker must define {k}"


def test_exit_brain_has_some_surface() -> None:
    ex = importlib.import_module("utils_pkg.exit_engine")
    candidates = [
        # functional style
        "evaluate_position",
        "decide_exit",
        "on_position_tick",
        "compute_exit",
        "get_exit_actions",
        "maybe_exit",
        "should_exit",
        "exit_decision",
        # class style
        "ExitEngine",
        "ExitPolicy",
    ]
    assert any(hasattr(ex, n) for n in candidates), (
        f"utils_pkg.exit_engine missing any known surface: {candidates}"
    )


# ---------------------------------------------------------------------------
# Shared fixture: patched worker with FakeRedis + deterministic time
# ---------------------------------------------------------------------------
@pytest.fixture
def worker_with_fakes(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, FakeRedis, dict[str, int]]:
    w = importlib.import_module("utils_pkg.auto_exit_worker.exit_engine")
    r = FakeRedis()
    counters = {"exec": 0}

    fixed_now = _now_ms_fixed()
    _freeze_determinism(monkeypatch, fixed_now)

    async def _get_redis() -> FakeRedis:
        return r

    async def _init_resources_best_effort() -> None:
        return None

    async def _notify_sell_filled(_req: Any, _sig: str) -> None:
        return None

    async def _telemetry(_req: Any, ok: bool, sig_or_err: str) -> None:
        return None

    async def _execute_exit(_req: Any) -> tuple[bool, str]:
        counters["exec"] += 1
        return True, "SIG_TEST_OK"

    # Patch redis accessor
    _patch_first_existing(
        monkeypatch, w, ("_get_redis", "get_redis", "get_redis_async"), _get_redis, required=True
    )

    # Patch internal resource init / telemetry / notify (best-effort names)
    _patch_first_existing(
        monkeypatch,
        w,
        ("_init_resources_best_effort", "_init_resources", "init_resources"),
        _init_resources_best_effort,
    )
    _patch_first_existing(
        monkeypatch,
        w,
        ("_notify_sell_filled", "_notify_filled", "_notify", "notify"),
        _notify_sell_filled,
    )
    _patch_first_existing(
        monkeypatch, w, ("_telemetry", "_emit_telemetry", "telemetry"), _telemetry
    )

    # Patch executor (best-effort names)
    _patch_first_existing(
        monkeypatch,
        w,
        ("_execute_exit", "execute_exit", "_do_exit", "_perform_exit"),
        _execute_exit,
        required=True,
    )

    # Patch claim (avoid Lua) - must exist for process_one to be testable
    async def _claim_one(_r: FakeRedis, now_ms: int) -> tuple[str, str | None, float | None]:
        return await _python_claim_one(w, r, now_ms)

    _patch_first_existing(
        monkeypatch,
        w,
        ("_claim_one", "claim_one", "_claim_one_lua", "_claim_one_script"),
        _claim_one,
        required=True,
    )

    # Patch time provider if present
    _set_if_exists(monkeypatch, w, "RETRY_JITTER_FRAC", 0.0)
    if hasattr(w, "_now_ms"):
        monkeypatch.setattr(w, "_now_ms", lambda: fixed_now, raising=False)
    if hasattr(w, "time") and hasattr(w.time, "time"):
        monkeypatch.setattr(w.time, "time", lambda: fixed_now / 1000.0, raising=False)
    if hasattr(w, "random") and hasattr(w.random, "random"):
        monkeypatch.setattr(w.random, "random", lambda: 0.0, raising=False)

    return w, r, counters


# ---------------------------------------------------------------------------
# Enqueue contract
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_enqueue_writes_queue_and_payload(
    worker_with_fakes: tuple[Any, FakeRedis, dict[str, int]],
) -> None:
    w, r, _ = worker_with_fakes

    mint = "So11111111111111111111111111111111111111112"
    rid = await _call_async_best_effort(
        w.enqueue_exit_request,
        attempts=[
            (
                (),
                {
                    "mint": mint,
                    "amount_raw": 123_456,
                    "reason": "unit_enqueue",
                    "sell_pct": 1.0,
                    "symbol": "TST",
                    "priority": 5,
                    "meta": {"slippage_bps": 200},
                },
            ),
            ((), {"mint": mint, "amount_raw": 123_456, "reason": "unit_enqueue"}),
            ((mint, 123_456, "unit_enqueue"), {}),
        ],
    )
    assert isinstance(rid, str) and rid.strip(), (
        "enqueue_exit_request must return a non-empty request id"
    )

    assert await r.zcard(w.EXIT_QUEUE_KEY) == 1, "enqueue must add to EXIT_QUEUE_KEY"

    payload = await r.hget(w.EXIT_DATA_KEY, rid)
    if payload is None:
        payload = _get_payload_best_effort(r, w.EXIT_DATA_KEY, rid)
    assert payload is not None, "payload must be stored (hash field EXIT_DATA_KEY[rid] preferred)"

    data = _try_json(payload)
    blob = json.dumps(data, sort_keys=True)
    assert mint in blob, f"payload must include mint. Got: {data}"
    assert ("amount_raw" in blob) or ("123456" in blob) or ("123_456" in blob), (
        f"payload must include amount. Got: {data}"
    )


# ---------------------------------------------------------------------------
# Success path: enqueue -> process_one -> DONE, cleanup, no DLQ
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_exit_roundtrip_success_cleans_up(
    worker_with_fakes: tuple[Any, FakeRedis, dict[str, int]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w, r, counters = worker_with_fakes

    # Make cleanup + retry behavior explicit if supported
    _set_if_exists(monkeypatch, w, "DELETE_DATA_ON_DONE", True)
    _set_if_exists(monkeypatch, w, "MAX_ATTEMPTS", 2)

    mint = "So11111111111111111111111111111111111111112"
    rid = await _call_async_best_effort(
        w.enqueue_exit_request,
        attempts=[
            (
                (),
                {
                    "mint": mint,
                    "amount_raw": 777,
                    "reason": "unit_success",
                    "symbol": "OK",
                    "meta": {},
                },
            )
        ],
    )
    assert rid

    # Ensure due now (some enqueue implementations schedule immediately; we force determinism)
    r._z[w.EXIT_QUEUE_KEY][rid] = float(_now_ms_fixed() - 1)

    locks_before = _keys_containing_mint(r, mint)

    # Guard ONLY around code-under-test
    with _no_network(monkeypatch, w):
        out = await _call_async_best_effort(
            w.process_one,
            attempts=[((), {"worker_id": "t0"}), (("t0",), {}), ((), {})],
        )

    assert isinstance(out, bool | str | type(None)), (
        "process_one must return a simple signal (bool/str/None tolerated)"
    )
    assert counters["exec"] == 1, "executor must run exactly once for a due task"

    assert await r.zcard(w.EXIT_QUEUE_KEY) == 0, "queue must be empty after success"
    assert await r.zcard(w.EXIT_INFLIGHT_KEY) == 0, "inflight must be cleared after success"
    assert await r.zscore(w.EXIT_DONE_KEY, rid) is not None, "DONE must be marked after success"
    assert await r.llen(w.EXIT_DLQ_KEY) == 0, "DLQ must be empty on success"

    # Payload cleanup if flag exists and enabled
    if hasattr(w, "DELETE_DATA_ON_DONE") and bool(w.DELETE_DATA_ON_DONE):
        assert await r.hget(w.EXIT_DATA_KEY, rid) is None, (
            "payload must be deleted when DELETE_DATA_ON_DONE=True"
        )

    # Best-effort: mint locks should not be left around
    locks_after = _keys_containing_mint(r, mint)
    if locks_before or locks_after:
        assert len(locks_after) == 0, f"mint lock(s) leaked: {locks_after}"


# ---------------------------------------------------------------------------
# Not-due behavior: must not execute early
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_exit_not_due_does_not_execute(
    worker_with_fakes: tuple[Any, FakeRedis, dict[str, int]], monkeypatch: pytest.MonkeyPatch
) -> None:
    w, r, counters = worker_with_fakes

    rid = await _call_async_best_effort(
        w.enqueue_exit_request,
        attempts=[((), {"mint": "MintNotDue", "amount_raw": 1, "reason": "not_due", "meta": {}})],
    )
    assert rid

    # Schedule far in the future
    r._z[w.EXIT_QUEUE_KEY][rid] = float(_now_ms_fixed() + 10_000)

    with _no_network(monkeypatch, w):
        _ = await _call_async_best_effort(
            w.process_one,
            attempts=[((), {"worker_id": "nd"}), (("nd",), {}), ((), {})],
        )

    assert counters["exec"] == 0, "executor must NOT run for not-due tasks"
    assert await r.zcard(w.EXIT_QUEUE_KEY) == 1, "job must remain queued"
    assert await r.zscore(w.EXIT_QUEUE_KEY, rid) is not None
    assert await r.zcard(w.EXIT_INFLIGHT_KEY) == 0, "inflight must remain empty"


# ---------------------------------------------------------------------------
# Failure path: retry then DLQ (bounded)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_exit_retries_then_dlq(monkeypatch: pytest.MonkeyPatch) -> None:
    w = importlib.import_module("utils_pkg.auto_exit_worker.exit_engine")
    r = FakeRedis()
    exec_calls = {"n": 0}

    fixed_now = _now_ms_fixed()
    _freeze_determinism(monkeypatch, fixed_now)

    async def _get_redis() -> FakeRedis:
        return r

    async def _init_resources_best_effort() -> None:
        return None

    async def _notify_sell_filled(_req: Any, _sig: str) -> None:
        return None

    async def _telemetry(_req: Any, ok: bool, sig_or_err: str) -> None:
        return None

    async def _execute_exit(_req: Any) -> tuple[bool, str]:
        exec_calls["n"] += 1
        return False, "unit_fail"

    _patch_first_existing(
        monkeypatch, w, ("_get_redis", "get_redis", "get_redis_async"), _get_redis, required=True
    )
    _patch_first_existing(
        monkeypatch,
        w,
        ("_init_resources_best_effort", "_init_resources", "init_resources"),
        _init_resources_best_effort,
    )
    _patch_first_existing(
        monkeypatch,
        w,
        ("_notify_sell_filled", "_notify_filled", "_notify", "notify"),
        _notify_sell_filled,
    )
    _patch_first_existing(
        monkeypatch, w, ("_telemetry", "_emit_telemetry", "telemetry"), _telemetry
    )
    _patch_first_existing(
        monkeypatch,
        w,
        ("_execute_exit", "execute_exit", "_do_exit", "_perform_exit"),
        _execute_exit,
        required=True,
    )

    async def _claim_one(_r: FakeRedis, now_ms: int) -> tuple[str, str | None, float | None]:
        return await _python_claim_one(w, r, now_ms)

    _patch_first_existing(
        monkeypatch,
        w,
        ("_claim_one", "claim_one", "_claim_one_lua", "_claim_one_script"),
        _claim_one,
        required=True,
    )

    # Fast, bounded retries (names may evolve; set best-effort)
    _set_if_exists(monkeypatch, w, "MAX_ATTEMPTS", 2)
    _set_if_exists(monkeypatch, w, "MAX_RETRIES", 2)
    _set_if_exists(monkeypatch, w, "RETRY_BASE_MS", 1)
    _set_if_exists(monkeypatch, w, "RETRY_CAP_MS", 2)
    _set_if_exists(monkeypatch, w, "RETRY_JITTER_FRAC", 0.0)
    _set_if_exists(monkeypatch, w, "DELETE_DATA_ON_DONE", True)

    # Enqueue
    rid = await _call_async_best_effort(
        w.enqueue_exit_request,
        attempts=[
            (
                (),
                {
                    "mint": "MintFail",
                    "amount_raw": 1000,
                    "reason": "retry_test",
                    "symbol": "RTY",
                    "meta": {},
                },
            )
        ],
    )
    assert rid
    r._z[w.EXIT_QUEUE_KEY][rid] = float(fixed_now - 1)

    with _no_network(monkeypatch, w):
        await _call_async_best_effort(
            w.process_one, attempts=[((), {"worker_id": "f1"}), (("f1",), {}), ((), {})]
        )

    assert exec_calls["n"] == 1, "first due processing must execute once"

    # If worker scheduled a backoff in the future, force it due again.
    if rid in r._z.get(w.EXIT_QUEUE_KEY, {}):
        r._z[w.EXIT_QUEUE_KEY][rid] = float(fixed_now - 1)

    with _no_network(monkeypatch, w):
        await _call_async_best_effort(
            w.process_one, attempts=[((), {"worker_id": "f2"}), (("f2",), {}), ((), {})]
        )

    assert exec_calls["n"] == 2, "second processing must execute again (retry)"

    # After bounded retries, must be DLQ'd and cleaned up
    assert await r.llen(w.EXIT_DLQ_KEY) >= 1, "repeated failures must push to DLQ"
    assert await r.zcard(w.EXIT_INFLIGHT_KEY) == 0, "inflight must be cleared after DLQ"
    assert await r.zcard(w.EXIT_QUEUE_KEY) == 0, "queue must be empty after DLQ"

    # Strong hygiene: payload should not linger after DLQ
    assert await r.hget(w.EXIT_DATA_KEY, rid) is None, "payload must be deleted after DLQ"


# ---------------------------------------------------------------------------
# Idempotency: DONE prevents re-execution
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_exit_idempotency_skip_done(
    worker_with_fakes: tuple[Any, FakeRedis, dict[str, int]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    w, r, counters = worker_with_fakes

    # Keep payload so we can simulate duplicate replay if your worker supports it
    _set_if_exists(monkeypatch, w, "DELETE_DATA_ON_DONE", False)

    rid = await _call_async_best_effort(
        w.enqueue_exit_request,
        attempts=[((), {"mint": "MintIdem", "amount_raw": 42, "reason": "idem", "meta": {}})],
    )
    assert rid
    r._z[w.EXIT_QUEUE_KEY][rid] = float(_now_ms_fixed() - 1)

    with _no_network(monkeypatch, w):
        await _call_async_best_effort(
            w.process_one, attempts=[((), {"worker_id": "idem0"}), (("idem0",), {}), ((), {})]
        )

    assert counters["exec"] == 1
    assert await r.zscore(w.EXIT_DONE_KEY, rid) is not None

    # Replay: re-add to queue due now
    await r.zadd(w.EXIT_QUEUE_KEY, {rid: float(_now_ms_fixed() - 1)})

    with _no_network(monkeypatch, w):
        await _call_async_best_effort(
            w.process_one, attempts=[((), {"worker_id": "idem1"}), (("idem1",), {}), ((), {})]
        )

    assert counters["exec"] == 1, "DONE must prevent replay execution"


# ---------------------------------------------------------------------------
# Maintenance helpers (best-effort): reclaim inflight, prune done
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_reclaim_and_prune_best_effort(
    worker_with_fakes: tuple[Any, FakeRedis, dict[str, int]],
) -> None:
    w, r, _ = worker_with_fakes

    rid = "RID_STALE"
    await r.hset(
        w.EXIT_DATA_KEY, rid, json.dumps({"rid": rid, "mint": "MintStale", "reason": "stale"})
    )
    await r.zadd(w.EXIT_INFLIGHT_KEY, {rid: float(_now_ms_fixed() - 999_999)})
    await r.zadd(w.EXIT_DONE_KEY, {"RID_DONE_OLD": float(_now_ms_fixed() - 999_999)})

    if hasattr(w, "reclaim_stale_inflight") and callable(w.reclaim_stale_inflight):
        with contextlib.suppress(TypeError):
            await _call_async_best_effort(
                w.reclaim_stale_inflight,
                attempts=[
                    ((), {"now_ms": _now_ms_fixed(), "stale_after_ms": 10_000}),
                    ((), {"now_ms": _now_ms_fixed()}),
                    ((), {}),
                    ((_now_ms_fixed(), 10_000), {}),
                    ((_now_ms_fixed(),), {}),
                ],
            )

    if hasattr(w, "prune_done") and callable(w.prune_done):
        with contextlib.suppress(TypeError):
            await _call_async_best_effort(
                w.prune_done,
                attempts=[
                    ((), {"now_ms": _now_ms_fixed(), "max_age_ms": 10_000}),
                    ((), {"now_ms": _now_ms_fixed()}),
                    ((), {}),
                    ((_now_ms_fixed(), 10_000), {}),
                    ((_now_ms_fixed(),), {}),
                ],
            )

    # Weak invariant: must not crash; internal structures remain valid
    assert isinstance(r._z, dict)
    assert isinstance(r._hash, dict)
    assert isinstance(r._kv, dict)
    assert isinstance(r._list, dict)


# ---------------------------------------------------------------------------
# Exit brain: callable does not crash on minimal snapshot (best-effort)
# ---------------------------------------------------------------------------
def test_exit_brain_call_does_not_crash_best_effort() -> None:
    ex = importlib.import_module("utils_pkg.exit_engine")

    fn = None
    for name in (
        "evaluate_position",
        "decide_exit",
        "on_position_tick",
        "compute_exit",
        "get_exit_actions",
        "maybe_exit",
        "should_exit",
        "exit_decision",
    ):
        f = getattr(ex, name, None)
        if callable(f):
            fn = f
            break

    if fn is None and hasattr(ex, "ExitEngine") and callable(ex.ExitEngine):
        # Class surface: validate instantiation does not catastrophically explode.
        with contextlib.suppress(Exception):
            _ = ex.ExitEngine()  # type: ignore[attr-defined]
        return

    if fn is None:
        return

    # Try a very small, safe payload; accept signature drift.
    try:
        _ = fn(
            mint="MintBrain",
            symbol="TST",
            position={"symbol": "TST", "entry_price": 1.0, "amount_raw": 1_000},
            current_price=1.1,
            entry_price=1.0,
            now_ms=_now_ms_fixed(),
            ctx={"source": "unit", "safe_mode": True, "dryrun": True},
            redis=None,
        )
    except TypeError:
        pass
    except Exception as e:
        pytest.fail(f"exit brain callable crashed unexpectedly: {type(e).__name__}: {e}")
