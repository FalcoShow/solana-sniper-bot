# tests/test_core_utils.py
from __future__ import annotations

"""
tests/test_core_utils.py

God-Tier Overhaul (P0) — strict, deterministic unit/contract tests for utils_pkg.core_utils.

Hard rules
----------
- NO real network calls (socket-level guard).
- NO real Redis (Redis is faked).
- NO real SQLite/filesystem DB work (aiosqlite.connect is faked).
- Safe-mode / dryrun always forced.
- Tests must be stable, fast, and Windows-friendly (pytest-asyncio teardown safe).

What we cover (core contracts)
------------------------------
- Price helpers: convert_sol_to_usd(), get_token_price()
- Entry price cache helpers: save_entry_price(), get_entry_price()
- Simple logic: dca_profit_exit()
- Telegram alert: send_telegram()
- Jupiter quote + swap "early fail" behavior: get_jupiter_quote(), swap_token_jupiter()
- Sniped cache helpers: cache_sniped_token(), was_token_sniped()
- AI fraud helper "no session safe": ai_fraud_score()
"""

import contextlib
import importlib
import inspect
import os
import socket
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytestmark = [pytest.mark.unit]


# ─────────────────────────────────────────────────────────────────────────────
# Ensure repo root is on sys.path (so pytest works anywhere)
# ─────────────────────────────────────────────────────────────────────────────
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, *_HERE.parents):
    if (_p / "utils_pkg").is_dir():
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
        break


# ─────────────────────────────────────────────────────────────────────────────
# SAFE MODE defaults (module-import time safety)
# ─────────────────────────────────────────────────────────────────────────────
# NOTE: these are defaults; fixtures also enforce.
os.environ.setdefault("DRYRUN", "1")
os.environ.setdefault("DRY_RUN", "1")
os.environ.setdefault("SAFE_MODE", "1")
os.environ.setdefault("NO_NETWORK", "1")
os.environ.setdefault("DISABLE_NETWORK", "1")
os.environ.setdefault("CI", "1")
os.environ.setdefault("CORE_LOAD_DOTENV", "0")
os.environ.setdefault("TRADING_ENABLED", "0")
os.environ.setdefault("ALLOW_BUYS", "0")
os.environ.setdefault("ALLOW_SELLS", "0")

# Common API key names (some codepaths require presence but should not be real)
os.environ.setdefault("BIRDEYE_API_KEY", "test")
os.environ.setdefault("BIRDEYE_KEY", "test")


# ─────────────────────────────────────────────────────────────────────────────
# Network guard (Windows-friendly: allow loopback ephemeral connects)
# ─────────────────────────────────────────────────────────────────────────────
class _NetworkGuardError(RuntimeError):
    pass


def _is_loopback_ephemeral(addr: Any) -> bool:
    """
    Allow ONLY Windows asyncio internals loopback connects:
      - host in {127.0.0.1, localhost, ::1}
      - port >= 49152 (Windows ephemeral range)
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
    Strictly block real network, but allow loopback ephemeral connects required by
    Windows asyncio internals. Patch scope is LIMITED to this context manager
    to avoid pytest-asyncio teardown failures.
    """
    with monkeypatch.context() as m:
        orig_create_connection = getattr(socket, "create_connection", None)

        def _guard_create_connection(*args: Any, **kwargs: Any) -> Any:
            addr = args[0] if args else kwargs.get("address")
            if _is_loopback_ephemeral(addr) and callable(orig_create_connection):
                return orig_create_connection(*args, **kwargs)
            raise _NetworkGuardError(
                "Network blocked in core_utils tests (socket.create_connection)."
            )

        m.setattr(socket, "create_connection", _guard_create_connection, raising=False)

        orig_socket_cls = socket.socket

        class _GuardSocket(orig_socket_cls):  # type: ignore[misc]
            def connect(self, *args: Any, **kwargs: Any) -> Any:
                addr = args[0] if args else None
                if _is_loopback_ephemeral(addr):
                    return super().connect(*args, **kwargs)
                raise _NetworkGuardError("Network blocked in core_utils tests (socket.connect).")

            def connect_ex(self, *args: Any, **kwargs: Any) -> Any:
                addr = args[0] if args else None
                if _is_loopback_ephemeral(addr):
                    return super().connect_ex(*args, **kwargs)
                raise _NetworkGuardError("Network blocked in core_utils tests (socket.connect_ex).")

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

                for name in ("request", "get", "post"):
                    if hasattr(requests, name):
                        m.setattr(requests, name, _guard_requests, raising=False)

                if hasattr(requests, "Session"):

                    class _GuardSession:
                        def __init__(self, *args: Any, **kwargs: Any) -> None:
                            raise _NetworkGuardError("Network blocked (requests.Session).")

                    m.setattr(requests, "Session", _GuardSession, raising=False)

        yield


# ─────────────────────────────────────────────────────────────────────────────
# Import helper (fresh/reload)
# ─────────────────────────────────────────────────────────────────────────────
def _import_cu(*, reload: bool = True) -> Any:
    name = "utils_pkg.core_utils"
    if reload and name in sys.modules:
        return importlib.reload(sys.modules[name])
    return importlib.import_module(name)


# ─────────────────────────────────────────────────────────────────────────────
# Async HTTP fakes (aiohttp-like)
# ─────────────────────────────────────────────────────────────────────────────
class FakeResponse:
    def __init__(
        self, status: int = 200, json_data: dict[str, Any] | None = None, text_data: str = "OK"
    ):
        self.status = int(status)
        self._json = json_data if json_data is not None else {}
        self._text = text_data

    async def json(self) -> dict[str, Any]:
        return self._json

    async def text(self) -> str:
        return self._text

    async def read(self) -> bytes:
        return self._text.encode("utf-8", errors="ignore")

    # Common client patterns sometimes call raise_for_status()
    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}: {self._text}")

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeSession:
    """
    Minimal aiohttp.ClientSession-like fake supporting .get() and .post()
    returning async context managers.
    """

    def __init__(self) -> None:
        self.get_calls: list[dict[str, Any]] = []
        self.post_calls: list[dict[str, Any]] = []

    def get(
        self, url: str, headers: dict[str, str] | None = None, params: dict[str, Any] | None = None
    ):
        self.get_calls.append({"url": url, "headers": headers, "params": params})
        u = url.lower()

        # CoinGecko SOL price
        if "coingecko" in u:
            return FakeResponse(200, {"solana": {"usd": 150.0}})

        # Birdeye token price (be tolerant)
        if "birdeye" in u and ("price" in u or "token_price" in u or "token" in u):
            return FakeResponse(200, {"data": {"value": 0.123456}})

        # Jupiter quote
        if ("jup.ag" in u or "quote-api" in u) and "quote" in u:
            p = params or {}
            return FakeResponse(
                200,
                {
                    "inputMint": p.get("inputMint") or p.get("inMint"),
                    "outputMint": p.get("outputMint") or p.get("outMint"),
                    "inAmount": str(p.get("amount", "")),
                    "otherAmountThreshold": "0",
                    "routePlan": [{"swapInfo": {"label": "fake"}}],
                },
            )

        return FakeResponse(404, {}, "not found")

    def post(
        self, url: str, json: dict[str, Any] | None = None, headers: dict[str, str] | None = None
    ):
        self.post_calls.append({"url": url, "json": json, "headers": headers})
        u = url.lower()

        # Telegram sendMessage
        if "api.telegram.org" in u:
            return FakeResponse(200, {"ok": True, "result": {"message_id": 1}})

        # Jupiter swap/tx endpoint (if your code hits it in DRYRUN it should still not broadcast)
        if ("jup.ag" in u or "quote-api" in u) and ("swap" in u or "transaction" in u):
            return FakeResponse(200, {"swapTransaction": "BASE64_FAKE_TXN"})

        return FakeResponse(200, {"ok": True})

    async def close(self) -> None:
        return None

    async def aclose(self) -> None:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Redis fake
# ─────────────────────────────────────────────────────────────────────────────
class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}

    async def set(self, key: str, value: Any, ex: int | None = None) -> None:
        self.store[key] = value

    async def get(self, key: str) -> Any:
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: Any) -> None:
        self.store[key] = value

    async def exists(self, key: str) -> int:
        return 1 if key in self.store else 0

    async def lpush(self, key: str, value: Any) -> None:
        self.store.setdefault(key, [])
        self.store[key].insert(0, value)

    async def ltrim(self, key: str, start: int, end: int) -> None:
        if key in self.store and isinstance(self.store[key], list):
            self.store[key] = self.store[key][start : end + 1]

    async def close(self) -> None:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# aiosqlite fake (supports await + async with + execute/fetchone patterns)
# ─────────────────────────────────────────────────────────────────────────────
class _FakeCursor:
    def __init__(self, db: _FakeSqliteDB, last_select_mint: str | None):
        self._db = db
        self._last_select_mint = last_select_mint

    async def fetchone(self) -> Any:
        if not self._last_select_mint:
            return None
        # Return aiosqlite-like row (tuple) if present
        if self._last_select_mint in self._db.tokens:
            # (mint, price)
            return (self._last_select_mint, self._db.tokens[self._last_select_mint])
        return None

    async def fetchall(self) -> list[Any]:
        return []

    async def __aenter__(self) -> _FakeCursor:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeSqliteDB:
    def __init__(self) -> None:
        self.tokens: dict[str, float] = {}

    async def execute(self, sql: str, params: Any = None) -> _FakeCursor:
        s = (sql or "").lower()
        p = params or ()
        # Very tolerant parsing: try to find a mint string in params
        mint = None
        price = None

        if isinstance(p, list | tuple) and p:
            # common: (mint,) or (mint, price)
            mint = p[0] if isinstance(p[0], str) else None
            if len(p) >= 2 and isinstance(p[1], int | float):
                price = float(p[1])

        if "insert" in s or "replace" in s or "upsert" in s:
            if mint:
                self.tokens[mint] = (
                    float(price) if price is not None else self.tokens.get(mint, 0.0)
                )
            return _FakeCursor(self, None)

        if "select" in s:
            return _FakeCursor(self, mint)

        return _FakeCursor(self, None)

    async def commit(self) -> None:
        return None

    async def close(self) -> None:
        return None


class _FakeAioSqliteConn:
    """
    Behaves like aiosqlite.connect(...) return value:
    - awaitable (optional)
    - async context manager
    - has execute/commit/close
    """

    def __init__(self, db: _FakeSqliteDB):
        self._db = db

    def __await__(self):
        async def _coro():
            return self

        return _coro().__await__()

    async def __aenter__(self) -> _FakeSqliteDB:
        return self._db

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


def _make_fake_aiosqlite_connect(db: _FakeSqliteDB):
    def _connect(*args: Any, **kwargs: Any) -> _FakeAioSqliteConn:
        return _FakeAioSqliteConn(db)

    return _connect


# ─────────────────────────────────────────────────────────────────────────────
# Signature-aware call helper (keeps tests stable across refactors)
# ─────────────────────────────────────────────────────────────────────────────
async def _call_maybe_async(fn: Callable[..., Any], **kwargs: Any) -> Any:
    out = fn(**kwargs)
    if inspect.isawaitable(out):
        out = await out
    return out


def _kwargs_for_sig(fn: Callable[..., Any], pool: dict[str, Any]) -> dict[str, Any]:
    sig = inspect.signature(fn)
    kwargs = {k: v for k, v in pool.items() if k in sig.parameters}

    # If function supports **kwargs, pass a compact but helpful subset
    if not kwargs and any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
        for k in (
            "session",
            "http",
            "redis",
            "r",
            "mint",
            "token_mint",
            "amount",
            "amount_raw",
            "slippage",
            "slippage_bps",
        ):
            if k in pool:
                kwargs[k] = pool[k]

    # Fill required params best-effort
    for p in sig.parameters.values():
        if p.default is not inspect._empty:
            continue
        if p.name in kwargs:
            continue
        pn = p.name.lower()
        if "session" in pn or pn in ("http", "client"):
            kwargs[p.name] = pool.get("session")
        elif pn in ("redis", "r"):
            kwargs[p.name] = pool.get("redis")
        elif "mint" in pn:
            kwargs[p.name] = pool.get("mint") or pool.get("token_mint") or "AnyMint"
        elif "amount" in pn or "lamport" in pn:
            kwargs[p.name] = pool.get("amount") or pool.get("amount_raw") or 1
        elif "slippage" in pn:
            kwargs[p.name] = pool.get("slippage") or pool.get("slippage_bps") or 50
        else:
            kwargs[p.name] = None

    return kwargs


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures: safe env + isolate globals + provide fakes
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture(autouse=True)
def _safe_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("SAFE_MODE", "1")
    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("CORE_LOAD_DOTENV", "0")
    monkeypatch.setenv("TRADING_ENABLED", "0")
    monkeypatch.setenv("ALLOW_BUYS", "0")
    monkeypatch.setenv("ALLOW_SELLS", "0")

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test_token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "12345")

    monkeypatch.setenv("BIRDEYE_API_KEY", "test")
    monkeypatch.setenv("BIRDEYE_KEY", "test")


@pytest.fixture
def core_utils(monkeypatch: pytest.MonkeyPatch) -> Any:
    cu = _import_cu(reload=True)

    # Keep module-level globals isolated per test (no cross-test bleed)
    for name in ("session", "_session", "redis", "_redis", "http_session", "_http_session"):
        if hasattr(cu, name):
            monkeypatch.setattr(cu, name, None, raising=False)

    return cu


@pytest.fixture
def fake_session(core_utils: Any, monkeypatch: pytest.MonkeyPatch) -> FakeSession:
    s = FakeSession()

    # Patch the common "cached session" globals & accessors
    for name in ("session", "_session", "http_session", "_http_session"):
        if hasattr(core_utils, name):
            monkeypatch.setattr(core_utils, name, s, raising=False)

    for getter in ("get_session", "get_http_session", "get_aiohttp_session"):
        if hasattr(core_utils, getter):
            monkeypatch.setattr(core_utils, getter, lambda: s, raising=False)

    # If core_utils imported aiohttp and constructs ClientSession directly, redirect it.
    aiohttp = getattr(core_utils, "aiohttp", None)
    if aiohttp is not None and hasattr(aiohttp, "ClientSession"):
        monkeypatch.setattr(aiohttp, "ClientSession", lambda *a, **k: s, raising=False)

    return s


@pytest.fixture
def fake_redis(core_utils: Any, monkeypatch: pytest.MonkeyPatch) -> FakeRedis:
    r = FakeRedis()
    for name in ("redis", "_redis"):
        if hasattr(core_utils, name):
            monkeypatch.setattr(core_utils, name, r, raising=False)

    if hasattr(core_utils, "get_redis_async"):

        async def _get_redis_async():
            return r

        monkeypatch.setattr(core_utils, "get_redis_async", _get_redis_async, raising=False)

    return r


@pytest.fixture
def fake_aiosqlite(core_utils: Any, monkeypatch: pytest.MonkeyPatch) -> _FakeSqliteDB:
    db = _FakeSqliteDB()
    aiosqlite = getattr(core_utils, "aiosqlite", None)
    if aiosqlite is not None and hasattr(aiosqlite, "connect"):
        monkeypatch.setattr(aiosqlite, "connect", _make_fake_aiosqlite_connect(db), raising=True)
    return db


# ─────────────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────────────
def test_core_utils_import_is_side_effect_free(monkeypatch: pytest.MonkeyPatch) -> None:
    # Import under hard no-network guard (catches accidental top-level I/O)
    with _no_network(monkeypatch):
        cu = _import_cu(reload=True)
    assert cu is not None


@pytest.mark.asyncio
async def test_convert_sol_to_usd_uses_fake_coingecko(
    core_utils: Any, fake_session: FakeSession, monkeypatch: pytest.MonkeyPatch
):
    assert hasattr(core_utils, "convert_sol_to_usd"), "core_utils.convert_sol_to_usd missing"
    fn = core_utils.convert_sol_to_usd

    pool = {"amount_sol": 2.0, "sol": 2.0, "amount": 2.0, "session": fake_session}
    kwargs = _kwargs_for_sig(fn, pool)

    with _no_network(monkeypatch, core_utils):
        out = await _call_maybe_async(fn, **kwargs)

    assert isinstance(out, int | float)
    assert float(out) == pytest.approx(300.0, rel=1e-9)
    assert any("coingecko" in c["url"].lower() for c in fake_session.get_calls)


@pytest.mark.asyncio
async def test_get_token_price_birdeye(
    core_utils: Any, fake_session: FakeSession, monkeypatch: pytest.MonkeyPatch
):
    assert hasattr(core_utils, "get_token_price"), "core_utils.get_token_price missing"
    fn = core_utils.get_token_price

    pool = {
        "mint": "SomeMint1111111111111111111111111111111",
        "token_mint": "SomeMint1111111111111111111111111111111",
        "session": fake_session,
    }
    kwargs = _kwargs_for_sig(fn, pool)

    with _no_network(monkeypatch, core_utils):
        price = await _call_maybe_async(fn, **kwargs)

    # Must be numeric and match fake
    assert isinstance(price, int | float)
    assert float(price) == pytest.approx(0.123456, rel=1e-9)


@pytest.mark.asyncio
async def test_get_token_price_no_session_returns_zero(
    core_utils: Any, monkeypatch: pytest.MonkeyPatch
):
    assert hasattr(core_utils, "get_token_price"), "core_utils.get_token_price missing"
    fn = core_utils.get_token_price

    # Force no session (should return 0.0, not crash)
    for name in ("session", "_session", "http_session", "_http_session"):
        if hasattr(core_utils, name):
            monkeypatch.setattr(core_utils, name, None, raising=False)

    pool = {"mint": "AnyMint", "token_mint": "AnyMint", "session": None}
    kwargs = _kwargs_for_sig(fn, pool)

    with _no_network(monkeypatch, core_utils):
        price = await _call_maybe_async(fn, **kwargs)

    assert price in (0.0, 0, None), f"Expected safe zero/None, got: {price!r}"
    if price is None:
        return
    assert float(price) == 0.0


def test_dca_profit_exit_threshold(core_utils: Any):
    assert hasattr(core_utils, "dca_profit_exit"), "core_utils.dca_profit_exit missing"
    fn = core_utils.dca_profit_exit

    # Keep simple and strict (pure math helper)
    assert fn(current=2.0, entry=1.0, target=2.0) is True
    assert fn(current=1.99, entry=1.0, target=2.0) is False


@pytest.mark.asyncio
async def test_save_and_get_entry_price(
    core_utils: Any, fake_redis: FakeRedis, monkeypatch: pytest.MonkeyPatch
):
    assert hasattr(core_utils, "save_entry_price"), "core_utils.save_entry_price missing"
    assert hasattr(core_utils, "get_entry_price"), "core_utils.get_entry_price missing"

    save_fn = core_utils.save_entry_price
    get_fn = core_utils.get_entry_price

    # Be tolerant to signature changes (some versions accept redis explicitly, some use global)
    pool_save = {
        "redis": fake_redis,
        "r": fake_redis,
        "mint": "MintX",
        "token_mint": "MintX",
        "price": 0.42,
        "entry_price": 0.42,
    }
    pool_get = {"redis": fake_redis, "r": fake_redis, "mint": "MintX", "token_mint": "MintX"}

    with _no_network(monkeypatch, core_utils):
        await _call_maybe_async(save_fn, **_kwargs_for_sig(save_fn, pool_save))
        val = await _call_maybe_async(get_fn, **_kwargs_for_sig(get_fn, pool_get))

    assert val is not None
    assert float(val) == pytest.approx(0.42, rel=1e-12)


@pytest.mark.asyncio
async def test_send_telegram_posts_once(
    core_utils: Any, fake_session: FakeSession, monkeypatch: pytest.MonkeyPatch
):
    assert hasattr(core_utils, "send_telegram"), "core_utils.send_telegram missing"
    fn = core_utils.send_telegram

    # Some implementations read module globals for token/chat; keep both env + attrs covered
    for attr, val in (("TELEGRAM_BOT_TOKEN", "bot:token"), ("TELEGRAM_CHAT_ID", "12345")):
        if hasattr(core_utils, attr):
            monkeypatch.setattr(core_utils, attr, val, raising=False)

    pool = {
        "message": "Hello",
        "text": "Hello",
        "msg": "Hello",
        "sol_value": 1.0,
        "session": fake_session,
    }
    kwargs = _kwargs_for_sig(fn, pool)

    with _no_network(monkeypatch, core_utils):
        _ = await _call_maybe_async(fn, **kwargs)

    assert len(fake_session.post_calls) == 1
    assert "api.telegram.org" in fake_session.post_calls[0]["url"].lower()


@pytest.mark.asyncio
async def test_get_jupiter_quote_returns_none_on_404(
    core_utils: Any, fake_session: FakeSession, monkeypatch: pytest.MonkeyPatch
):
    assert hasattr(core_utils, "get_jupiter_quote"), "core_utils.get_jupiter_quote missing"
    fn = core_utils.get_jupiter_quote

    # Force a non-quote endpoint so FakeSession returns 404
    for name in ("JUPITER_QUOTE_API", "JUPITER_QUOTE_URL", "JUPITER_SWAP_API", "JUPITER_API"):
        if hasattr(core_utils, name):
            monkeypatch.setattr(core_utils, name, "https://quote-api.jup.ag/v6/nope", raising=False)

    pool = {
        "input_mint": "So11111111111111111111111111111111111111112",
        "in_mint": "So11111111111111111111111111111111111111112",
        "output_mint": "SomeMint",
        "out_mint": "SomeMint",
        "amount": 1_000_000,
        "amount_raw": 1_000_000,
        "slippage_pct": 0.005,
        "slippage": 0.005,
        "slippage_bps": 50,
        "session": fake_session,
    }
    kwargs = _kwargs_for_sig(fn, pool)

    with _no_network(monkeypatch, core_utils):
        q = await _call_maybe_async(fn, **kwargs)

    assert q is None or q == {}, f"Expected None/empty on 404, got: {q!r}"


@pytest.mark.asyncio
async def test_get_jupiter_quote_happy_path(
    core_utils: Any, fake_session: FakeSession, monkeypatch: pytest.MonkeyPatch
):
    assert hasattr(core_utils, "get_jupiter_quote"), "core_utils.get_jupiter_quote missing"
    fn = core_utils.get_jupiter_quote

    # Ensure URL contains 'quote' so FakeSession returns a quote payload
    for name in ("JUPITER_QUOTE_API", "JUPITER_QUOTE_URL", "JUPITER_SWAP_API", "JUPITER_API"):
        if hasattr(core_utils, name):
            monkeypatch.setattr(
                core_utils, name, "https://quote-api.jup.ag/v6/quote", raising=False
            )

    pool = {
        "input_mint": "So11111111111111111111111111111111111111112",
        "in_mint": "So11111111111111111111111111111111111111112",
        "output_mint": "SomeMint",
        "out_mint": "SomeMint",
        "amount": 123,
        "amount_raw": 123,
        "slippage_pct": 0.005,
        "slippage": 0.005,
        "slippage_bps": 50,
        "session": fake_session,
    }
    kwargs = _kwargs_for_sig(fn, pool)

    with _no_network(monkeypatch, core_utils):
        q = await _call_maybe_async(fn, **kwargs)

    assert isinstance(q, dict)
    assert ("routePlan" in q) or ("data" in q) or ("routes" in q), (
        f"Unexpected quote payload: {q!r}"
    )


@pytest.mark.asyncio
async def test_swap_token_jupiter_early_fail_no_quote(
    core_utils: Any, fake_session: FakeSession, monkeypatch: pytest.MonkeyPatch
):
    assert hasattr(core_utils, "swap_token_jupiter"), "core_utils.swap_token_jupiter missing"
    assert hasattr(core_utils, "get_jupiter_quote"), "core_utils.get_jupiter_quote missing"

    # Force quote failure
    for name in ("JUPITER_QUOTE_API", "JUPITER_QUOTE_URL", "JUPITER_SWAP_API", "JUPITER_API"):
        if hasattr(core_utils, name):
            monkeypatch.setattr(core_utils, name, "https://quote-api.jup.ag/v6/nope", raising=False)

    fn = core_utils.swap_token_jupiter

    pool = {
        "input_mint": "So11111111111111111111111111111111111111112",
        "in_mint": "So11111111111111111111111111111111111111112",
        "output_mint": "SomeMint",
        "out_mint": "SomeMint",
        "amount_float": 1.0,
        "amount": 1.0,
        "buy": True,
        "side": "buy",
        "slippage": 0.005,
        "slippage_pct": 0.005,
        "session": fake_session,
        "dryrun": True,
        "safe_mode": True,
    }
    kwargs = _kwargs_for_sig(fn, pool)

    with _no_network(monkeypatch, core_utils):
        out = await _call_maybe_async(fn, **kwargs)

    # Support multiple return shapes
    if isinstance(out, tuple) and len(out) >= 2:
        ok, sig = out[0], out[1]
        assert ok is False
        assert sig in ("", None)
        return

    if isinstance(out, dict):
        ok = out.get("ok")
        sig = out.get("sig") or out.get("tx_sig") or out.get("signature")
        assert ok is False or ok is None
        assert sig in ("", None)
        return

    # Last resort: a single bool
    if isinstance(out, bool):
        assert out is False
        return

    raise AssertionError(
        f"Unexpected swap_token_jupiter return type: {type(out).__name__}: {out!r}"
    )


@pytest.mark.asyncio
async def test_cache_and_check_sniped_token_no_real_sqlite(
    core_utils: Any,
    fake_redis: FakeRedis,
    fake_aiosqlite: _FakeSqliteDB,
    monkeypatch: pytest.MonkeyPatch,
):
    assert hasattr(core_utils, "cache_sniped_token"), "core_utils.cache_sniped_token missing"
    assert hasattr(core_utils, "was_token_sniped"), "core_utils.was_token_sniped missing"

    cache_fn = core_utils.cache_sniped_token
    was_fn = core_utils.was_token_sniped

    # Ensure module redis points to our fake (some implementations use module global)
    for name in ("redis", "_redis"):
        if hasattr(core_utils, name):
            monkeypatch.setattr(core_utils, name, fake_redis, raising=False)

    with _no_network(monkeypatch, core_utils):
        # cache_sniped_token signatures vary a lot; be flexible
        await _call_maybe_async(
            cache_fn,
            **_kwargs_for_sig(
                cache_fn,
                {
                    "mint": "MintY",
                    "token_mint": "MintY",
                    "price": 0.12,
                    "entry_price": 0.12,
                    "redis": fake_redis,
                },
            ),
        )
        ok_y = await _call_maybe_async(
            was_fn,
            **_kwargs_for_sig(
                was_fn, {"mint": "MintY", "token_mint": "MintY", "redis": fake_redis}
            ),
        )
        ok_o = await _call_maybe_async(
            was_fn,
            **_kwargs_for_sig(
                was_fn, {"mint": "Other", "token_mint": "Other", "redis": fake_redis}
            ),
        )

    assert bool(ok_y) is True
    assert bool(ok_o) is False


@pytest.mark.asyncio
async def test_ai_fraud_score_no_session_safe(core_utils: Any, monkeypatch: pytest.MonkeyPatch):
    assert hasattr(core_utils, "ai_fraud_score"), "core_utils.ai_fraud_score missing"
    fn = core_utils.ai_fraud_score

    # Force no session
    for name in ("session", "_session", "http_session", "_http_session"):
        if hasattr(core_utils, name):
            monkeypatch.setattr(core_utils, name, None, raising=False)

    with _no_network(monkeypatch, core_utils):
        out = await _call_maybe_async(
            fn, **_kwargs_for_sig(fn, {"mint": "AnyMint", "token_mint": "AnyMint", "session": None})
        )

    # Must be safe: 0 / 0.0 / None acceptable, but must not crash.
    if out is None:
        return
    assert isinstance(out, int | float)
    assert float(out) == 0.0
