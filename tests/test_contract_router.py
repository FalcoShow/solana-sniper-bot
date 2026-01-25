# tests/test_contract_router.py
from __future__ import annotations

"""
Contract tests: Router (P0 LOCK-IN)
==================================

Goals
-----
✅ Import-safe (no secrets required, no network at import time)
✅ No network I/O during tests (hard guards)
✅ Windows-friendly (pytest-asyncio teardown must not break)
✅ Exposes async buy/sell quote helpers
✅ DRYRUN/SAFE_MODE quotes do not hit network and do not crash
✅ Optional: deterministic request-builder if exposed

These tests validate safety + interface/shape only (not quote quality).
"""

import asyncio
import contextlib
import importlib
import inspect
import json
import os
import socket
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.contract


# -----------------------------------------------------------------------------
# Ensure repo root (folder that contains `utils_pkg`) is on sys.path
# -----------------------------------------------------------------------------
_HERE = Path(__file__).resolve().parent
for _p in (_HERE, *_HERE.parents):
    if (_p / "utils_pkg").is_dir():
        if str(_p) not in sys.path:
            sys.path.insert(0, str(_p))
        break


# -----------------------------------------------------------------------------
# Stable constants (no network dependency)
# -----------------------------------------------------------------------------
SOL_MINT = "So11111111111111111111111111111111111111112"  # Wrapped SOL mint
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # USDC mint


# -----------------------------------------------------------------------------
# Safe-mode + network guard helpers
# -----------------------------------------------------------------------------
class NetworkBlockedError(RuntimeError):
    """Raised when contract tests detect network I/O."""


@pytest.fixture(autouse=True)
def _safe_mode_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Force router into NO-NETWORK / DRYRUN mode.
    Set multiple toggles so whichever your code recognizes will work.
    """
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("PYTEST_MODE", "1")
    monkeypatch.setenv("BACKTEST_MODE", "1")
    monkeypatch.setenv("SANDBOX_MODE", os.getenv("SANDBOX_MODE", "1"))

    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("SAFE_MODE", "1")
    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")
    monkeypatch.setenv("DISABLE_RPC", "1")
    monkeypatch.setenv("DISABLE_HTTP", "1")

    # Prevent dotenv/autoload side effects (if supported)
    monkeypatch.setenv("CORE_LOAD_DOTENV", "0")

    # Keep contracts import-safe (tests should never hard-fail on missing secrets)
    monkeypatch.setenv("ENV_STRICT", os.getenv("ENV_STRICT", "0"))
    monkeypatch.setenv("STRICT_ENV", os.getenv("STRICT_ENV", "0"))
    monkeypatch.setenv("CORE_UTILS_ENV_STRICT", os.getenv("CORE_UTILS_ENV_STRICT", "0"))

    # Quiet logs in unit tests
    monkeypatch.setenv("LOG_LEVEL", os.getenv("LOG_LEVEL", "WARNING"))


def _is_win() -> bool:
    return (os.name == "nt") or sys.platform.startswith("win")


def _ephemeral_low_port() -> int:
    """
    Lower bound for ephemeral ports.
    - Windows default ephemeral range starts at 49152.
    - Linux typically 32768+ (read kernel if possible).
    """
    if _is_win():
        return 49152
    try:
        p = Path("/proc/sys/net/ipv4/ip_local_port_range")
        if p.exists():
            raw = p.read_text(encoding="utf-8", errors="ignore").strip().split()
            if len(raw) >= 2:
                lo = int(raw[0])
                if 1024 <= lo <= 65000:
                    return lo
    except Exception:
        pass
    return 32768


_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def _norm_host(host: Any) -> str:
    if host is None:
        return ""
    if isinstance(host, bytes):
        try:
            host = host.decode("utf-8", "ignore")
        except Exception:
            host = str(host)
    return str(host).strip().strip("[]").lower()


def _extract_host_port(sockaddr: Any) -> tuple[str | None, int | None]:
    """
    INET/INET6 sockaddrs:
      - ('host', port)
      - ('host', port, flowinfo, scopeid)
    """
    if isinstance(sockaddr, tuple) and len(sockaddr) >= 2:
        host = sockaddr[0]
        port = sockaddr[1]
        return (str(host), int(port) if isinstance(port, int) else None)
    return (None, None)


def _allow_loopback_ephemeral_only(sockaddr: Any) -> bool:
    """
    Additive guard on top of tests/conftest.py:
      - Allow loopback only
      - AND only if port is ephemeral (or 0)

    Prevents accidental connects to local services (redis/postgres/RPC).
    """
    host, port = _extract_host_port(sockaddr)
    if host is None or port is None:
        return False
    h = _norm_host(host)
    if h not in _LOOPBACK_HOSTS:
        return False
    if port == 0:
        return True
    return port >= _ephemeral_low_port()


def _raise_blocked(where: str, host: Any, port: Any, why: str = "") -> None:
    h = _norm_host(host) or "<unknown>"
    p = port if port is not None else "<unknown>"
    msg = (
        f"Network is blocked in router contract tests.\nBlocked at: {where}\nDestination: {h}:{p}\n"
    )
    if why:
        msg += f"Reason: {why}\n"
    msg += (
        "Allowed by this file's guard: loopback + ephemeral ports only.\n"
        "If you intended to use real network/local services, this must NOT be a contract test.\n"
    )
    raise NetworkBlockedError(msg)


class _DummyHTTP:
    """
    Placeholder for required session/client params.
    If router incorrectly tries to use this in DRYRUN, we hard-fail.
    """

    def request(self, *args: Any, **kwargs: Any) -> Any:
        raise NetworkBlockedError("Network blocked (dummy http.request called).")

    def get(self, *args: Any, **kwargs: Any) -> Any:
        raise NetworkBlockedError("Network blocked (dummy http.get called).")

    def post(self, *args: Any, **kwargs: Any) -> Any:
        raise NetworkBlockedError("Network blocked (dummy http.post called).")

    async def aclose(self) -> None:
        return

    async def close(self) -> None:
        return


@contextlib.contextmanager
def _no_network_local_services(monkeypatch: pytest.MonkeyPatch, *mods: Any):
    """
    EXTRA guard on top of tests/conftest.py.

    conftest.py usually blocks non-loopback; this further blocks loopback
    connections to non-ephemeral ports, preventing accidental local services.

    Scoped via monkeypatch.context() so pytest-asyncio teardown stays healthy.
    """
    with monkeypatch.context() as m:
        orig_create_connection = getattr(socket, "create_connection", None)
        orig_connect = socket.socket.connect
        orig_connect_ex = socket.socket.connect_ex

        def create_connection(address: Any, *args: Any, **kwargs: Any) -> Any:
            # AF_UNIX path string: allow (local-only)
            if isinstance(address, str | bytes):
                if callable(orig_create_connection):
                    return orig_create_connection(address, *args, **kwargs)
                raise NetworkBlockedError(
                    "socket.create_connection unavailable to call for AF_UNIX path."
                )
            host, port = _extract_host_port(address)
            if host is not None and port is not None:
                if _allow_loopback_ephemeral_only(address):
                    assert callable(orig_create_connection)
                    return orig_create_connection(address, *args, **kwargs)
                _raise_blocked("socket.create_connection", host, port, why="loopback non-ephemeral")
            raise NetworkBlockedError(
                "Network blocked: socket.create_connection unsupported address type."
            )

        def connect(self: socket.socket, address: Any) -> Any:
            if isinstance(address, str | bytes):
                return orig_connect(self, address)
            host, port = _extract_host_port(address)
            if host is not None and port is not None:
                if _allow_loopback_ephemeral_only(address):
                    return orig_connect(self, address)
                _raise_blocked("socket.socket.connect", host, port, why="loopback non-ephemeral")
            raise NetworkBlockedError("Network blocked: socket.connect unsupported address type.")

        def connect_ex(self: socket.socket, address: Any) -> Any:
            if isinstance(address, str | bytes):
                return orig_connect_ex(self, address)
            host, port = _extract_host_port(address)
            if host is not None and port is not None:
                if _allow_loopback_ephemeral_only(address):
                    return orig_connect_ex(self, address)
                _raise_blocked("socket.socket.connect_ex", host, port, why="loopback non-ephemeral")
            raise NetworkBlockedError(
                "Network blocked: socket.connect_ex unsupported address type."
            )

        if callable(orig_create_connection):
            m.setattr(socket, "create_connection", create_connection, raising=True)
        m.setattr(socket.socket, "connect", connect, raising=True)
        m.setattr(socket.socket, "connect_ex", connect_ex, raising=True)

        # If router module namespace exposes HTTP clients, hard-block creating sessions/clients.
        for mod in mods:
            aiohttp = getattr(mod, "aiohttp", None)
            if aiohttp is not None:

                class _GuardAiohttpSession:
                    def __init__(self, *args: Any, **kwargs: Any) -> None:
                        raise NetworkBlockedError("Network blocked (aiohttp.ClientSession).")

                m.setattr(aiohttp, "ClientSession", _GuardAiohttpSession, raising=False)

            httpx = getattr(mod, "httpx", None)
            if httpx is not None:

                class _GuardHttpxAsyncClient:
                    def __init__(self, *args: Any, **kwargs: Any) -> None:
                        raise NetworkBlockedError("Network blocked (httpx.AsyncClient).")

                m.setattr(httpx, "AsyncClient", _GuardHttpxAsyncClient, raising=False)

            requests = getattr(mod, "requests", None)
            if requests is not None:

                def _guard_requests(*args: Any, **kwargs: Any) -> Any:
                    raise NetworkBlockedError("Network blocked (requests).")

                for nm in ("request", "get", "post"):
                    if hasattr(requests, nm):
                        m.setattr(requests, nm, _guard_requests, raising=False)

        yield


# -----------------------------------------------------------------------------
# Import helpers
# -----------------------------------------------------------------------------
def _fresh_import(module_name: str) -> Any:
    for name in list(sys.modules.keys()):
        if name == module_name or name.startswith(module_name + "."):
            sys.modules.pop(name, None)
    return importlib.import_module(module_name)


def _import_router(*, fresh: bool = True) -> Any:
    return (
        _fresh_import("utils_pkg.router") if fresh else importlib.import_module("utils_pkg.router")
    )


def _is_async_callable(fn: Any) -> bool:
    return callable(fn) and asyncio.iscoroutinefunction(fn)


# -----------------------------------------------------------------------------
# Optional deterministic request builder discovery
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class BuildResult:
    url: str | None
    params: dict[str, Any]


def _normalize_build_output(out: Any) -> BuildResult:
    """
    Normalize builder outputs:
      - (url, params_dict)
      - {"url": "...", "params": {...}} or {"endpoint": "...", "query": {...}}
      - dict params
    """
    if isinstance(out, tuple) and len(out) == 2:
        url, params = out
        if isinstance(params, dict):
            return BuildResult(url=str(url) if url is not None else None, params=dict(params))

    if isinstance(out, dict):
        if "params" in out and isinstance(out["params"], dict):
            url = out.get("url") or out.get("endpoint")
            return BuildResult(url=str(url) if url else None, params=dict(out["params"]))
        if "query" in out and isinstance(out["query"], dict):
            url = out.get("url") or out.get("endpoint")
            return BuildResult(url=str(url) if url else None, params=dict(out["query"]))
        return BuildResult(url=None, params=dict(out))

    raise AssertionError(f"Unrecognized request builder output: {type(out).__name__}: {out!r}")


def _find_callable(mod: Any, names: tuple[str, ...]) -> Callable[..., Any] | None:
    for n in names:
        fn = getattr(mod, n, None)
        if callable(fn):
            return fn
    return None


def _discover_request_builder(router_mod: Any) -> Callable[..., Any] | None:
    candidates = (
        "build_quote_request",
        "build_quote_params",
        "build_quote_query",
        "make_quote_params",
        "make_quote_request",
        "_build_quote_request",
        "_build_quote_params",
        "_build_quote_query",
        "_make_quote_params",
        "_make_quote_request",
        "build_jupiter_quote_params",
        "_build_jupiter_quote_params",
    )
    return _find_callable(router_mod, candidates)


def _run_awaitable_sync(x: Any) -> Any:
    """
    Run an awaitable from sync code without relying on ambient loops.
    Uses a fresh event loop (pytest-asyncio safe).
    """
    if not inspect.isawaitable(x):
        return x
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(x)
    finally:
        with contextlib.suppress(Exception):
            loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


# -----------------------------------------------------------------------------
# Signature-aware call helpers
# -----------------------------------------------------------------------------
def _fill_required_kwargs(sig: inspect.Signature, *, direction: str) -> dict[str, Any]:
    """
    Build kwargs that satisfy required router params across likely signatures.

    direction:
      - "buy": buy token with SOL  (SOL -> token)
      - "sell": sell token for SOL (token -> SOL)
    """
    token_mint = USDC_MINT
    in_mint = SOL_MINT if direction == "buy" else token_mint
    out_mint = token_mint if direction == "buy" else SOL_MINT

    # Lamports for "amount raw" paths; also provide float SOL inputs as fallback.
    amount_raw = 1_000_000
    sol_in = 0.001

    pool: dict[str, Any] = {
        # mint-ish
        "mint": token_mint,
        "token_mint": token_mint,
        "base_mint": token_mint,
        "target_mint": token_mint,
        "in_mint": in_mint,
        "input_mint": in_mint,
        "out_mint": out_mint,
        "output_mint": out_mint,
        # amount-ish
        "amount": amount_raw,
        "amt": amount_raw,
        "lamports": amount_raw,
        "amount_raw": amount_raw,
        "in_amount_raw": amount_raw,
        "size_raw": amount_raw,
        "sol_in": sol_in,
        "sol": sol_in,
        "amount_sol": sol_in,
        # slippage-ish
        "slippage_bps": 50,
        "slippage": 50,
        # controls / safety
        "dryrun": True,
        "dry_run": True,
        "safe_mode": True,
        "no_network": True,
        "disable_network": True,
        "timeout_s": 5.0,
        "timeout": 5.0,
        "max_retries": 0,
        "retries": 0,
        "retry": 0,
        # required client/session placeholders
        "session": _DummyHTTP(),
        "client": _DummyHTTP(),
        "http": _DummyHTTP(),
        "logger": None,
    }

    kwargs: dict[str, Any] = {}

    for p in sig.parameters.values():
        if p.default is not inspect._empty:
            continue

        pn = p.name.lower()

        if p.name in pool:
            kwargs[p.name] = pool[p.name]
            continue

        if "in_mint" in pn or "input_mint" in pn:
            kwargs[p.name] = in_mint
            continue
        if "out_mint" in pn or "output_mint" in pn:
            kwargs[p.name] = out_mint
            continue
        if "mint" in pn:
            kwargs[p.name] = token_mint
            continue

        if (
            "lamport" in pn
            or "amount_raw" in pn
            or (("amount" in pn or "amt" in pn) and "sol" not in pn)
        ):
            kwargs[p.name] = amount_raw
            continue
        if "sol" in pn and ("amount" in pn or "in" in pn or pn in ("sol", "sol_in", "amount_sol")):
            kwargs[p.name] = sol_in
            continue

        if "slippage" in pn:
            kwargs[p.name] = 50
            continue
        if "timeout" in pn:
            kwargs[p.name] = 5.0
            continue
        if "retry" in pn:
            kwargs[p.name] = 0
            continue
        if "session" in pn or "client" in pn or "http" in pn:
            kwargs[p.name] = _DummyHTTP()
            continue

        # Last resort: satisfy required params with None (contract is "no crash in dryrun")
        kwargs[p.name] = None

    # Also pass common optional toggles if accepted (even if not required)
    for k, v in (
        ("dryrun", True),
        ("dry_run", True),
        ("safe_mode", True),
        ("no_network", True),
        ("disable_network", True),
        ("slippage_bps", 50),
        ("timeout_s", 5.0),
        ("timeout", 5.0),
        ("max_retries", 0),
    ):
        if k in sig.parameters and k not in kwargs:
            kwargs[k] = v

    return kwargs


def _coerce_quote_to_dict(q: Any) -> dict[str, Any]:
    """
    Accept:
      - dict
      - dataclass
      - object with to_dict()
      - object with __dict__ (last resort)
    """
    if isinstance(q, dict):
        return q
    if is_dataclass(q):
        return asdict(q)
    td = getattr(q, "to_dict", None)
    if callable(td):
        out = td()
        assert isinstance(out, dict), "quote.to_dict() must return dict"
        return out
    if hasattr(q, "__dict__"):
        return dict(q.__dict__)
    raise AssertionError(f"Unsupported quote type: {type(q).__name__}")


def _try_ensure_route_quote(q: Any) -> Any:
    """
    If contracts module exposes ensure_route_quote, use it to validate/normalize shape.
    Otherwise, return original.
    """
    # Prefer utils_pkg.contracts.ensure_route_quote
    with contextlib.suppress(Exception):
        c = importlib.import_module("utils_pkg.contracts")
        fn = getattr(c, "ensure_route_quote", None)
        if callable(fn):
            return fn(q)
    # Fallback: router may export it
    return q


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
def test_router_import_is_side_effect_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Contract: importing router must not require secrets and must not open network sockets.
    (conftest.py blocks outbound; this adds local-service loopback protection.)
    """
    with _no_network_local_services(monkeypatch):
        mod = _import_router(fresh=True)
    assert mod is not None


def test_router_exposes_async_quote_functions(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Contract: router exposes async buy/sell quote helpers.
    """
    router = _import_router(fresh=True)

    get_buy = getattr(router, "get_buy_quote", None)
    get_sell = getattr(router, "get_sell_quote", None)

    assert callable(get_buy), "utils_pkg.router must expose get_buy_quote(...)"
    assert callable(get_sell), "utils_pkg.router must expose get_sell_quote(...)"
    assert _is_async_callable(get_buy), "get_buy_quote must be async"
    assert _is_async_callable(get_sell), "get_sell_quote must be async"


def test_quote_signatures_contain_mint_and_amountish_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Lock interface shape (flexible on exact names):
      - must include a mint-ish parameter somewhere
      - must include an amount-ish parameter somewhere
      - must not use positional-only args (stability)
    """
    router = _import_router(fresh=True)

    for fn_name in ("get_buy_quote", "get_sell_quote"):
        fn = getattr(router, fn_name)
        sig = inspect.signature(fn)

        names = [p.name.lower() for p in sig.parameters.values()]
        assert any("mint" in n for n in names), f"{fn_name} must include a mint-ish parameter"
        assert any(
            ("amount" in n)
            or ("amt" in n)
            or ("lamport" in n)
            or (n in ("sol", "sol_in", "amount_sol"))
            for n in names
        ), f"{fn_name} must include an amount/lamports/sol parameter"

        assert all(p.kind != inspect.Parameter.POSITIONAL_ONLY for p in sig.parameters.values()), (
            f"{fn_name} should not use positional-only args (keeps call sites stable)"
        )


def test_deterministic_request_building_if_builder_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    If router provides a pure request builder, it must be deterministic:
      same inputs -> identical url/params (no timestamps/nonces/random ids).
    """
    router = _import_router(fresh=True)
    builder = _discover_request_builder(router)
    if builder is None:
        pytest.skip("No deterministic request builder found (build_quote_*).")

    sig = inspect.signature(builder)

    base_kwargs = dict(
        # mints
        mint=USDC_MINT,
        token_mint=USDC_MINT,
        out_mint=USDC_MINT,
        output_mint=USDC_MINT,
        in_mint=SOL_MINT,
        input_mint=SOL_MINT,
        # amount
        amount=1_000_000,
        amount_raw=1_000_000,
        in_amount_raw=1_000_000,
        lamports=1_000_000,
        sol_in=0.001,
        amount_sol=0.001,
        # slippage/mode
        slippage_bps=50,
        slippage=50,
        swap_mode="ExactIn",
        mode="ExactIn",
        only_direct_routes=False,
        strict=False,
        # safety
        dryrun=True,
        safe_mode=True,
        no_network=True,
        disable_network=True,
    )

    def call_once() -> BuildResult:
        kwargs = {k: v for k, v in base_kwargs.items() if k in sig.parameters}
        with _no_network_local_services(monkeypatch, router):
            out = builder(**kwargs)
        out = _run_awaitable_sync(out)
        return _normalize_build_output(out)

    a = call_once()
    b = call_once()

    assert a.url == b.url, "Builder url must be deterministic"
    assert a.params == b.params, "Builder params must be deterministic"

    suspicious = {"ts", "timestamp", "nonce", "request_id", "rid", "uuid", "rand", "random"}
    assert not (set(k.lower() for k in a.params) & suspicious), (
        "Builder params should not include timestamps/nonces/request_ids in contract tests."
    )

    json.dumps(a.params, sort_keys=True)


@pytest.mark.asyncio
async def test_dryrun_quotes_do_not_hit_network_and_return_route_quote_like(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Contract: In DRYRUN/SAFE_MODE/NO_NETWORK, quote helpers must:
      - not crash
      - not hit network/local services (guards hard-fail)
      - return quickly
      - return a RouteQuote-like object (dict/dataclass), ideally normalized by ensure_route_quote
    """
    router = _import_router(fresh=True)
    get_buy = router.get_buy_quote
    get_sell = router.get_sell_quote

    async def _call(fn: Callable[..., Any], *, direction: str) -> Any:
        sig = inspect.signature(fn)
        kwargs = _fill_required_kwargs(sig, direction=direction)
        assert isinstance(kwargs, dict) and kwargs, "Could not build kwargs for router call"
        with _no_network_local_services(monkeypatch, router):
            return await asyncio.wait_for(fn(**kwargs), timeout=5.0)

    q_buy = await _call(get_buy, direction="buy")
    q_sell = await _call(get_sell, direction="sell")

    # Normalize/validate shape if contracts helper exists
    q_buy2 = _try_ensure_route_quote(q_buy)
    q_sell2 = _try_ensure_route_quote(q_sell)

    d_buy = _coerce_quote_to_dict(q_buy2)
    d_sell = _coerce_quote_to_dict(q_sell2)

    for d in (d_buy, d_sell):
        assert isinstance(d, dict)

        # Should be JSON-safe for logs (allow default=str for any exotic types)
        json.dumps(d, default=str, sort_keys=True)

        # Minimal interface expectations (LOCK-IN)
        if "ok" in d:
            assert isinstance(d["ok"], bool | int), "quote.ok must be bool-like"
        if "reason" in d and d["reason"] is not None:
            assert isinstance(d["reason"], str), "quote.reason must be str"

        # Mint-ish fields must exist somewhere
        mintish_keys = [k for k in d if "mint" in str(k).lower()]
        assert mintish_keys, f"quote dict missing mint-ish fields: keys={sorted(d.keys())[:60]}"
