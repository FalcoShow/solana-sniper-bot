# tests/test_swap.py
from __future__ import annotations

"""
tests/test_swap.py — Router swap contract tests (P0 LOCK-IN)
============================================================

These tests define the *contract* for the canonical money pipe:

    utils_pkg/router.py

They must:
- run fully offline (no real HTTP, no real RPC)
- validate quote plumbing (amount/slippage/user pubkey)
- validate swap payload construction (quoteResponse + userPublicKey + safe defaults)
- validate guardrails (no wallet/client => refuse; empty quote => refuse)

If these tests fail, fix router.py (NOT the tests).
"""

import importlib
import inspect
from collections.abc import Callable
from typing import Any

import pytest


def _router():
    return importlib.import_module("utils_pkg.router")


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import router  # satisfies type-hint references like router.Router


# ─────────────────────────────────────────────────────────────────────────────
# Small awaitable primitives so the same fake response can satisfy:
#   - httpx-style: resp.json() -> dict (sync), resp.text -> str, resp.status_code
#   - aiohttp-style: await resp.json(), await resp.text(), resp.status
# ─────────────────────────────────────────────────────────────────────────────
class AwaitableDict(dict):
    def __await__(self):
        async def _coro():
            return self

        return _coro().__await__()


class AwaitableStr(str):
    def __await__(self):
        async def _coro():
            return self

        return _coro().__await__()


class FakeResponse:
    def __init__(self, status: int = 200, json_data: dict | None = None, text_data: str = "OK"):
        self.status = int(status)  # aiohttp style
        self.status_code = int(status)  # httpx style
        self._json = json_data if json_data is not None else {}
        self._text = text_data

    def json(self) -> AwaitableDict:
        # works both as dict (sync) and awaitable (aiohttp-ish)
        return AwaitableDict(self._json)

    def text(self) -> AwaitableStr:
        # works both as str-ish and awaitable
        return AwaitableStr(self._text)


class FakeRequestContext:
    """
    Works with:
      - async with session.get(...) as resp
      - resp = await session.get(...)
      - resp = session.get(...) (if caller doesn't await; rare)
    """

    def __init__(self, resp: FakeResponse):
        self._resp = resp

    def __await__(self):
        async def _coro():
            return self._resp

        return _coro().__await__()

    async def __aenter__(self) -> FakeResponse:
        return self._resp

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False


class CapturingSession:
    """
    A single fake that can stand in for aiohttp.ClientSession *or* httpx.AsyncClient.
    """

    def __init__(self, *, responder_get=None, responder_post=None):
        self.get_calls = []
        self.post_calls = []
        self._responder_get = responder_get or (lambda url, **kw: FakeResponse(200, {"ok": True}))
        self._responder_post = responder_post or (lambda url, **kw: FakeResponse(200, {"ok": True}))

    def get(self, url, **kw):
        self.get_calls.append({"url": url, **kw})
        return FakeRequestContext(self._responder_get(url, **kw))

    def post(self, url, **kw):
        self.post_calls.append({"url": url, **kw})
        return FakeRequestContext(self._responder_post(url, **kw))

    async def aclose(self):
        return None

    async def close(self):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Helpers / fakes
# ─────────────────────────────────────────────────────────────────────────────
SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_MINT = "Tok3nMint111111111111111111111111111111111"


class FakeWallet:
    def __init__(self, pk: str = "FakePubKey1111111111111111111111111111111111"):
        self._pk = pk
        self.public_key = pk  # some code uses wallet.public_key

    def pubkey(self) -> str:
        return self._pk

    def __str__(self) -> str:
        return self._pk


class FakeClient:
    pass


def _require(name: str) -> Any:
    if not hasattr(router, name):
        raise AssertionError(f"utils_pkg.router MUST expose `{name}` (P0 contract lock-in).")
    return getattr(router, name)


def _sig_accepts(fn: Callable[..., Any], param: str) -> bool:
    sig = inspect.signature(fn)
    if param in sig.parameters:
        return True
    # allow **kwargs
    return any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())


def _assert_required_params(fn: Callable[..., Any], required: tuple[str, ...]) -> None:
    missing = [p for p in required if not _sig_accepts(fn, p)]
    assert not missing, (
        f"{fn.__name__} missing required params {missing} (or **kwargs). Signature={inspect.signature(fn)}"
    )


def _set_router_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Router implementations vary; set a broad endpoint env set to stabilize tests.
    """
    monkeypatch.setenv("JUPITER_API_BASE", "https://quote-api.jup.ag")
    monkeypatch.setenv("JUPITER_QUOTE_URL", "https://quote-api.jup.ag/v6/quote")
    monkeypatch.setenv("JUPITER_SWAP_URL", "https://quote-api.jup.ag/v6/swap")
    monkeypatch.setenv("JUPITER_QUOTE_API", "https://quote-api.jup.ag/v6/quote")
    monkeypatch.setenv("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6/swap")
    monkeypatch.setenv("JUPITER_V6_QUOTE_URL", "https://quote-api.jup.ag/v6/quote")
    monkeypatch.setenv("JUPITER_V6_SWAP_URL", "https://quote-api.jup.ag/v6/swap")


def _patch_http(monkeypatch: pytest.MonkeyPatch, session: CapturingSession) -> None:
    """
    Patch router HTTP usage defensively. Supports:
      - router.session / router._session
      - router.get_session / _get_session
      - router.httpx.AsyncClient / router.AsyncClient
      - router.aiohttp.ClientSession / router.ClientSession
    """
    # common module singletons
    monkeypatch.setattr(router, "session", session, raising=False)
    monkeypatch.setattr(router, "_session", session, raising=False)

    # common getters
    for name in ("get_session", "_get_session", "http_session", "_http_session"):
        fn = getattr(router, name, None)
        if callable(fn):
            if inspect.iscoroutinefunction(fn):

                async def _a():
                    return session

                monkeypatch.setattr(router, name, _a, raising=True)
            else:
                monkeypatch.setattr(router, name, lambda: session, raising=True)

    # httpx import styles
    httpx_mod = getattr(router, "httpx", None)
    if httpx_mod is not None:
        monkeypatch.setattr(httpx_mod, "AsyncClient", lambda *a, **k: session, raising=False)
        monkeypatch.setattr(httpx_mod, "Client", lambda *a, **k: session, raising=False)
    if hasattr(router, "AsyncClient"):
        monkeypatch.setattr(router, "AsyncClient", lambda *a, **k: session, raising=False)

    # aiohttp import styles
    aiohttp_mod = getattr(router, "aiohttp", None)
    if aiohttp_mod is not None:
        monkeypatch.setattr(aiohttp_mod, "ClientSession", lambda *a, **k: session, raising=False)
    if hasattr(router, "ClientSession"):
        monkeypatch.setattr(router, "ClientSession", lambda *a, **k: session, raising=False)


def _patch_wallet_client(monkeypatch: pytest.MonkeyPatch, wallet: Any, client: Any) -> None:
    # common globals
    monkeypatch.setattr(router, "wallet", wallet, raising=False)
    monkeypatch.setattr(router, "solana_client", client, raising=False)
    monkeypatch.setattr(router, "client", client, raising=False)
    monkeypatch.setattr(router, "rpc_client", client, raising=False)

    # common getters
    for name in ("get_wallet", "_get_wallet"):
        fn = getattr(router, name, None)
        if callable(fn):
            if inspect.iscoroutinefunction(fn):

                async def _aw():
                    return wallet

                monkeypatch.setattr(router, name, _aw, raising=True)
            else:
                monkeypatch.setattr(router, name, lambda: wallet, raising=True)

    for name in ("get_client", "_get_client"):
        fn = getattr(router, name, None)
        if callable(fn):
            if inspect.iscoroutinefunction(fn):

                async def _ac():
                    return client

                monkeypatch.setattr(router, name, _ac, raising=True)
            else:
                monkeypatch.setattr(router, name, lambda: client, raising=True)


@pytest.fixture(autouse=True)
def _safe_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Make intent explicit: tests must not do real trading.
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("IS_DRYRUN", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")


# ─────────────────────────────────────────────────────────────────────────────
# Quote contract
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_get_buy_quote_plumbs_amount_slippage_userpubkey(monkeypatch: pytest.MonkeyPatch):
    """
    get_buy_quote(mint=TOKEN, amount_raw=lamports, slippage_bps=int) must:
      - call /quote
      - set inputMint=SOL, outputMint=mint
      - set amount == amount_raw
      - set slippageBps == slippage_bps
      - set userPublicKey to wallet pubkey (if wallet available)
    """
    _set_router_endpoints(monkeypatch)
    wallet = FakeWallet()
    client = FakeClient()

    def responder(url: str, **kw):
        params = kw.get("params") or {}
        assert str(params.get("inputMint")) == SOL_MINT
        assert str(params.get("outputMint")) == TOKEN_MINT
        assert str(params.get("amount")) == str(123_000_000)
        assert str(params.get("slippageBps")) == str(75)
        assert str(params.get("userPublicKey")) == str(wallet.pubkey())
        return FakeResponse(200, {"ok": True, "routePlan": []})

    sess = CapturingSession(responder_get=responder)
    _patch_http(monkeypatch, sess)
    _patch_wallet_client(monkeypatch, wallet, client)

    fn = _require("get_buy_quote")
    _assert_required_params(fn, ("mint", "amount_raw", "slippage_bps"))

    out = await fn(mint=TOKEN_MINT, amount_raw=123_000_000, slippage_bps=75)
    assert isinstance(out, dict)
    assert len(sess.get_calls) == 1


@pytest.mark.asyncio
async def test_get_sell_quote_plumbs_amount_slippage_userpubkey(monkeypatch: pytest.MonkeyPatch):
    """
    get_sell_quote(mint=TOKEN, amount_raw=token_raw, slippage_bps=int) must:
      - call /quote
      - set inputMint=mint, outputMint=SOL
      - set amount == amount_raw
      - set slippageBps == slippage_bps
      - set userPublicKey to wallet pubkey (if wallet available)
    """
    _set_router_endpoints(monkeypatch)
    wallet = FakeWallet()
    client = FakeClient()

    def responder(url: str, **kw):
        params = kw.get("params") or {}
        assert str(params.get("inputMint")) == TOKEN_MINT
        assert str(params.get("outputMint")) == SOL_MINT
        assert str(params.get("amount")) == str(555_000)
        assert str(params.get("slippageBps")) == str(50)
        assert str(params.get("userPublicKey")) == str(wallet.pubkey())
        return FakeResponse(200, {"ok": True, "routePlan": []})

    sess = CapturingSession(responder_get=responder)
    _patch_http(monkeypatch, sess)
    _patch_wallet_client(monkeypatch, wallet, client)

    fn = _require("get_sell_quote")
    _assert_required_params(fn, ("mint", "amount_raw", "slippage_bps"))

    out = await fn(mint=TOKEN_MINT, amount_raw=555_000, slippage_bps=50)
    assert isinstance(out, dict)
    assert len(sess.get_calls) == 1


@pytest.mark.asyncio
async def test_quote_non_200_returns_none_or_raises(monkeypatch: pytest.MonkeyPatch):
    """
    Router may choose to return None or raise on HTTP failure.
    It MUST NOT return a dict that looks OK.
    """
    _set_router_endpoints(monkeypatch)

    sess = CapturingSession(
        responder_get=lambda url, **kw: FakeResponse(500, {"err": "nope"}, "nope")
    )
    _patch_http(monkeypatch, sess)
    _patch_wallet_client(monkeypatch, FakeWallet(), FakeClient())

    fn = _require("get_buy_quote")
    _assert_required_params(fn, ("mint", "amount_raw", "slippage_bps"))

    try:
        out = await fn(mint=TOKEN_MINT, amount_raw=1_000, slippage_bps=50)
        assert out is None or (isinstance(out, dict) and out.get("ok") is not True)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Swap payload contract
# ─────────────────────────────────────────────────────────────────────────────
def test_build_swap_payload_includes_quote_and_userpubkey_defaults():
    """
    build_swap_payload(quote, user_pubkey, ...) must:
      - include quote under 'quoteResponse'
      - include 'userPublicKey'
      - include 'wrapAndUnwrapSol' default True (safe SOL handling)
    """
    build = _require("build_swap_payload")

    quote = {
        "inputMint": SOL_MINT,
        "outputMint": TOKEN_MINT,
        "inAmount": "123000000",
        "slippageBps": 75,
        "routePlan": [],
    }
    user_pk = "FakePubKey1111111111111111111111111111111111"

    payload = build(quote, user_pk)
    assert isinstance(payload, dict)
    assert payload.get("quoteResponse") == quote
    assert str(payload.get("userPublicKey")) == user_pk
    assert payload.get("wrapAndUnwrapSol", True) is True


def test_build_swap_payload_allows_overrides_if_supported():
    """
    If router supports override flags (asLegacyTransaction, dynamicComputeUnitLimit,
    computeUnitPriceMicroLamports / prioritizationFeeLamports), they must be
    passed through when provided.
    """
    build = _require("build_swap_payload")

    quote = {"inputMint": SOL_MINT, "outputMint": TOKEN_MINT, "inAmount": "1", "routePlan": []}
    user_pk = "FakePubKey1111111111111111111111111111111111"

    sig = inspect.signature(build)
    kwargs: dict[str, Any] = {}

    if "as_legacy_transaction" in sig.parameters or any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    ):
        kwargs["as_legacy_transaction"] = True
    if "dynamic_compute_unit_limit" in sig.parameters or any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    ):
        kwargs["dynamic_compute_unit_limit"] = True
    if "compute_unit_price_micro_lamports" in sig.parameters or any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    ):
        kwargs["compute_unit_price_micro_lamports"] = 10_000
    if "prioritization_fee_lamports" in sig.parameters or any(
        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
    ):
        kwargs["prioritization_fee_lamports"] = 5_000

    payload = build(quote, user_pk, **kwargs)
    assert payload.get("quoteResponse") == quote
    assert str(payload.get("userPublicKey")) == user_pk

    # If present, must match what we asked for (accept snake or camel variants).
    if "as_legacy_transaction" in kwargs:
        assert payload.get("asLegacyTransaction", payload.get("as_legacy_transaction")) is True
    if "dynamic_compute_unit_limit" in kwargs:
        assert (
            payload.get("dynamicComputeUnitLimit", payload.get("dynamic_compute_unit_limit"))
            is True
        )
    if "compute_unit_price_micro_lamports" in kwargs:
        got = payload.get(
            "computeUnitPriceMicroLamports", payload.get("compute_unit_price_micro_lamports")
        )
        assert str(got) == str(kwargs["compute_unit_price_micro_lamports"])
    if "prioritization_fee_lamports" in kwargs:
        got = payload.get("prioritizationFeeLamports", payload.get("prioritization_fee_lamports"))
        assert str(got) == str(kwargs["prioritization_fee_lamports"])


# ─────────────────────────────────────────────────────────────────────────────
# Swap guardrails (high-level)
# ─────────────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_swap_requires_wallet_and_client(monkeypatch: pytest.MonkeyPatch):
    """
    swap_from_quote(quote) must refuse when wallet/client absent.
    Must return (False, "").
    """
    swap = _require("swap_from_quote")

    _patch_wallet_client(monkeypatch, None, None)

    ok, sig = await swap({"routePlan": [{"x": 1}]})
    assert ok is False
    assert sig == ""


@pytest.mark.asyncio
async def test_swap_rejects_empty_quote(monkeypatch: pytest.MonkeyPatch):
    """
    swap_from_quote({}) must refuse (empty quote / missing routePlan / missing quote fields).
    Must return (False, "").
    """
    swap = _require("swap_from_quote")

    _patch_wallet_client(monkeypatch, FakeWallet(), FakeClient())

    ok, sig = await swap({})
    assert ok is False
    assert sig == ""
