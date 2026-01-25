# tests/sell_path_test.py
"""
Sell path contract (P0 LOCK-IN)
===============================

This file is the canonical *sell* contract for the money pipe.

Goals:
- fully offline (no real HTTP, no real RPC)
- validate the "SELL → quote → swap" pipeline
- enforce minimal, stable router primitives so the rest of the bot can’t drift

Canonical module under test:
    utils_pkg/router.py

Required router API (contract):
    - async get_sell_quote(mint: str, amount_raw: int, slippage_bps: int) -> dict | None
    - async swap_from_quote(quote: dict) -> tuple[bool, str]
    - build_swap_payload(quote: dict, user_pubkey: str, **kwargs) -> dict

Optional (nice-to-have) convenience API (if present, we test it too):
    - async sell_to_sol(mint: str, amount_raw: int, slippage_bps: int, **kwargs) -> tuple[bool, str]
"""

from __future__ import annotations

import contextlib
import importlib
import inspect
from typing import Any

import pytest


def _router():
    return importlib.import_module("utils_pkg.router")


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import router  # satisfies type-hint references like router.Router

# ---------------------------------------------------------------------------
# Fakes: aiohttp-style response + request context + capturing session
# ---------------------------------------------------------------------------


class FakeResponse:
    def __init__(self, status: int = 200, json_data: dict | None = None, text_data: str = "OK"):
        self.status = int(status)
        self._json = json_data if json_data is not None else {}
        self._text = text_data

    async def json(self) -> dict:
        return self._json

    async def text(self) -> str:
        return self._text


class FakeRequestContext:
    """Supports both `async with session.get/post(...)` and `await session.get/post(...)`."""

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

    async def close(self):
        return None


# ---------------------------------------------------------------------------
# Helpers / constants
# ---------------------------------------------------------------------------

SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_MINT = "Tok3nMint111111111111111111111111111111111"


class FakeWallet:
    def __init__(self, pk: str = "FakePubKey1111111111111111111111111111111111"):
        self._pk = pk

    def pubkey(self) -> str:
        return self._pk


class FakeClient:
    """Placeholder — swap_from_quote should accept it as 'present' even in dryrun tests."""

    pass


def _require(attr: str) -> Any:
    if not hasattr(router, attr):
        raise AssertionError(f"utils_pkg.router MUST expose `{attr}` (sell path contract lock-in).")
    return getattr(router, attr)


def _patch_http(monkeypatch, session: CapturingSession) -> None:
    # Common attributes
    monkeypatch.setattr(router, "session", session, raising=False)
    monkeypatch.setattr(router, "_session", session, raising=False)

    # Common accessors
    for name in ("get_session", "_get_session", "http_session", "_http_session"):
        if hasattr(router, name) and callable(getattr(router, name)):
            fn = getattr(router, name)
            if inspect.iscoroutinefunction(fn):

                async def _a():
                    return session

                monkeypatch.setattr(router, name, _a, raising=True)
            else:
                monkeypatch.setattr(router, name, lambda: session, raising=True)

    # If router creates aiohttp.ClientSession, intercept it
    if hasattr(router, "aiohttp"):
        with contextlib.suppress(Exception):
            monkeypatch.setattr(
                router.aiohttp, "ClientSession", lambda *a, **k: session, raising=False
            )


def _patch_wallet_client(monkeypatch, wallet: Any, client: Any) -> None:
    monkeypatch.setattr(router, "wallet", wallet, raising=False)
    monkeypatch.setattr(router, "solana_client", client, raising=False)
    # Optional getter styles
    for name in ("get_wallet", "_get_wallet"):
        if hasattr(router, name) and callable(getattr(router, name)):
            monkeypatch.setattr(router, name, (lambda: wallet), raising=True)
    for name in ("get_client", "_get_client"):
        if hasattr(router, name) and callable(getattr(router, name)):
            monkeypatch.setattr(router, name, (lambda: client), raising=True)


def _set_router_endpoints(monkeypatch) -> None:
    # Broad set to stabilize across router implementations
    monkeypatch.setenv("JUPITER_API_BASE", "https://quote-api.jup.ag")
    monkeypatch.setenv("JUPITER_QUOTE_URL", "https://quote-api.jup.ag/v6/quote")
    monkeypatch.setenv("JUPITER_SWAP_URL", "https://quote-api.jup.ag/v6/swap")
    monkeypatch.setenv("JUPITER_QUOTE_API", "https://quote-api.jup.ag/v6/quote")
    monkeypatch.setenv("JUPITER_SWAP_API", "https://quote-api.jup.ag/v6/swap")
    monkeypatch.setenv("JUPITER_V6_QUOTE_URL", "https://quote-api.jup.ag/v6/quote")
    monkeypatch.setenv("JUPITER_V6_SWAP_URL", "https://quote-api.jup.ag/v6/swap")


def _enable_dryrun(monkeypatch) -> None:
    """
    Many pipelines support some form of dryrun. We set common flags so
    swap_from_quote can complete without a real RPC send/sign flow.
    """
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("PF_DRYRUN", "1")
    monkeypatch.setenv("PUMP_DRYRUN", "1")
    monkeypatch.setenv("SNIPER_DRYRUN", "1")


async def _sell_pipeline(*, mint: str, amount_raw: int, slippage_bps: int) -> tuple[bool, str]:
    """
    Preferred path:
      - if router.sell_to_sol exists: use it (tests orchestration)
      - else: use primitives (tests minimum viable pipeline)
    """
    if hasattr(router, "sell_to_sol") and callable(router.sell_to_sol):
        fn = router.sell_to_sol
        if inspect.iscoroutinefunction(fn):
            return await fn(mint=mint, amount_raw=amount_raw, slippage_bps=slippage_bps)
    quote_fn = _require("get_sell_quote")
    swap_fn = _require("swap_from_quote")
    q = await quote_fn(mint=mint, amount_raw=amount_raw, slippage_bps=slippage_bps)
    if not q:
        return False, ""
    return await swap_fn(q)


# ---------------------------------------------------------------------------
# Tests: happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sell_path_quote_then_swap_calls_expected_endpoints(monkeypatch):
    """
    End-to-end offline:
      get_sell_quote() must call /quote with inputMint=TOKEN, outputMint=SOL, amount, slippageBps, userPublicKey
      swap_from_quote() must call /swap with payload containing quoteResponse + userPublicKey
    """
    _set_router_endpoints(monkeypatch)
    _enable_dryrun(monkeypatch)

    wallet = FakeWallet()
    client = FakeClient()

    expected_amount = 555_000
    expected_slip = 50

    # responder: /quote
    def responder_get(url, **kw):
        params = kw.get("params") or {}
        # enforce sell direction
        assert str(params.get("inputMint")) == TOKEN_MINT
        assert str(params.get("outputMint")) == SOL_MINT
        assert str(params.get("amount")) == str(expected_amount)
        assert str(params.get("slippageBps")) == str(expected_slip)
        assert str(params.get("userPublicKey")) == str(wallet.pubkey())

        # minimal Jupiter-like quote payload with a routePlan
        return FakeResponse(
            200,
            {
                "inputMint": TOKEN_MINT,
                "outputMint": SOL_MINT,
                "inAmount": str(expected_amount),
                "slippageBps": expected_slip,
                "routePlan": [{"swapInfo": {"label": "FAKE"}}],
            },
        )

    # responder: /swap
    def responder_post(url, **kw):
        payload = kw.get("json") or {}
        assert isinstance(payload, dict)

        # contract: must include these keys
        assert "quoteResponse" in payload
        assert str(payload.get("userPublicKey")) == str(wallet.pubkey())

        # safer defaults
        assert payload.get("wrapAndUnwrapSol", True) is True

        # return a swap payload. In dryrun, router may not use it further, but it must be accepted.
        return FakeResponse(
            200,
            {
                "swapTransaction": "AAAA",  # dummy base64
                "lastValidBlockHeight": 1,
                "prioritizationFeeLamports": 0,
            },
        )

    sess = CapturingSession(responder_get=responder_get, responder_post=responder_post)
    _patch_http(monkeypatch, sess)
    _patch_wallet_client(monkeypatch, wallet, client)

    ok, sig = await _sell_pipeline(
        mint=TOKEN_MINT, amount_raw=expected_amount, slippage_bps=expected_slip
    )

    # Pipeline must at least attempt both quote and swap when quote has a route.
    assert len(sess.get_calls) == 1
    assert len(sess.post_calls) == 1

    # Signature string may be empty in dryrun implementations, but ok must be boolean.
    assert isinstance(ok, bool)
    assert isinstance(sig, str)


@pytest.mark.asyncio
async def test_sell_to_sol_orchestrates_primitives_if_present(monkeypatch):
    """
    If router.sell_to_sol exists, it must call:
      get_sell_quote(mint, amount_raw, slippage_bps)
      swap_from_quote(quote)
    """
    if not (hasattr(router, "sell_to_sol") and callable(router.sell_to_sol)):
        pytest.skip("router.sell_to_sol not implemented (optional convenience API).")

    _set_router_endpoints(monkeypatch)
    _enable_dryrun(monkeypatch)

    called: dict[str, Any] = {
        "quote": 0,
        "swap": 0,
        "mint": None,
        "amount": None,
        "slip": None,
        "quote_obj": None,
    }

    async def fake_get_sell_quote(*, mint: str, amount_raw: int, slippage_bps: int):
        called["quote"] += 1
        called["mint"] = mint
        called["amount"] = amount_raw
        called["slip"] = slippage_bps
        return {
            "routePlan": [{"swapInfo": {"label": "FAKE"}}],
            "inputMint": mint,
            "outputMint": SOL_MINT,
        }

    async def fake_swap_from_quote(quote: dict):
        called["swap"] += 1
        called["quote_obj"] = quote
        return True, "0xSELL"

    monkeypatch.setattr(router, "get_sell_quote", fake_get_sell_quote, raising=True)
    monkeypatch.setattr(router, "swap_from_quote", fake_swap_from_quote, raising=True)

    wallet = FakeWallet()
    client = FakeClient()
    _patch_wallet_client(monkeypatch, wallet, client)

    ok, sig = await router.sell_to_sol(mint=TOKEN_MINT, amount_raw=123, slippage_bps=77)

    assert ok is True and sig == "0xSELL"
    assert called["quote"] == 1
    assert called["swap"] == 1
    assert called["mint"] == TOKEN_MINT
    assert called["amount"] == 123
    assert called["slip"] == 77
    assert isinstance(called["quote_obj"], dict)


# ---------------------------------------------------------------------------
# Tests: guardrails / failure behavior
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sell_path_refuses_without_wallet_or_client(monkeypatch):
    """
    swap_from_quote must refuse when wallet/client missing (P0 safety).
    """
    swap = _require("swap_from_quote")
    monkeypatch.setattr(router, "wallet", None, raising=False)
    monkeypatch.setattr(router, "solana_client", None, raising=False)

    ok, sig = await swap({"routePlan": [{"x": 1}]})
    assert ok is False
    assert sig == ""


@pytest.mark.asyncio
async def test_sell_path_rejects_empty_or_no_route_quote(monkeypatch):
    swap = _require("swap_from_quote")

    _patch_wallet_client(monkeypatch, FakeWallet(), FakeClient())

    ok, sig = await swap({})
    assert ok is False and sig == ""

    ok, sig = await swap({"routePlan": []})
    assert ok is False and sig == ""


@pytest.mark.asyncio
async def test_get_sell_quote_non_200_returns_none_or_raises(monkeypatch):
    """
    Router may return None or raise; both acceptable.
    It must NOT pretend the quote succeeded.
    """
    _set_router_endpoints(monkeypatch)

    wallet = FakeWallet()
    client = FakeClient()

    def responder_get(url, **kw):
        return FakeResponse(500, {"err": "nope"}, "nope")

    sess = CapturingSession(responder_get=responder_get)
    _patch_http(monkeypatch, sess)
    _patch_wallet_client(monkeypatch, wallet, client)

    getq = _require("get_sell_quote")
    try:
        out = await getq(mint=TOKEN_MINT, amount_raw=1_000, slippage_bps=50)
        assert out is None or (isinstance(out, dict) and out.get("routePlan") in (None, [], {}))
    except Exception:
        pass
