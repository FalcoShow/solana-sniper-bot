# tests/test_swap.py
import pytest
import types

import utils_pkg.core_utils as cu


# ---------------------------
# Minimal async response/session fakes
# ---------------------------

class FakeResponse:
    def __init__(self, status=200, json_data=None, text_data="OK"):
        self.status = status
        self._json = json_data or {}
        self._text = text_data

    async def json(self):
        return self._json

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class CapturingSession:
    """Captures GETs for param inspection; returns a configurable response factory."""
    def __init__(self, responder=None):
        self.get_calls = []
        self._responder = responder or (lambda url, **kw: FakeResponse(200, {"ok": True}))
        self.post_calls = []

    def get(self, url, headers=None, params=None):
        self.get_calls.append({"url": url, "headers": headers, "params": params})
        return self._responder(url, headers=headers, params=params)

    def post(self, url, json=None):
        self.post_calls.append({"url": url, "json": json})
        return FakeResponse(200, {"ok": True})


# ---------------------------
# Helpers
# ---------------------------

SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_MINT = "Tok3nMint111111111111111111111111111111111"


# ---------------------------
# swap_token_jupiter — happy paths
# ---------------------------

@pytest.mark.asyncio
async def test_swap_buy_happy_path(monkeypatch):
    captured = {}
    async def fake_get_quote(input_mint, output_mint, amount, slippage_pct):
        captured["input_mint"] = input_mint
        captured["output_mint"] = output_mint
        captured["amount"] = amount
        captured["slippage_pct"] = slippage_pct
        return {"data": [{"route": "ROUTE"}]}

    async def fake_perform(quote):
        assert quote == {"data": [{"route": "ROUTE"}]}
        return True, "0xABC"

    monkeypatch.setattr(cu, "get_jupiter_quote", fake_get_quote, raising=True)
    monkeypatch.setattr(cu, "perform_jupiter_swap", fake_perform, raising=True)

    ok, sig = await cu.swap_token_jupiter(
        input_mint=SOL_MINT,
        output_mint=TOKEN_MINT,
        amount_float=1.2345,
        buy=True,
        slippage=0.0075,
    )
    assert ok is True and sig == "0xABC"

    # Amount is lamports (1e9), slippage is forwarded as pct (bps conversion happens in get_jupiter_quote)
    assert captured["input_mint"] == SOL_MINT
    assert captured["output_mint"] == TOKEN_MINT
    assert captured["amount"] == int(1.2345 * 1e9)
    assert captured["slippage_pct"] == 0.0075


@pytest.mark.asyncio
async def test_swap_sell_happy_path(monkeypatch):
    captured = {}
    async def fake_get_quote(input_mint, output_mint, amount, slippage_pct):
        captured.update(dict(input_mint=input_mint, output_mint=output_mint, amount=amount, slippage_pct=slippage_pct))
        return {"data": [{"route": "ROUTE"}]}

    async def fake_perform(quote):
        return True, "0xSELL"

    monkeypatch.setattr(cu, "get_jupiter_quote", fake_get_quote, raising=True)
    monkeypatch.setattr(cu, "perform_jupiter_swap", fake_perform, raising=True)

    ok, sig = await cu.swap_token_jupiter(
        input_mint=TOKEN_MINT,
        output_mint=SOL_MINT,
        amount_float=0.5,
        buy=False,
        slippage=0.005,
    )
    assert ok is True and sig == "0xSELL"
    assert captured["input_mint"] == TOKEN_MINT
    assert captured["output_mint"] == SOL_MINT
    assert captured["amount"] == int(0.5 * 1e9)
    assert captured["slippage_pct"] == 0.005


# ---------------------------
# swap_token_jupiter — failures/validation
# ---------------------------

@pytest.mark.asyncio
async def test_swap_invalid_inputs(monkeypatch):
    ok, sig = await cu.swap_token_jupiter(None, SOL_MINT, 1.0, buy=True, slippage=0.005)
    assert ok is False and sig == ""

    ok, sig = await cu.swap_token_jupiter(TOKEN_MINT, None, 1.0, buy=False, slippage=0.005)
    assert ok is False and sig == ""


@pytest.mark.asyncio
async def test_swap_quote_failure(monkeypatch):
    async def fake_get_quote(*a, **k): return None
    monkeypatch.setattr(cu, "get_jupiter_quote", fake_get_quote, raising=True)

    ok, sig = await cu.swap_token_jupiter(SOL_MINT, TOKEN_MINT, 1.0, buy=True, slippage=0.005)
    assert ok is False and sig == ""


# ---------------------------
# get_jupiter_quote — param plumbing
# ---------------------------

@pytest.mark.asyncio
async def test_get_jupiter_quote_converts_to_bps_and_lamports(monkeypatch):
    # Fake wallet so userPublicKey gets populated
    class FakeWallet:
        def pubkey(self): return "FakePubKey1111111111111111111111111111111111"

    cu.wallet = FakeWallet()

    def responder(url, headers=None, params=None):
        # Assert slippageBps and amount formatting
        assert isinstance(params, dict)
        assert params["slippageBps"] == int(0.006 * 10000)
        assert params["amount"] == str(123_000_000)  # 0.123 SOL/token * 1e9
        assert params["userPublicKey"] == str(cu.wallet.pubkey())
        return FakeResponse(200, {"data": [{"route": "OK"}]})

    sess = CapturingSession(responder=responder)
    monkeypatch.setattr(cu, "session", sess, raising=False)
    monkeypatch.setattr(cu, "JUPITER_SWAP_API", "https://quote-api.jup.ag/v6/quote", raising=False)

    out = await cu.get_jupiter_quote(SOL_MINT, TOKEN_MINT, amount=123_000_000, slippage_pct=0.006)
    assert out == {"data": [{"route": "OK"}]}
    # sanity: one GET call recorded
    assert len(sess.get_calls) == 1


# ---------------------------
# perform_jupiter_swap — guard rails
# ---------------------------

@pytest.mark.asyncio
async def test_perform_swap_requires_client_and_wallet(monkeypatch):
    # No client/wallet → should early-return False, ""
    monkeypatch.setattr(cu, "solana_client", None, raising=False)
    monkeypatch.setattr(cu, "wallet", None, raising=False)
    ok, sig = await cu.perform_jupiter_swap({"data": [{"route": "R"}]})
    assert ok is False and sig == ""


@pytest.mark.asyncio
async def test_perform_swap_no_routes(monkeypatch):
    # Provide dummy client+wallet so we pass the first guard,
    # but quote has no routes → False, ""
    class Dummy:
        pass

    monkeypatch.setattr(cu, "solana_client", Dummy(), raising=False)
    monkeypatch.setattr(cu, "wallet", Dummy(), raising=False)

    ok, sig = await cu.perform_jupiter_swap({"data": []})
    assert ok is False and sig == ""
