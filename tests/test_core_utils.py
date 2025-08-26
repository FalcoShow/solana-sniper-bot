# tests/test_core_utils.py
import asyncio
import json
import types
import pytest

# Module under test
import utils_pkg.core_utils as cu


# ---------------------------
# Lightweight async test fakes
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


class FakeSession:
    def __init__(self):
        self.get_calls = []
        self.post_calls = []

    # aiohttp returns an async context manager; we return a FakeResponse
    def get(self, url, headers=None, params=None):
        self.get_calls.append({"url": url, "headers": headers, "params": params})
        # Route-aware stubs
        if "coingecko" in url:
            return FakeResponse(200, {"solana": {"usd": 150.0}})
        if "public-api.birdeye.so/public/price" in url:
            return FakeResponse(200, {"data": {"value": 0.123456}})
        # default: 404
        return FakeResponse(404, {}, "not found")

    def post(self, url, json=None):
        self.post_calls.append({"url": url, "json": json})
        return FakeResponse(200, {"ok": True})


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def set(self, key, value, ex=None):
        self.store[key] = value

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, ttl, value):
        self.store[key] = value

    async def exists(self, key):
        return 1 if key in self.store else 0

    async def lpush(self, key, value):
        self.store.setdefault(key, [])
        self.store[key].insert(0, value)

    async def ltrim(self, key, start, end):
        if key in self.store and isinstance(self.store[key], list):
            self.store[key] = self.store[key][start : end + 1]

    async def close(self):
        return None


# ---------------------------
# Fixtures
# ---------------------------

@pytest.fixture(autouse=True)
def isolate_globals(monkeypatch):
    """
    Keep core_utils globals isolated per test to avoid cross-test bleed.
    """
    # Reset global session/redis to None (tests will assign fakes)
    monkeypatch.setattr(cu, "session", None, raising=False)
    monkeypatch.setattr(cu, "redis", None, raising=False)


@pytest.fixture
def fake_session(monkeypatch):
    s = FakeSession()
    monkeypatch.setattr(cu, "session", s, raising=False)
    return s


@pytest.fixture
def fake_redis(monkeypatch):
    r = FakeRedis()
    monkeypatch.setattr(cu, "redis", r, raising=False)
    return r


# ---------------------------
# Tests
# ---------------------------

@pytest.mark.asyncio
async def test_convert_sol_to_usd(fake_session):
    out = await cu.convert_sol_to_usd(2.0)  # 2 SOL * $150
    assert out == 300.0
    # sanity: one GET hit coingecko
    assert any("coingecko" in call["url"] for call in fake_session.get_calls)


@pytest.mark.asyncio
async def test_get_token_price(fake_session):
    price = await cu.get_token_price("SomeMint1111111111111111111111111111111")
    assert isinstance(price, float)
    assert price == pytest.approx(0.123456, rel=1e-6)


@pytest.mark.asyncio
async def test_token_price_handles_missing_session(monkeypatch):
    # No session → function should return 0.0 and not crash
    monkeypatch.setattr(cu, "session", None, raising=False)
    price = await cu.get_token_price("AnyMint")
    assert price == 0.0


def test_dca_profit_exit():
    assert cu.dca_profit_exit(current=2.0, entry=1.0, target=2.0) is True
    assert cu.dca_profit_exit(current=1.99, entry=1.0, target=2.0) is False


@pytest.mark.asyncio
async def test_save_and_get_entry_price(fake_redis):
    await cu.save_entry_price(fake_redis, "MintX", 0.42)
    val = await cu.get_entry_price(fake_redis, "MintX")
    assert val == 0.42


@pytest.mark.asyncio
async def test_send_telegram(fake_session, monkeypatch):
    # Ensure config tokens exist for URL build
    monkeypatch.setattr(cu, "TELEGRAM_BOT_TOKEN", "bot:token", raising=False)
    monkeypatch.setattr(cu, "TELEGRAM_CHAT_ID", "12345", raising=False)

    await cu.send_telegram("Hello", sol_value=1.0)
    # Should have posted once to Telegram API
    assert len(fake_session.post_calls) == 1
    assert "api.telegram.org" in fake_session.post_calls[0]["url"]


@pytest.mark.asyncio
async def test_get_jupiter_quote_handles_404(fake_session, monkeypatch):
    # Force unknown URL path so our fake returns 404
    monkeypatch.setattr(cu, "JUPITER_SWAP_API", "https://quote-api.jup.ag/v6/unknown", raising=False)
    q = await cu.get_jupiter_quote("So11111111111111111111111111111111111111112",
                                   "SomeMint", amount=1_000_000, slippage_pct=0.005)
    assert q is None


@pytest.mark.asyncio
async def test_swap_token_jupiter_early_fail_no_quote(fake_session, monkeypatch):
    # Point to a path that returns 404 so quote returns None
    monkeypatch.setattr(cu, "JUPITER_SWAP_API", "https://quote-api.jup.ag/v6/unknown", raising=False)
    ok, sig = await cu.swap_token_jupiter(
        input_mint="So11111111111111111111111111111111111111112",
        output_mint="SomeMint",
        amount_float=1.0,
        buy=True,
        slippage=0.005,
    )
    assert ok is False
    assert sig == ""


@pytest.mark.asyncio
async def test_cache_and_check_sniped_token(fake_redis, monkeypatch):
    # Use fake redis; stub sqlite connect to no-op
    class _NoopConn:
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): return False
        async def execute(self, *a, **k): return None
        async def commit(self): return None

    async def _fake_connect(_): return _NoopConn()

    monkeypatch.setattr(cu.aiosqlite, "connect", _fake_connect, raising=True)
    monkeypatch.setattr(cu, "redis", fake_redis, raising=False)

    await cu.cache_sniped_token("MintY", 0.12)
    assert await cu.was_token_sniped("MintY") is True
    assert await cu.was_token_sniped("Other") is False


@pytest.mark.asyncio
async def test_ai_fraud_score_handles_no_session(monkeypatch):
    monkeypatch.setattr(cu, "session", None, raising=False)
    score = await cu.ai_fraud_score("AnyMint")
    assert score == 0.0
