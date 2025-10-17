import asyncio
from scripts import main as M
import datetime as _dt

def test_approx_minutes_since_number_seconds():
    now = _dt.datetime.now(_dt.timezone.utc).timestamp()
    rv = M._approx_minutes_since(now)
    assert rv is not None and rv < 1.0

def test_approx_minutes_since_millis():
    now_ms = int(_dt.datetime.now(_dt.timezone.utc).timestamp() * 1000)
    rv = M._approx_minutes_since(now_ms)
    assert rv is not None and rv < 1.0

def test_normalize_from_source_basic():
    assert isinstance(M._normalize_from_source("  abc "), str)

def test_is_probable_mint_fake():
    # should be False for empty or nonsense
    assert M._is_probable_mint("") is False

def test_passes_basic_filters_min_age():
    # craft a minimal event too old
    ev = {"pool": "pump", "created_at": "2000-01-01T00:00:00Z", "txType": "create", "initialBuy": 1000}
    # ensure ENTRY_MAX_AGE_MIN small so check triggers
    prev = M.ENTRY_MAX_AGE_MIN
    try:
        M.ENTRY_MAX_AGE_MIN = 1
        assert M._passes_basic_filters(ev) is False
    finally:
        M.ENTRY_MAX_AGE_MIN = prev
