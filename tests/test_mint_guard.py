import types
from utils_pkg import mint_guard as mg

def test_probable_mint_regex_only_when_router_missing(monkeypatch):
    # Simulate missing router validator → fallback regex
    monkeypatch.setattr(mg, "_router_is_valid", None, raising=False)
    assert mg.is_probable_mint("11111111111111111111111111111111") is True  # 32 x '1' matches shape
    assert mg.is_probable_mint("bad!") is False

def test_scan_and_normalize_suffixed(monkeypatch):
    # Stub validator to treat any 43/44 as 'valid'
    monkeypatch.setattr(mg, "_is_valid_pk", lambda s: 32 <= len(s) <= 44, raising=True)
    base = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdEFGhijk"  # len 44
    raw = base + "pump"
    assert mg.normalize_from_any(raw) == base

def test_ambiguous_raises(monkeypatch):
    monkeypatch.setattr(mg, "_is_valid_pk", lambda s: True, raising=True)
    raw = "xxx ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcd yyy " \
          "zzzz 123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdEFGhijk"
    try:
        mg.normalize_from_any(raw)
        assert False, "should have raised"
    except ValueError as e:
        assert "ambiguous" in str(e)

def test_keep_plain_valid(monkeypatch):
    monkeypatch.setattr(mg, "_is_valid_pk", lambda s: True, raising=True)
    base = "KLmnopqrstuvwXYZaBCDEFGHJKLMNPQrstuvwxYz1234"
    assert mg.normalize_from_any(base) == base
