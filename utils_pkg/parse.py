"""
utils_pkg/parse.py

God-Tier Parse Helpers (Phase P3)
=================================

Purpose
-------
A tiny, dependency-free parsing layer used across scanners / routers / risk / workers.

Design goals
------------
- Zero side effects at import time
- Mypy-safe: never call float(object) / int(object) on unknown types
- Ruff-clean: tidy imports, no unused symbols
- Predictable defaults: never raises; always returns a value
- Handles common messy inputs:
  - None / empty strings
  - ints / floats / bools
  - numeric strings with commas/underscores: "12,345.6" / "12_345.6"
  - percent strings: "12%" -> 0.12
  - bytes-like -> utf-8 decode (best-effort)
  - parentheses negatives: "(123.4)" -> -123.4

Public API (stable)
-------------------
- parse_float(value: object, default: float = 0.0) -> float
- parse_int(value: object, default: int = 0) -> int
- get_str(value: object, default: str = "") -> str
- get_bool(value: object, default: bool = False) -> bool
- clamp_float(value: object, lo: float, hi: float, default: float | None = None) -> float
"""

from __future__ import annotations

import math

_TRUE = frozenset({"1", "true", "yes", "y", "on", "t"})
_FALSE = frozenset({"0", "false", "no", "n", "off", "f"})


def get_str(value: object, default: str = "") -> str:
    """
    Coerce to a trimmed string.
    - None -> default
    - bytes/bytearray/memoryview -> utf-8 decode (replace errors)
    - other -> str(value)
    Returns default if the result is empty after stripping.
    Never raises.
    """
    if value is None:
        return default

    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            b = bytes(value)
            s = b.decode("utf-8", errors="replace").strip()
            return s if s else default
        except Exception:
            return default

    try:
        s = str(value).strip()
        return s if s else default
    except Exception:
        return default


def get_bool(value: object, default: bool = False) -> bool:
    """
    Parse booleans from common forms:
      - bool -> itself
      - int/float -> non-zero (finite only)
      - strings: true/false sets (case-insensitive)
    Never raises.
    """
    if value is None:
        return bool(default)

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        try:
            f = float(value)
        except Exception:
            return bool(default)
        if not math.isfinite(f):
            return bool(default)
        return f != 0.0

    s = get_str(value, "")
    if not s:
        return bool(default)

    low = s.lower()
    if low in _TRUE:
        return True
    if low in _FALSE:
        return False

    return bool(default)


def _normalize_num_str(s: str) -> tuple[str, bool]:
    """
    Normalize a numeric string:
      - trims
      - removes commas and underscores
      - supports parentheses negatives: "(123.4)" -> "-123.4"
      - supports trailing '%' (returns is_pct=True)
    """
    s2 = s.strip()
    if not s2:
        return "", False

    # Parentheses negatives (common in finance exports)
    if len(s2) >= 2 and s2[0] == "(" and s2[-1] == ")":
        inner = s2[1:-1].strip()
        if inner:
            s2 = f"-{inner}"

    # Common separators
    s2 = s2.replace(",", "").replace("_", "")

    is_pct = s2.endswith("%")
    if is_pct:
        s2 = s2[:-1].strip()

    return s2, is_pct


def parse_float(value: object, default: float = 0.0) -> float:
    """
    Safe float parsing.

    Notes:
    - bool is NOT treated as 1.0/0.0 to avoid surprises; returns default.
    - percent strings like "12%" become 0.12.
    - NaN/Inf -> default.
    Never raises.
    """
    if value is None:
        return float(default)

    # Avoid bool -> 1.0 surprises in risk sizing / caps.
    if isinstance(value, bool):
        return float(default)

    if isinstance(value, (int, float)):
        try:
            f = float(value)
        except Exception:
            return float(default)
        return f if math.isfinite(f) else float(default)

    if isinstance(value, (bytes, bytearray, memoryview)):
        return parse_float(get_str(value, ""), default=default)

    if isinstance(value, str):
        s_norm, is_pct = _normalize_num_str(value)
        if not s_norm:
            return float(default)
        try:
            f = float(s_norm)
        except Exception:
            return float(default)
        if not math.isfinite(f):
            return float(default)
        return (f / 100.0) if is_pct else f

    # Last-resort: stringify then parse (mypy-safe, avoids float(object)).
    s = get_str(value, "")
    if not s:
        return float(default)

    s_norm, is_pct = _normalize_num_str(s)
    if not s_norm:
        return float(default)

    try:
        f = float(s_norm)
    except Exception:
        return float(default)

    if not math.isfinite(f):
        return float(default)

    return (f / 100.0) if is_pct else f


def parse_int(value: object, default: int = 0) -> int:
    """
    Safe int parsing.

    Notes:
    - bool returns default (avoid True -> 1 in env parsing)
    - string supports commas/underscores and decimal forms ("12.0")
    - percent strings like "12%" become 12
    - NaN/Inf -> default
    Never raises.
    """
    if value is None:
        return int(default)

    if isinstance(value, bool):
        return int(default)

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if not math.isfinite(value):
            return int(default)
        return int(value)

    if isinstance(value, (bytes, bytearray, memoryview)):
        return parse_int(get_str(value, ""), default=default)

    if isinstance(value, str):
        s_norm, _is_pct = _normalize_num_str(value)
        if not s_norm:
            return int(default)
        try:
            f = float(s_norm)
        except Exception:
            return int(default)
        if not math.isfinite(f):
            return int(default)
        return int(f)

    # Last-resort: stringify then parse (mypy-safe).
    s = get_str(value, "")
    if not s:
        return int(default)

    s_norm, _is_pct = _normalize_num_str(s)
    if not s_norm:
        return int(default)

    try:
        f = float(s_norm)
    except Exception:
        return int(default)

    if not math.isfinite(f):
        return int(default)

    return int(f)


def clamp_float(value: object, lo: float, hi: float, default: float | None = None) -> float:
    """
    Parse float then clamp to [lo, hi].

    If default is None, default becomes `lo` (after fixing order).
    Never raises.
    """
    if lo > hi:
        lo, hi = hi, lo

    d = lo if default is None else float(default)
    v = parse_float(value, default=d)

    if v < lo:
        return float(lo)
    if v > hi:
        return float(hi)
    return float(v)


__all__ = [
    "clamp_float",
    "get_bool",
    "get_str",
    "parse_float",
    "parse_int",
]
