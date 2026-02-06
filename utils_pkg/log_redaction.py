from __future__ import annotations

"""
utils_pkg/log_redaction.py — V3+++ (God-Tier Overhaul)
======================================================

Central, dependency-free redaction utilities to ensure logs never leak:
- DSNs / connection strings (postgres/mysql/redis/etc.)
- API keys / tokens (query params, headers, bearer tokens, JWTs, Telegram bot tokens)
- Private keys (PEM blocks), mnemonics/seed phrases when detected
- Password-like fields in dicts / JSON payloads
- "long token" heuristics (while preserving likely public Solana base58 pubkeys)

Design goals
------------
- Safe: never raises
- Deterministic: no randomness
- Low overhead: stdlib-only, compiled regexes
- Useful: preserves non-secret debugging info where safe (e.g., public Solana mints)

This module has NO side effects at import time.

IMPORTANT FIX
-------------
Cycle detection must NOT treat primitives as cycle candidates.
CPython may reuse ids for cached small ints / interned strings, causing false "<cycle>".
We only track containers / objects that can actually participate in cycles.
"""

import logging
import re
from collections.abc import Mapping, Sequence
from typing import Any

MASK = "***REDACTED***"

# -----------------------------------------------------------------------------
# Key classification
# -----------------------------------------------------------------------------
_SENSITIVE_KEY_FRAGMENTS = {
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "access_token",
    "refresh_token",
    "api_key",
    "apikey",
    "client_secret",
    "auth",
    "authorization",
    "bearer",
    "cookie",
    "set_cookie",
    "set-cookie",
    "private",
    "mnemonic",
    "seed",
    "signature",
    "sig",
    "dsn",
    "uri",
    "database_url",
    "redis_url",
    "postgres_url",
    "mysql_url",
    "url",  # URLs might contain creds; we redact text patterns regardless
    "key",
}


def is_sensitive_key(key: str) -> bool:
    """Best-effort classification of key names that should always be masked."""
    try:
        k = (key or "").strip().lower().replace("-", "_")
        if not k:
            return False
        if k in _SENSITIVE_KEY_FRAGMENTS:
            return True
        return any(frag in k for frag in _SENSITIVE_KEY_FRAGMENTS)
    except Exception:
        return False


# -----------------------------------------------------------------------------
# Regex patterns (compiled)
# -----------------------------------------------------------------------------

# URI userinfo: scheme://user:pass@host...
_DSN_CRED_RE = re.compile(
    r"(?i)\b((?:postgres(?:ql)?|mysql|mariadb|redis|rediss|mongodb(?:\+srv)?|amqp|kafka)://)"
    r"([^/@:\s]+):([^@/\s]+)@"
)

# Generic userinfo mask in urls: scheme://something@host (mask userinfo)
_URL_USERINFO_RE = re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)([^/@\s]+)@")

# Query params: ?token=...&api_key=... etc.
_QP_SECRET_RE = re.compile(
    r"(?i)([?&](?:token|access_token|refresh_token|api_key|apikey|key|secret|password|pwd|signature|sig)=)([^&\s]+)"
)

# Header-ish secrets: Authorization: Bearer xxx / x-api-key: xxx / token=xxx
_HDR_SECRET_RE = re.compile(
    r"(?i)\b(authorization|x-api-key|api-key|x_auth_token|x-token|token|bearer)\b"
    r"(\s*[:=]\s*|\s+)([^\s,;]+)"
)

# Telegram bot token: 123456789:AA... (digits ":" base64-ish)
_TELEGRAM_TOKEN_RE = re.compile(r"\b(\d{5,}):([A-Za-z0-9_-]{20,})\b")

# JWT: eyJ... . ... . ...
_JWT_RE = re.compile(r"\b(eyJ[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,}\.[a-zA-Z0-9_-]{10,})\b")

# PEM private key blocks
_PEM_PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----.*?-----END(?: [A-Z0-9]+)? PRIVATE KEY-----",
    re.DOTALL,
)

# JSON array that looks like a secret key: [12,34, ... many ints ...]
_SECRET_KEY_ARRAY_RE = re.compile(r"\[\s*\d{1,3}\s*(?:,\s*\d{1,3}\s*){30,}\]")

# Hex tokens 32+ chars (common for secrets)
_LONG_HEX_RE = re.compile(r"\b[a-fA-F0-9]{32,}\b")

# Generic long token candidate (alnum/_/-) 32+ chars
_LONG_TOKEN_RE = re.compile(r"\b[A-Za-z0-9_-]{32,}\b")

# Base58 public keys (Solana mints/wallets) are typically 32-44 chars of base58 alphabet.
# We preserve these for debugging (they are not secrets).
_BASE58_PUBKEY_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def _mask_keep_prefix(value: str, keep: int = 3) -> str:
    try:
        v = value or ""
        if keep <= 0:
            return MASK
        if len(v) <= keep:
            return MASK
        return f"{v[:keep]}…{MASK}"
    except Exception:
        return MASK


def redact_text(text: str) -> str:
    """
    Redact secrets inside arbitrary text.
    Safe: never raises.
    """
    try:
        s = "" if text is None else str(text)

        # PEM private keys
        s = _PEM_PRIVATE_KEY_RE.sub(MASK, s)

        # DSN credentials (user:pass@)
        s = _DSN_CRED_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}:{MASK}@", s)

        # URL userinfo (mask whole userinfo if present)
        s = _URL_USERINFO_RE.sub(lambda m: f"{m.group(1)}{MASK}@", s)

        # Query params
        s = _QP_SECRET_RE.sub(lambda m: f"{m.group(1)}{MASK}", s)

        # Header-like secrets
        s = _HDR_SECRET_RE.sub(lambda m: f"{m.group(1)}{m.group(2)}{MASK}", s)

        # Telegram token
        s = _TELEGRAM_TOKEN_RE.sub(lambda m: f"{m.group(1)}:{MASK}", s)

        # JWT
        s = _JWT_RE.sub(MASK, s)

        # Secret-key arrays
        s = _SECRET_KEY_ARRAY_RE.sub(MASK, s)

        # Long hex secrets
        s = _LONG_HEX_RE.sub(lambda m: _mask_keep_prefix(m.group(0), keep=4), s)

        # Generic long tokens (skip base58 pubkeys)
        def repl_long(m: re.Match[str]) -> str:
            tok = m.group(0)
            if _BASE58_PUBKEY_RE.match(tok):
                return tok
            return _mask_keep_prefix(tok, keep=3)

        s = _LONG_TOKEN_RE.sub(repl_long, s)

        return s
    except Exception:
        return MASK


def _truncate_str(s: str, max_len: int) -> str:
    try:
        if max_len <= 0:
            return ""
        if len(s) <= max_len:
            return s
        return s[: max(1, max_len - 1)] + "…"
    except Exception:
        return ""


def _is_cycle_candidate(obj: Any) -> bool:
    """
    Only containers / objects that can actually form cycles should be tracked.
    DO NOT track primitives (ints/str/bool/bytes), because CPython can reuse ids.
    """
    try:
        if obj is None:
            return False
        if isinstance(obj, (str, int, float, bool, bytes, bytearray, memoryview)):
            return False
        # Containers & common cycle-capable objects
        if isinstance(obj, (Mapping, list, tuple, set, frozenset)):
            return True
        # Objects with attributes can be cyclic via __dict__
        return hasattr(obj, "__dict__")
    except Exception:
        return False


def redact_obj(
    obj: Any,
    *,
    max_depth: int = 6,
    max_str_len: int = 4096,
    max_items: int = 200,
    _seen: set[int] | None = None,
) -> Any:
    """
    Redact secrets from nested objects (dict/list/tuple/set/etc.).

    Rules:
    - Sensitive keys => masked
    - Strings => redact_text + truncate
    - Exceptions => redact message
    - Bytes => masked
    - Large collections => truncated
    - Cycle-safe via _seen (containers only; see _is_cycle_candidate)
    Safe: never raises.
    """
    try:
        # Fast-path primitives first (prevents false cycle detection on cached objects)
        if obj is None:
            return None

        if max_depth <= 0:
            return MASK

        if isinstance(obj, str):
            return _truncate_str(redact_text(obj), max_str_len)

        if isinstance(obj, (int, float, bool)):
            return obj

        if isinstance(obj, (bytes, bytearray, memoryview)):
            return MASK

        if isinstance(obj, BaseException):
            return _truncate_str(redact_text(f"{type(obj).__name__}: {obj}"), max_str_len)

        if _seen is None:
            _seen = set()

        # Cycle detection (containers/objects only)
        if _is_cycle_candidate(obj):
            oid = id(obj)
            if oid in _seen:
                return "<cycle>"
            _seen.add(oid)

        # Mapping
        if isinstance(obj, Mapping):
            out: dict[str, Any] = {}
            n = 0
            for k, v in obj.items():
                n += 1
                if n > max_items:
                    out["…"] = f"truncated_after_{max_items}"
                    break
                ks = str(k)
                if is_sensitive_key(ks):
                    out[ks] = MASK
                else:
                    out[ks] = redact_obj(
                        v,
                        max_depth=max_depth - 1,
                        max_str_len=max_str_len,
                        max_items=max_items,
                        _seen=_seen,
                    )
            return out

        # Sequence (but not strings/bytes which were handled above)
        if isinstance(obj, Sequence) and not isinstance(obj, (str, bytes, bytearray)):
            out_list: list[Any] = []
            for i, v in enumerate(obj):
                if i >= max_items:
                    out_list.append(f"…truncated_after_{max_items}")
                    break
                out_list.append(
                    redact_obj(
                        v,
                        max_depth=max_depth - 1,
                        max_str_len=max_str_len,
                        max_items=max_items,
                        _seen=_seen,
                    )
                )
            return out_list

        # Sets (not Sequences)
        if isinstance(obj, (set, frozenset)):
            out_set: list[Any] = []
            for i, v in enumerate(list(obj)):
                if i >= max_items:
                    out_set.append(f"…truncated_after_{max_items}")
                    break
                out_set.append(
                    redact_obj(
                        v,
                        max_depth=max_depth - 1,
                        max_str_len=max_str_len,
                        max_items=max_items,
                        _seen=_seen,
                    )
                )
            return out_set

        # Object with attributes: redact a bounded view of __dict__
        if hasattr(obj, "__dict__"):
            d = getattr(obj, "__dict__", None)
            if isinstance(d, dict):
                # Keep it bounded and redacted
                return {
                    "__type__": type(obj).__name__,
                    "__dict__": redact_obj(
                        d,
                        max_depth=max_depth - 1,
                        max_str_len=max_str_len,
                        max_items=max_items,
                        _seen=_seen,
                    ),
                }

        # Fallback: string repr
        return _truncate_str(redact_text(str(obj)), max_str_len)
    except Exception:
        return MASK


def safe_repr(obj: Any, *, max_len: int = 2048) -> str:
    """
    Redacted, truncated string representation suitable for logs.
    Safe: never raises.
    """
    try:
        s = str(redact_obj(obj, max_str_len=max_len))
        return _truncate_str(s, max_len)
    except Exception:
        return MASK


# -----------------------------------------------------------------------------
# Logging integration (optional)
# -----------------------------------------------------------------------------
_STD_RECORD_ATTRS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}


def redact_record_extras(record: logging.LogRecord) -> None:
    """
    Best-effort redaction of LogRecord extra attributes in-place.
    Safe: never raises.
    """
    try:
        d = getattr(record, "__dict__", {})
        for k, v in list(d.items()):
            if k in _STD_RECORD_ATTRS:
                continue
            d[k] = redact_obj(v)
    except Exception:
        return


class RedactionFilter(logging.Filter):
    """
    Filter that redacts formatted log message AND record extras.
    Note: exception formatting is formatter-dependent; use RedactingFormatter or a
    redacting JSON formatter for best results.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            # Redact extras first (so formatters that include extras are safe)
            redact_record_extras(record)

            # Redact the message
            msg = record.getMessage()
            record.msg = redact_text(msg)
            record.args = ()
        except Exception:
            return True
        return True


class RedactingFormatter(logging.Formatter):
    """
    Formatter that redacts exception text as well as the main message.
    """

    def format(self, record: logging.LogRecord) -> str:
        try:
            # Ensure extras are redacted even if filter wasn't installed.
            redact_record_extras(record)
            out = super().format(record)
            return redact_text(out)
        except Exception:
            return MASK

    def formatException(self, ei) -> str:  # noqa: N802
        try:
            raw = super().formatException(ei)
            return redact_text(raw)
        except Exception:
            return MASK


def install_redaction_filter(logger: logging.Logger | None = None) -> None:
    """
    Install RedactionFilter on the given logger (default: root).
    Safe: never raises.
    """
    try:
        target = logger if logger is not None else logging.getLogger()
        for f in getattr(target, "filters", []):
            if isinstance(f, RedactionFilter):
                return
        target.addFilter(RedactionFilter())
    except Exception:
        return


__all__ = [
    "MASK",
    "RedactingFormatter",
    "RedactionFilter",
    "install_redaction_filter",
    "is_sensitive_key",
    "redact_obj",
    "redact_record_extras",
    "redact_text",
    "safe_repr",
]
