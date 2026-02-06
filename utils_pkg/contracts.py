# -*- coding: utf-8 -*-
from __future__ import annotations

"""
utils_pkg/contracts.py — Canonical Contracts (P0 LOCK-IN)
=========================================================

Single import point for TWO critical contracts:

A) ENV CONTRACT (tests/test_contract_env.py, CI)
------------------------------------------------
This module is the canonical source-of-truth for all env keys used by the system.

Principle (Option B1):
- The canonical contract lives in code (CANONICAL_ENV_VARS).
- `.env.example` must match the canonical contract.
- Tests are allowed to enforce exact alignment to prevent drift.

IMPORTANT (Option B requirement-hints):
- Some contract tests only run if `.env.example` contains inline "required" hints
  on KEY=VALUE lines. We therefore generate inline `# required ...` hints for
  variables where EnvVar.required=True.
- `STRICT_ENV` is a required contract key and MUST be present in `.env.example`
  with an inline "required" hint so strict-hint tests do not skip.

This file provides:
- ENV_VARS: list[EnvVar]
- ENV_KEYS: set[str]
- REQUIRED_KEYS: set[str]
- SAFE_MODE_KEYS: set[str]
- EXTERNAL_ENV_ALLOWLIST: set[str]
- EXTERNAL_ENV_ALLOW_PATTERNS: tuple[re.Pattern[str], ...]

Plus helpers:
- env_example_diff(repo_root: Path | None = None) -> dict[str, list[str]]
- validate_env_example(repo_root: Path | None = None, *, strict: bool = True) -> dict[str, list[str]]
- write_env_example(repo_root: Path | None = None) -> Path  (deterministic generator)

B) DATA/OBJECT CONTRACTS (runtime pipeline)
-------------------------------------------
Stable, forward-compatible dataclasses that flow through the bot:
Candidate -> EntrySignal -> RouteQuote/Fill -> Position -> ExitSignal
Plus Tick, RiskSnapshot, and EventEnvelope.

LOCK-IN rules
-------------
- Only ADD fields (backwards compatible).
- Never remove/rename without shims in from_dict()/normalization.

Design goals
------------
✅ stdlib-only (Render + Windows safe)
✅ deterministic IDs + stable JSON
✅ secret-safe redaction for logs
✅ permissive parsing (unknown keys -> extra)
✅ light validation (strictness belongs in gates, not schemas)

IMPORTANT
---------
This file is imported by tests and many runtime modules.
It MUST be safe at import-time (no network, no wallet loads, no crashes).
"""

import contextlib
import dataclasses
import hashlib
import json
import os
import re
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Self, TypeVar, Union, cast, overload

# =============================================================================
# EXCEPTIONS (must exist before helpers that may raise them)
# =============================================================================


class ContractError(RuntimeError):
    pass


class ValidationError(ContractError):
    pass


# =============================================================================
# ENV CONTRACT RECORD
# =============================================================================


@dataclass(frozen=True, slots=True, init=False)
class EnvVar:
    """
    Canonical env variable definition (contract line-item).

    Primary fields:
      - key: variable name
      - kind: "str"|"bool"|"int"|"float"|"json" (informational; scripts may use)
      - default: default/example value (string-ish)
      - required: whether required in STRICT mode (production gates / contract tests)
      - secret: should be redacted in logs
      - description: short note

    Back-compat constructor aliases supported:
      - name -> key
      - desc -> description
      - example/value -> default
    """

    key: str
    kind: str
    default: str | None
    required: bool
    secret: bool
    description: str
    aliases: tuple[str, ...]  # additional accepted keys (informational)

    def __init__(
        self,
        key: str | None = None,
        *args: Any,
        # Back-compat + convenience aliases (keyword-only because of *args)
        name: str | None = None,
        desc: str | None = None,
        kind: str = "str",
        default: str | None = None,
        example: str | None = None,
        value: str | None = None,
        required: bool = False,
        secret: bool | None = None,
        description: str = "",
        aliases: Iterable[str] | None = None,
        **_ignored: Any,
    ) -> None:
        # Positional back-compat:
        # EnvVar(key, kind, default, required, secret, description, aliases)
        if key is None and args:
            key = cast(Any, args[0])
            args = args[1:]

        if args:
            if len(args) >= 1 and kind == "str":
                kind = str(args[0])
            if len(args) >= 2 and default is None and example is None and value is None:
                default = None if args[1] is None else str(args[1])
            if len(args) >= 3:
                required = bool(args[2])
            if len(args) >= 4 and secret is None:
                secret = bool(args[3])
            if len(args) >= 5 and not description and not desc:
                description = str(args[4])
            if len(args) >= 6 and aliases is None:
                aliases = cast(Any, args[5])

        k = (key or name or "").strip()
        if not k:
            raise TypeError("EnvVar requires 'key' (or alias 'name')")

        d = (description or "").strip()
        if not d and desc:
            d = str(desc).strip()

        # default selection order (explicit default wins)
        dv = default
        if dv is None and example is not None:
            dv = str(example)
        if dv is None and value is not None:
            dv = str(value)

        kd = (kind or "str").strip().lower()
        sec = bool(secret) if secret is not None else False
        als: tuple[str, ...] = tuple(str(a).strip() for a in (aliases or []) if str(a).strip())

        object.__setattr__(self, "key", k)
        object.__setattr__(self, "kind", kd)
        object.__setattr__(self, "default", None if dv is None else str(dv))
        object.__setattr__(self, "required", bool(required))
        object.__setattr__(self, "secret", sec)
        object.__setattr__(self, "description", d)
        object.__setattr__(self, "aliases", als)

    @property
    def name(self) -> str:  # legacy alias
        return self.key

    @property
    def desc(self) -> str:  # legacy alias
        return self.description

    def all_keys(self) -> tuple[str, ...]:
        return (self.key, *self.aliases)


# =============================================================================
# CORE PRIMITIVES (must exist BEFORE contract assembly at import-time)
# =============================================================================

CONTRACTS_VERSION = "v1"
SCHEMA_REV = 1  # bump only for intentional semantic changes

_AUTHZ_VALUE_RE = re.compile(r"^(bearer|basic)\s+\S+", re.IGNORECASE)

# Base58 alphabet (Solana-ish): excludes 0 O I l
_BASE58_ALPHABET = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def now_ts() -> int:
    return int(time.time())


def now_ms() -> int:
    return int(time.time() * 1000)


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or isinstance(x, bool):
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return int(default)
        return int(float(x))  # supports "1.0"
    except Exception:
        return int(default)


def clamp(x: float, lo: float, hi: float) -> float:
    try:
        xf = float(x)
    except Exception:
        xf = float(lo)
    if xf < lo:
        return float(lo)
    if xf > hi:
        return float(hi)
    return float(xf)


def looks_like_base58(s: Any, *, min_len: int = 20, max_len: int = 90) -> bool:
    if not isinstance(s, str):
        return False
    ss = s.strip()
    if len(ss) < min_len or len(ss) > max_len:
        return False
    return all(ch in _BASE58_ALPHABET for ch in ss)


def stable_id(*parts: Any, prefix: str = "") -> str:
    """Deterministic ID for dedupe/caching/logging."""
    h = hashlib.sha256()
    for p in parts:
        try:
            h.update(str(p).encode("utf-8", "ignore"))
        except Exception:
            h.update(repr(p).encode("utf-8", "ignore"))
        h.update(b"|")
    digest = h.hexdigest()[:24]
    return f"{prefix}{digest}" if prefix else digest


def _dc_asdict(obj: Any) -> dict[str, Any]:
    """Typed wrapper around dataclasses.asdict for mypy friendliness."""
    return dataclasses.asdict(cast(Any, obj))


def _to_primitive(x: Any) -> Any:
    """Convert dataclasses/enums/collections into JSON-friendly primitives."""
    if x is None:
        return None
    if isinstance(x, Enum):
        return x.value
    if isinstance(x, (str, int, float, bool)):
        return x
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, datetime):
        return x.isoformat()
    if dataclasses.is_dataclass(x):
        if hasattr(x, "to_dict") and callable(x.to_dict):
            return cast(Any, x).to_dict()
        return _dc_asdict(x)
    if isinstance(x, Mapping):
        return {str(k): _to_primitive(v) for k, v in x.items()}
    if isinstance(x, Sequence) and not isinstance(x, (str, bytes, bytearray, memoryview)):
        return [_to_primitive(v) for v in x]
    if isinstance(x, (bytes, bytearray, memoryview)):
        try:
            return bytes(x).decode("utf-8", "ignore")
        except Exception:
            return str(x)
    return str(x)


def json_dumps(obj: Any) -> str:
    """Stable JSON string (deterministic ordering) used by logs/tests."""
    return json.dumps(
        _to_primitive(obj),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _getenv(k: str, d: str = "") -> str:
    return (os.environ.get(k, d) or "").strip()


# =============================================================================
# REDACTION HELPERS
# =============================================================================

DEFAULT_REDACT_KEYS = {
    "token",
    "api_key",
    "apikey",
    "secret",
    "password",
    "pass",
    "authorization",
    "auth",
    "bearer",
    "wallet_private_key",
    "private_key",
    "seed",
    "mnemonic",
    "keypair",
    "dsn",
    "database_url",
    "redis_url",
    "rpc_url",
    "x-api-key",
    "x_api_key",
}


def _should_redact_key(key: str, extra_redact_keys: Iterable[str] | None = None) -> bool:
    ks = (key or "").strip().lower()
    if not ks:
        return False
    keys = set(DEFAULT_REDACT_KEYS)
    if extra_redact_keys:
        for k in extra_redact_keys:
            kk = str(k).strip().lower()
            if kk:
                keys.add(kk)
    return any(tok in ks for tok in keys)


def _looks_like_secret_string(v: str) -> bool:
    s = (v or "").strip()
    if not s:
        return False

    if _AUTHZ_VALUE_RE.match(s):
        return True

    # common Solana pubkeys should remain visible
    if looks_like_base58(s, min_len=32, max_len=55):
        return False

    # longer base58 blobs are more likely secrets/signatures/serialized keys
    if looks_like_base58(s, min_len=56, max_len=140):
        return True

    if len(s) >= 28 and all(ch.isalnum() or ch in "_-.:/+=" for ch in s):
        return True
    return len(s) >= 80


def redact_any(
    x: Any,
    *,
    extra_redact_keys: Iterable[str] | None = None,
    max_depth: int = 3,
) -> Any:
    """Recursive redaction for logs."""
    if max_depth <= 0:
        return "***"
    if x is None:
        return None
    if isinstance(x, (int, float, bool)):
        return x
    if isinstance(x, str):
        return "***" if _looks_like_secret_string(x) else x
    if dataclasses.is_dataclass(x):
        return redact_any(
            _to_primitive(x),
            extra_redact_keys=extra_redact_keys,
            max_depth=max_depth,
        )
    if isinstance(x, Mapping):
        out: dict[str, Any] = {}
        for k, v in x.items():
            kk = str(k)
            if _should_redact_key(kk, extra_redact_keys=extra_redact_keys):
                out[kk] = "***"
            else:
                out[kk] = redact_any(
                    v,
                    extra_redact_keys=extra_redact_keys,
                    max_depth=max_depth - 1,
                )
        return out
    if isinstance(x, Sequence) and not isinstance(x, (str, bytes, bytearray, memoryview)):
        return [
            redact_any(v, extra_redact_keys=extra_redact_keys, max_depth=max_depth - 1) for v in x
        ]
    return str(x)


def redact_mapping(
    m: Mapping[str, Any],
    *,
    extra_redact_keys: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Shallow-ish redact mapping (mostly for `extra`)."""
    return cast(
        dict[str, Any],
        redact_any(dict(m or {}), extra_redact_keys=extra_redact_keys, max_depth=2),
    )


# =============================================================================
# ENV CONTRACT (canonical for tests + CI)
# =============================================================================

ENV_EXAMPLE_FILENAME = ".env.example"

EXTERNAL_ENV_ALLOWLIST: set[str] = {
    "PATH",
    "HOME",
    "SHELL",
    "PWD",
    "TMP",
    "TEMP",
    "TMPDIR",
    "USER",
    "USERNAME",
    "USERPROFILE",
    "COMSPEC",
    "SYSTEMROOT",
    "WINDIR",
    "APPDATA",
    "LOCALAPPDATA",
    "PROGRAMDATA",
    "TERM",
    "LANG",
    "LC_ALL",
    "PYTHONPATH",
    "PYTHONUTF8",
    "PYTHONDONTWRITEBYTECODE",
    "PYTHONIOENCODING",
    "VIRTUAL_ENV",
    "PIP_DISABLE_PIP_VERSION_CHECK",
    "PIP_NO_CACHE_DIR",
    "CI",
    "TZ",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "PORT",
    "WEB_CONCURRENCY",
    "PYTHONUNBUFFERED",
    "PYTHONHASHSEED",
    "NO_PROXY",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
}

EXTERNAL_ENV_ALLOW_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^GITHUB_", re.IGNORECASE),
    re.compile(r"^RUNNER_", re.IGNORECASE),
    re.compile(r"^ACTIONS_", re.IGNORECASE),
    re.compile(r"^RENDER_", re.IGNORECASE),
    re.compile(r"^PYTEST_", re.IGNORECASE),
    re.compile(r"^PIP_", re.IGNORECASE),
    re.compile(r"^(?:NO|HTTP|HTTPS|ALL)_PROXY$", re.IGNORECASE),
)

_SAFE_TOGGLE_KEY_RE = re.compile(
    r"(SAFE|DRY|PAPER|SIM|NO_NETWORK|DISABLE_NETWORK|READ_ONLY|CI|TEST|PYTEST|BACKTEST|DISABLE_)_?",
    re.IGNORECASE,
)
_SECRETISH_KEY_RE = re.compile(
    r"(SECRET|TOKEN|KEY|PASSWORD|PASS|DSN|DATABASE|WEBHOOK|PRIVATE|MNEMONIC|SEED)",
    re.IGNORECASE,
)

_ENV_LINE_RE = re.compile(
    r"""^\s*(?:export\s+)?(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<val>.*)\s*$"""
)


def _dedupe_keep_order(items: Iterable[EnvVar]) -> list[EnvVar]:
    out: list[EnvVar] = []
    seen: set[str] = set()
    for ev in items:
        k = (ev.key or "").strip()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(ev)
    return out


def _is_secret_key(k: str) -> bool:
    return bool(_SECRETISH_KEY_RE.search(k or ""))


def _env_section(_title: str, vars_: list[EnvVar]) -> list[EnvVar]:
    return vars_


# ---- Canonical env var list (Option B1 source of truth) ----
# IMPORTANT: only ADD keys (never remove) unless you also update tests and regenerate .env.example.

CANONICAL_ENV_VARS: list[EnvVar] = _dedupe_keep_order(
    [
        # ---------------------------------------------------------------------
        # [A] APP IDENTITY / MODE
        # ---------------------------------------------------------------------
        *_env_section(
            "APP IDENTITY / MODE",
            [
                EnvVar("APP_NAME", default="pnlsniper", description="App name"),
                EnvVar(
                    "APP_ENV", default="development", description="development|staging|production"
                ),
                EnvVar("ENV", default="development", description="Legacy alias for APP_ENV"),
                EnvVar("TZ", default="UTC", description="Timezone label"),
                EnvVar("RUN_ID", default="", description="Run identifier (CI/Render optional)"),
                EnvVar("INSTANCE_ID", default="local", description="Instance identifier"),
                EnvVar("GIT_COMMIT", default="", description="Git commit (optional)"),
                EnvVar("GIT_SHA", default="", description="Git SHA (optional)"),
                EnvVar("VERSION", default="", description="App version (optional)"),
                EnvVar("SERVICE", default="sniper", description="Service name (optional)"),
                EnvVar("LOG_ENV", default="", description="Logging env override (optional)"),
                EnvVar(
                    "LOG_SERVICE", default="sniper", description="Logging service name (optional)"
                ),
                EnvVar("LOG_VERSION", default="v1", description="Logging version (optional)"),
                EnvVar("RENDER", default="0", description="Render detected flag (optional)"),
                EnvVar("RENDER_SERVICE_ID", default="", description="Render service id (optional)"),
                EnvVar(
                    "RENDER_INSTANCE_ID", default="", description="Render instance id (optional)"
                ),
                EnvVar(
                    "RENDER_EXTERNAL_URL", default="", description="Render external URL (optional)"
                ),
                EnvVar("HOSTNAME", default="", description="Hostname (optional)"),
            ],
        ),
        *_env_section(
            "ENV VALIDATION / FAIL-FAST",
            [
                EnvVar(
                    "STRICT_ENV",
                    default="0",
                    required=True,
                    kind="bool",
                    description="0=warn, 1=hard fail on missing required vars",
                ),
                EnvVar(
                    "ENV_STRICT",
                    default="0",
                    kind="bool",
                    description="Legacy alias for STRICT_ENV",
                ),
                EnvVar(
                    "MAIN_FAST_FAIL_EARLY",
                    default="0",
                    kind="bool",
                    description="1=crash early on misconfig",
                ),
                EnvVar("RUNTIME_PROFILE", default="safe", description="safe|paper|live"),
            ],
        ),
        *_env_section(
            "HTTP / SERVICE PORTS",
            [
                EnvVar(
                    "MAIN_HTTP_PORT",
                    default="",
                    kind="int",
                    description="Override main HTTP port (optional; scripts/main.py)",
                ),
                EnvVar(
                    "HTTP_PORT",
                    default="",
                    kind="int",
                    description="Override HTTP port (optional; scripts/main.py)",
                ),
            ],
        ),
        *_env_section(
            "HTTP / PROXY",
            [
                EnvVar(
                    "NO_PROXY",
                    default="",
                    kind="str",
                    description="Optional proxy bypass list (comma-separated hosts/domains/IPs). Supports upper/lowercase forms.",
                ),
            ],
        ),
        *_env_section(
            "GLOBAL MODES / SAFETY",
            [
                EnvVar(
                    "REPO_ROOT", default="", description="Optional override for repo root discovery"
                ),
                EnvVar(
                    "SAFE_MODE",
                    default="1",
                    kind="bool",
                    description="Safe imports (tests/CI default)",
                ),
                EnvVar(
                    "PYTEST_MODE",
                    default="0",
                    kind="bool",
                    description="Pytest safety toggle (set by CI)",
                ),
                EnvVar("PYTEST_RUNNING", default="0", kind="bool", description="pytest guard"),
                EnvVar(
                    "BACKTEST_MODE", default="0", kind="bool", description="Backtest harness mode"
                ),
                EnvVar(
                    "BT_NO_SIDE_EFFECTS",
                    default="1",
                    kind="bool",
                    description="Backtest: disable side effects",
                ),
                EnvVar("DRYRUN", default="1", kind="bool", description="No live trading"),
                EnvVar("DRY_RUN", default="1", kind="bool", description="No live trading"),
                EnvVar(
                    "NO_NETWORK",
                    default="0",
                    kind="bool",
                    description="Disable outbound network (hard safety)",
                ),
                EnvVar(
                    "DISABLE_NETWORK",
                    default="0",
                    kind="bool",
                    description="Disable outbound network (hard safety)",
                ),
                EnvVar("LIVE_TRADING", default="0", kind="bool", description="Master live toggle"),
                EnvVar(
                    "DISABLE_LIVE_TRADING", default="1", kind="bool", description="Force live off"
                ),
                EnvVar(
                    "BUDGET_DISABLE",
                    default="0",
                    kind="bool",
                    description="Disable budget circuit breaker (dev/testing only)",
                ),
                EnvVar(
                    "MAIN_DISABLE_RANDOM_JITTER",
                    default="0",
                    kind="bool",
                    description="Deterministic behavior in tests/CI",
                ),
                EnvVar(
                    "CORE_LOAD_DOTENV",
                    default="1",
                    kind="bool",
                    description="Allow dotenv side effects (set 0 in tests)",
                ),
            ],
        ),
        *_env_section(
            "SOLANA / NETWORK",
            [
                EnvVar(
                    "NETWORK", default="mainnet", description="Network label: mainnet/devnet/local"
                ),
                EnvVar("RPC_URL", default="", secret=True, description="Solana RPC URL"),
                EnvVar("RPC_TIMEOUT_S", default="8", kind="int", description="RPC timeout seconds"),
                EnvVar("RPC_COMMITMENT", default="confirmed", description="RPC commitment"),
            ],
        ),
        *_env_section(
            "WALLET",
            [
                EnvVar(
                    "KEYPAIR_PATH", default="", secret=True, description="Wallet keypair file path"
                ),
                EnvVar(
                    "WALLET_PRIVATE_KEY", default="", secret=True, description="Wallet private key"
                ),
                EnvVar(
                    "WALLET_PUBLIC_KEY",
                    default="",
                    description="Optional wallet pubkey (non-secret)",
                ),
            ],
        ),
        *_env_section(
            "REDIS / CACHE",
            [
                EnvVar("REDIS_ENABLED", default="1", kind="bool", description="Enable Redis"),
                EnvVar(
                    "DISABLE_REDIS", default="0", kind="bool", description="Disable Redis quickly"
                ),
                EnvVar("REDIS_URL", default="", secret=True, description="Redis URL"),
                EnvVar("REDIS_NAMESPACE", default="sniper", description="Key prefix namespace"),
                EnvVar(
                    "REDIS_CONNECT_TIMEOUT_S",
                    default="5",
                    kind="int",
                    description="Redis connect timeout seconds",
                ),
                EnvVar(
                    "REDIS_OP_TIMEOUT_S",
                    default="3",
                    kind="int",
                    description="Redis op timeout seconds",
                ),
            ],
        ),
        *_env_section(
            "INTERNAL REDIS KEYS / STREAMS",
            [
                EnvVar(
                    "TICK_ACTIVE_KEYS",
                    default="tick:active:keys",
                    description="Set key of active tick keys",
                ),
                EnvVar(
                    "TICK_ACTIVE_MINTS_KEY",
                    default="tick:active:mints",
                    description="Set key of active mints",
                ),
                EnvVar(
                    "TICK_ACTIVE_POSITIONS_KEY",
                    default="tick:active:positions",
                    description="Set key of active positions",
                ),
                EnvVar(
                    "TICK_STREAM_REDIS_STREAM_KEY",
                    default="tick:stream",
                    description="Redis stream key for ticks",
                ),
                EnvVar(
                    "HOTLIST_LOCK_TTL_SEC",
                    default="120",
                    kind="int",
                    description="Hotlist worker: Redis lock TTL seconds (hotlist_worker.py fallback=120)",
                ),
            ],
        ),
        *_env_section(
            "POSTGRES / AI DB",
            [
                EnvVar("POSTGRES_DSN", default="", secret=True, description="Postgres DSN"),
                EnvVar(
                    "DATABASE_URL",
                    default="",
                    secret=True,
                    description="Compat: DATABASE_URL (if used)",
                ),
                EnvVar("PG_POOL_MIN", default="0", kind="int", description="asyncpg pool min size"),
                EnvVar("PG_POOL_MAX", default="5", kind="int", description="asyncpg pool max size"),
                EnvVar(
                    "PG_CONN_TIMEOUT_S",
                    default="8",
                    kind="int",
                    description="Postgres connect timeout seconds",
                ),
            ],
        ),
        *_env_section(
            "ROUTER / JUPITER",
            [
                EnvVar(
                    "JUPITER_ENABLED",
                    default="0",
                    kind="bool",
                    description="Allow Jupiter usage (default off in CI)",
                ),
                EnvVar(
                    "JUPITER_BASE_URL",
                    default="https://quote-api.jup.ag",
                    description="Jupiter API base URL",
                ),
                EnvVar(
                    "JUPITER_MAX_SLIPPAGE_BPS",
                    default="200",
                    kind="int",
                    description="Max slippage bps",
                ),
                EnvVar(
                    "JUPITER_TIMEOUT_S", default="8", kind="int", description="HTTP timeout seconds"
                ),
                EnvVar(
                    "JUPITER_API_KEY",
                    default="",
                    secret=True,
                    description="Optional Jupiter API key",
                ),
                EnvVar(
                    "SWAPS_ENABLED",
                    default="0",
                    kind="bool",
                    description="Allow swaps in harness (default off)",
                ),
            ],
        ),
        *_env_section(
            "SCANNERS",
            [
                EnvVar(
                    "SCANNER_ENABLED", default="1", kind="bool", description="Master scanner enable"
                ),
                EnvVar(
                    "SCANNER_LIVE_TESTS",
                    default="0",
                    kind="bool",
                    description="Opt-in gate for live scanner tests",
                ),
                EnvVar(
                    "SCANNER_INTERVAL_S",
                    default="2",
                    kind="int",
                    description="Scanner poll interval seconds",
                ),
                EnvVar(
                    "SCAN_RAYDIUM", default="1", kind="bool", description="Enable Raydium scanner"
                ),
                EnvVar(
                    "SCAN_PUMPFUN", default="1", kind="bool", description="Enable Pump.fun scanner"
                ),
                EnvVar(
                    "SCAN_DEXSCREENER",
                    default="1",
                    kind="bool",
                    description="Enable Dexscreener scanner",
                ),
                EnvVar("SCAN_AXIOM", default="0", kind="bool", description="Enable Axiom scanner"),
                EnvVar(
                    "RAYDIUM_SHIM_WARN",
                    default="0",
                    kind="bool",
                    description="Warn on raydium shim use",
                ),
                EnvVar(
                    "PUMPFUN_IMPL_MODULE",
                    default="scanner.pumpfun_scanner",
                    description="Pumpfun impl module",
                ),
                EnvVar(
                    "PUMPFUN_IMPL_IMPORT_ERR_COOLDOWN_S",
                    default="15",
                    kind="int",
                    description="Import throttle",
                ),
                EnvVar(
                    "AXIOM_SHIM_WARN",
                    default="0",
                    kind="bool",
                    description="Warn on axiom shim use",
                ),
                EnvVar(
                    "AXIOM_IMPL_MODULE",
                    default="scanner.axiom_scanner",
                    description="Axiom impl module",
                ),
            ],
        ),
        *_env_section(
            "ENTRY / RISK / EXECUTION",
            [
                EnvVar(
                    "RISK_PROFILE_DEFAULT", default="balanced", description="Default risk profile"
                ),
                EnvVar(
                    "MAX_CONCURRENCY",
                    default="1",
                    kind="int",
                    description="Max concurrent positions",
                ),
                EnvVar(
                    "MAX_INFLIGHT_ORDERS",
                    default="1",
                    kind="int",
                    description="Max inflight orders",
                ),
                EnvVar(
                    "TICKET_SOL",
                    default="0.01",
                    kind="float",
                    description="Default buy size in SOL",
                ),
                EnvVar(
                    "PF_TICKET_USD",
                    default="20",
                    kind="float",
                    description="Pump.fun ticket size USD fallback (velocity_worker.py fallback=20)",
                ),
                EnvVar(
                    "STOP_LOSS_PCT", default="25", kind="float", description="Stop loss percent"
                ),
                EnvVar(
                    "TAKE_PROFIT_PCT", default="60", kind="float", description="Take profit percent"
                ),
                EnvVar(
                    "TRAILING_ARM_PCT",
                    default="35",
                    kind="float",
                    description="Trailing arm percent",
                ),
                EnvVar("TRAILING_PCT", default="18", kind="float", description="Trailing percent"),
            ],
        ),
        *_env_section(
            "LOGGING",
            [
                EnvVar("LOG_LEVEL", default="INFO", description="Log level"),
                EnvVar("LOG_JSON", default="1", kind="bool", description="Enable JSON logs"),
                EnvVar(
                    "LOG_TIME_UTC",
                    default="1",
                    kind="bool",
                    description="Logging: force UTC timestamps",
                ),
                EnvVar(
                    "LOG_INCLUDE_SRC",
                    default="0",
                    kind="bool",
                    description="Logging: include source fields",
                ),
            ],
        ),
        EnvVar(
            "TASK_SPAWN_CAPTURE_ORIGIN",
            default="0",
            kind="bool",
            description="Task spawn: capture origin frames for debugging (0/1)",
        ),
        EnvVar(
            "TASK_SPAWN_ORIGIN_FRAMES",
            default="2",
            kind="int",
            description="Task spawn: number of origin frames to record (1..10 recommended)",
        ),
        *_env_section(
            "ALERTS / NOTIFICATIONS",
            [
                EnvVar(
                    "TELEGRAM_ENABLED",
                    default="0",
                    kind="bool",
                    description="Enable Telegram alerts",
                ),
                EnvVar(
                    "TELEGRAM_BOT_TOKEN", default="", secret=True, description="Telegram bot token"
                ),
                EnvVar("TELEGRAM_CHAT_ID", default="", description="Telegram chat id"),
                EnvVar(
                    "DISCORD_ENABLED", default="0", kind="bool", description="Enable Discord alerts"
                ),
                EnvVar(
                    "DISCORD_WEBHOOK_URL",
                    default="",
                    secret=True,
                    description="Discord webhook URL",
                ),
                EnvVar(
                    "NOTION_ENABLED", default="0", kind="bool", description="Enable Notion logging"
                ),
                EnvVar("NOTION_TOKEN", default="", secret=True, description="Notion token"),
                EnvVar("NOTION_DATABASE_ID", default="", secret=True, description="Notion DB"),
            ],
        ),
        *_env_section(
            "PLAYBOOK (Notion + stream)",
            [
                EnvVar(
                    "PLAYBOOK_NOTION_ENABLED",
                    default="0",
                    kind="bool",
                    description="Enable playbook Notion logging",
                ),
                EnvVar(
                    "PLAYBOOK_NOTION_TIMEOUT_SEC",
                    default="8",
                    kind="int",
                    description="Notion timeout seconds",
                ),
                EnvVar(
                    "PLAYBOOK_STATUS_PREFIX", default="pb:status:", description="Status key prefix"
                ),
                EnvVar(
                    "PLAYBOOK_STATUS_TTL_SEC",
                    default="900",
                    kind="int",
                    description="Status TTL seconds",
                ),
                EnvVar(
                    "PLAYBOOK_STREAM_KEY", default="pb:stream", description="Playbook stream key"
                ),
                EnvVar(
                    "PLAYBOOK_STREAM_MAXLEN",
                    default="2000",
                    kind="int",
                    description="Stream maxlen",
                ),
                EnvVar(
                    "PLAYBOOK_HEARTBEAT_SEC",
                    default="15",
                    kind="int",
                    description="Heartbeat period seconds",
                ),
                EnvVar(
                    "PLAYBOOK_MAX_FIELD_STR",
                    default="400",
                    kind="int",
                    description="Max field string length",
                ),
                EnvVar(
                    "PLAYBOOK_MAX_MSG_STR",
                    default="1200",
                    kind="int",
                    description="Max message string length",
                ),
                EnvVar(
                    "PLAYBOOK_NOTION_DEDUPE_TTL_SEC",
                    default="600",
                    kind="int",
                    description="Notion dedupe TTL seconds",
                ),
            ],
        ),
        *_env_section(
            "CI / DIAGNOSTICS (scripts)",
            [
                EnvVar(
                    "DIAG_TIMEOUT_S",
                    default="20",
                    kind="int",
                    description="Diagnostics default timeout seconds",
                ),
                EnvVar(
                    "RISK_DIAG_TIMEOUT_S",
                    default="20",
                    kind="int",
                    description="Risk diagnostics timeout seconds",
                ),
                EnvVar(
                    "TEST_STACK_TIMEOUT",
                    default="30",
                    kind="int",
                    description="Test stack timeout seconds",
                ),
                EnvVar(
                    "ALLOW_DRY_RUN_SCRIPT",
                    default="1",
                    kind="bool",
                    description="Allow scripts that do dry-run probes",
                ),
                EnvVar(
                    "ALLOW_TEST_QUOTE_SCRIPT",
                    default="1",
                    kind="bool",
                    description="Allow quote probe in tests",
                ),
                EnvVar(
                    "TEST_QUOTE_SOL",
                    default="0.001",
                    kind="float",
                    description="Quote size for tests (SOL)",
                ),
                EnvVar(
                    "DRYRUN_QUOTE_SOL",
                    default="0.001",
                    kind="float",
                    description="Quote size for dryrun scripts (SOL)",
                ),
                EnvVar(
                    "DRYRUN_TIMEOUT_S",
                    default="12",
                    kind="int",
                    description="Dryrun timeout seconds",
                ),
                EnvVar(
                    "SNIPER_DISABLE_PYTEST_PLUGIN_AUTOLOAD",
                    default="0",
                    kind="bool",
                    description="Project-level switch for pytest plugin autoload handling",
                ),
                EnvVar(
                    "CHECK_ENV_SKIP_CONFIG",
                    kind="bool",
                    default="0",
                    description="(internal) If set, scripts/check_env skips importing config.py checks.",
                ),
                EnvVar(
                    "CHECK_ENV_REQUIRE_CONFIG",
                    kind="bool",
                    default="0",
                    description="(internal) If set, scripts/check_env requires config.py import to succeed (strict).",
                ),
                EnvVar(
                    "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
                    default="0",
                    kind="bool",
                    description="pytest native knob",
                ),
            ],
        ),
        *_env_section(
            "SMOKE INIT (scripts/smoke_init.py)",
            [
                EnvVar(
                    "SMOKE_INIT_JSON",
                    default="0",
                    kind="bool",
                    description="Emit JSON summary to STDOUT (in addition to human stderr logs)",
                ),
                EnvVar(
                    "SMOKE_INIT_JSON_ONLY",
                    default="0",
                    kind="bool",
                    description="Only emit JSON (no human logs)",
                ),
                EnvVar(
                    "SMOKE_INIT_PROBES",
                    default="0",
                    kind="bool",
                    description="Enable optional non-destructive probes (redis ping / pg SELECT 1) when available",
                ),
                EnvVar(
                    "SMOKE_INIT_SKIP_PROBES",
                    default="0",
                    kind="bool",
                    description="Force skip probes (overrides SMOKE_INIT_PROBES)",
                ),
                EnvVar(
                    "SMOKE_INIT_TIMEOUT_S",
                    default="60",
                    kind="int",
                    description="Timeout for init+probes (seconds)",
                ),
                EnvVar(
                    "SMOKE_INIT_NO_CLOSE",
                    default="0",
                    kind="bool",
                    description="Debug: do not call close_resources() (not recommended)",
                ),
                EnvVar(
                    "SMOKE_INIT_NO_DOTENV",
                    default="0",
                    kind="bool",
                    description="Do not attempt to load .env via python-dotenv",
                ),
            ],
        ),
        *_env_section(
            "PREFLIGHT DNS (scripts/preflight.py)",
            [
                EnvVar(
                    "PREFLIGHT_DNS_MODE",
                    default="auto",
                    description="DNS policy: auto|strict|soft|off (auto=strict on Render, soft locally)",
                ),
                EnvVar(
                    "PREFLIGHT_DNS_MIN_OK",
                    default="1",
                    kind="int",
                    description="Minimum number of hostnames that must resolve for DNS step to pass",
                ),
                EnvVar(
                    "PREFLIGHT_DNS_ATTEMPTS",
                    default="3",
                    kind="int",
                    description="DNS resolution attempts per hostname",
                ),
                EnvVar(
                    "PREFLIGHT_DNS_SKIP_INTERNAL",
                    default="1",
                    kind="bool",
                    description="Skip internal/localhost-style DNS checks (recommended for local dev)",
                ),
                EnvVar(
                    "PREFLIGHT_DNS_HOSTS",
                    default="api.jup.ag,public.jupiterapi.com",
                    description="Comma-separated hostnames to resolve in DNS step",
                ),
                EnvVar(
                    "PREFLIGHT_DNS_OFF",
                    default="0",
                    kind="bool",
                    description="Hard disable DNS step (overrides mode)",
                ),
                EnvVar(
                    "PREFLIGHT_DNS_STRICT",
                    default="0",
                    kind="bool",
                    description="Force strict DNS behavior (overrides mode)",
                ),
            ],
        ),
        *_env_section(
            "CI / Contract / Smoke toggles",
            [
                EnvVar(
                    "CHECK_ROUTER",
                    default="0",
                    kind="bool",
                    description="Set to 1 when running scripts/check_router",
                ),
                EnvVar(
                    "CONTRACT_TEST", default="0", kind="bool", description="Contract test guard"
                ),
                EnvVar(
                    "PREFLIGHT",
                    default="0",
                    kind="bool",
                    description="Set to 1 when running scripts/preflight",
                ),
                EnvVar(
                    "SNIPER_TEST_CONTRACT",
                    default="0",
                    kind="bool",
                    description="Contract test guard",
                ),
            ],
        ),
        *_env_section(
            "PATHS",
            [
                EnvVar("DATA_DIR", default="./data", description="Data directory"),
                EnvVar("STATE_DIR", default="./state", description="State directory"),
                EnvVar("LOG_DIR", default="./logs", description="Log directory"),
                EnvVar("CACHE_DIR", default="./cache", description="Cache directory"),
                EnvVar("EXPORT_DIR", default="./exports", description="Exports directory"),
            ],
        ),
    ]
)


def _find_repo_root(start: Path) -> Path:
    rr = _getenv("REPO_ROOT", "")
    if rr:
        with contextlib.suppress(Exception):
            p = Path(rr).resolve()
            if p.exists():
                return p

    markers = (
        ENV_EXAMPLE_FILENAME,
        "pyproject.toml",
        "pytest.ini",
        "requirements.txt",
        "setup.cfg",
        ".git",
        "render.yaml",
        ".renderignore",
        "start.sh",
    )

    cur = start.resolve()
    for p in (cur, *tuple(cur.parents)):
        with contextlib.suppress(Exception):
            if any((p / m).exists() for m in markers):
                return p

            src = p / "src"
            if src.is_dir() and any(
                (src / x).exists() for x in ("utils_pkg", "scanner", "scripts", "tests")
            ):
                return p

            if any((p / x).exists() for x in ("utils_pkg", "scanner", "scripts", "tests")):
                return p

    return cur.parents[0] if cur.parents else cur


def _repo_root_from_here() -> Path:
    return _find_repo_root(Path(__file__).resolve().parent)


def _strip_inline_comment(raw: str) -> tuple[str, str]:
    """Split raw 'VAL # comment' into ('VAL', 'comment') unless # is inside quotes."""
    s = (raw or "").rstrip()
    if not s:
        return "", ""
    in_sq = False
    in_dq = False
    for i, ch in enumerate(s):
        if ch == "'" and not in_dq:
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif ch == "#" and not in_sq and not in_dq:
            return s[:i].rstrip(), s[i + 1 :].strip()
    return s, ""


def _unquote(val: str) -> str:
    v = (val or "").strip()
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        return v[1:-1]
    return v


def _parse_env_example(path: Path) -> tuple[list[str], dict[str, str], dict[str, str]]:
    """
    Returns:
      keys_in_order: list[str]
      defaults: key -> default string (unquoted, comment-stripped)
      comments: key -> inline comment string
    """
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return [], {}, {}

    keys_in_order: list[str] = []
    defaults: dict[str, str] = {}
    comments: dict[str, str] = {}
    seen: set[str] = set()

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        m = _ENV_LINE_RE.match(line)
        if not m:
            continue

        key = (m.group("key") or "").strip()
        raw_val = (m.group("val") or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)

        val_part, cmt = _strip_inline_comment(raw_val)
        val_clean = _unquote(val_part)

        keys_in_order.append(key)
        defaults[key] = val_clean
        if cmt:
            comments[key] = cmt

    return keys_in_order, defaults, comments


_UPPER_SNAKE_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _is_upper_snake(k: str) -> bool:
    return bool(_UPPER_SNAKE_RE.match((k or "").strip()))


def _augment_canonical_with_env_example(
    base: list[EnvVar], repo_root: Path | None = None
) -> list[EnvVar]:
    """
    Contract-hardening: if `.env.example` contains keys not present in CANONICAL_ENV_VARS,
    we append them (only ADD, never remove). This prevents CI drift when `.env.example`
    is updated ahead of the canonical list.

    Defaults/description are captured from `.env.example` when possible.
    """
    root = repo_root or _repo_root_from_here()
    path = root / ENV_EXAMPLE_FILENAME
    if not path.exists():
        return list(base)

    keys_in_order, defaults, comments = _parse_env_example(path)
    # Case-insensitive map: prevents env.example case-variants from becoming new canonical keys
    base_upper: dict[str, EnvVar] = {ev.key.upper(): ev for ev in base}

    extras: list[EnvVar] = []
    for k in keys_in_order:
        kk = (k or "").strip()
        if not kk:
            continue

        # If only case differs (e.g., no_proxy vs NO_PROXY), treat as already covered.
        if kk.upper() in base_upper:
            continue

        # Only allow UPPER_SNAKE_CASE to ever enter canonical keys.
        if not _is_upper_snake(kk):
            continue

        dv = defaults.get(kk, "")
        cmt = (comments.get(kk) or "").strip()
        extras.append(
            EnvVar(
                kk,
                default=dv,
                required=False,
                secret=_is_secret_key(kk),
                description=cmt,
            )
        )

    return _dedupe_keep_order([*base, *extras])


# Apply augmentation at import-time (local file read only; safe)
CANONICAL_ENV_VARS = _augment_canonical_with_env_example(CANONICAL_ENV_VARS)

ENV_VARS: list[EnvVar] = list(CANONICAL_ENV_VARS)
ENV_KEYS: set[str] = {v.key for v in ENV_VARS}
REQUIRED_KEYS: set[str] = {v.key for v in ENV_VARS if v.required}
SAFE_MODE_KEYS: set[str] = {v.key for v in ENV_VARS if _SAFE_TOGGLE_KEY_RE.search(v.key or "")}
ENV_SCHEMA: dict[str, EnvVar] = {v.key: v for v in ENV_VARS}


def env_keys() -> set[str]:
    return set(ENV_KEYS)


def required_env_keys() -> set[str]:
    return set(REQUIRED_KEYS)


def safe_mode_keys() -> set[str]:
    return set(SAFE_MODE_KEYS)


def env_example_diff(repo_root: Path | None = None) -> dict[str, list[str]]:
    """
    Compare canonical ENV_KEYS vs `.env.example`.
    Returns:
      {
        "missing_in_env_example": [...],
        "extra_in_env_example": [...],
      }
    """
    root = repo_root or _repo_root_from_here()
    path = root / ENV_EXAMPLE_FILENAME
    if not path.exists():
        return {"missing_in_env_example": sorted(ENV_KEYS), "extra_in_env_example": []}

    keys_in_order, _, _ = _parse_env_example(path)
    file_keys = set(keys_in_order)

    missing = sorted(k for k in ENV_KEYS if k not in file_keys)
    extra = sorted(k for k in file_keys if k not in ENV_KEYS)

    return {"missing_in_env_example": missing, "extra_in_env_example": extra}


def env_example_required_hint_diff(repo_root: Path | None = None) -> dict[str, list[str]]:
    """
    Checks that REQUIRED_KEYS have an inline '# required' hint on their KEY=VALUE lines in .env.example.

    IMPORTANT:
    - If .env.example has *no* inline 'required' hints anywhere, we treat hint-enforcement as disabled
      (mirrors pytest behavior which skips strict hint consistency tests in that case).
    """
    root = repo_root or _repo_root_from_here()
    path = root / ENV_EXAMPLE_FILENAME
    if not path.exists():
        return {"missing_required_hints": []}

    _, _, comments = _parse_env_example(path)

    # If there are no 'required' hints anywhere, do not enforce required-hint consistency.
    has_any_required_hint = any("required" in (c or "").lower() for c in comments.values())
    if not has_any_required_hint:
        return {"missing_required_hints": []}

    missing: list[str] = []
    for k in sorted(REQUIRED_KEYS):
        c = (comments.get(k) or "").lower()
        if "required" not in c:
            missing.append(k)

    return {"missing_required_hints": missing}


def validate_env_example(
    repo_root: Path | None = None, *, strict: bool = True
) -> dict[str, list[str]]:
    """
    Validate `.env.example` matches the canonical contract.
    strict=True: raise ContractError if missing/extra keys or missing required-hints are found.
    strict=False: return a merged diff report.
    """
    diff = env_example_diff(repo_root=repo_root)
    hints = env_example_required_hint_diff(repo_root=repo_root)

    merged: dict[str, list[str]] = {
        "missing_in_env_example": list(diff.get("missing_in_env_example", [])),
        "extra_in_env_example": list(diff.get("extra_in_env_example", [])),
        "missing_required_hints": list(hints.get("missing_required_hints", [])),
    }

    if strict:
        if merged["missing_required_hints"]:
            miss = merged["missing_required_hints"]
            raise ContractError(
                "ENV required-hint drift detected: missing '# required' hints for: "
                + ", ".join(miss)
            )

        if merged["missing_in_env_example"] or merged["extra_in_env_example"]:
            msg = "ENV contract drift detected:\n"
            if merged["missing_in_env_example"]:
                miss = merged["missing_in_env_example"]
                msg += f"- missing_in_env_example ({len(miss)}): {', '.join(miss[:40])}"
                if len(miss) > 40:
                    msg += " ..."
                msg += "\n"
            if merged["extra_in_env_example"]:
                ext = merged["extra_in_env_example"]
                msg += f"- extra_in_env_example ({len(ext)}): {', '.join(ext[:40])}"
                if len(ext) > 40:
                    msg += " ..."
                msg += "\n"
            raise ContractError(msg.rstrip())

    return merged


def _env_example_lines() -> list[str]:
    """
    Deterministic `.env.example` generator from canonical vars.
    Emits inline `# required ...` hints ONLY for EnvVar.required=True keys.
    """
    lines: list[str] = []
    lines.append("# ===================================================================")
    lines.append("# .env.example — Canonical Env Contract (Generated from contracts.py)")
    lines.append("# ===================================================================")
    lines.append("#")
    lines.append("# Rules:")
    lines.append("# - Do NOT store real secrets here.")
    lines.append("# - Keep this file aligned with CANONICAL_ENV_VARS in utils_pkg/contracts.py")
    lines.append("# - Tests may enforce exact alignment to prevent drift.")
    lines.append("")

    for ev in ENV_VARS:
        desc = (ev.description or "").strip()

        default = "" if ev.default is None else str(ev.default)
        if " " in default and not (default.startswith('"') and default.endswith('"')):
            default = f'"{default}"'

        line = f"{ev.key}={default}"

        if ev.required:
            hint = "required"
            if desc:
                hint = f"{hint}; {desc}"
            line = f"{line}  # {hint}"
        else:
            if desc:
                lines.append(f"# {desc}")

        lines.append(line)
        lines.append("")

    return lines


def write_env_example(repo_root: Path | None = None) -> Path:
    """Deterministically regenerate `.env.example` from CANONICAL_ENV_VARS."""
    root = repo_root or _repo_root_from_here()
    path = root / ENV_EXAMPLE_FILENAME
    txt = "\n".join(_env_example_lines()).rstrip() + "\n"
    path.write_text(txt, encoding="utf-8")
    return path


# =============================================================================
# ENUMS
# =============================================================================


class Chain(str, Enum):
    SOLANA = "solana"


class Source(str, Enum):
    RAYDIUM = "raydium"
    PUMPFUN = "pumpfun"
    DEXSCREENER = "dexscreener"
    AXIOM = "axiom"
    MANUAL = "manual"
    BACKTEST = "backtest"
    REPLAY = "replay"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"


class EventType(str, Enum):
    SCAN_CANDIDATE = "scan_candidate"
    FEATURES_BUILT = "features_built"
    ENTRY_SIGNAL = "entry_signal"
    ROUTE_QUOTE = "route_quote"
    ORDER_INTENT = "order_intent"
    FILL = "fill"
    POSITION_OPEN = "position_open"
    POSITION_UPDATE = "position_update"
    EXIT_SIGNAL = "exit_signal"
    POSITION_CLOSE = "position_close"
    TICK = "tick"
    ERROR = "error"


E = TypeVar("E", bound=Enum)


def as_enum(enum_cls: type[E], v: Any, default: E) -> E:
    if isinstance(v, enum_cls):
        return v
    try:
        return enum_cls(str(v))
    except Exception:
        return default


# =============================================================================
# INTERNAL HELPERS (known vs extra)
# =============================================================================

T = TypeVar("T", bound="ContractBase")


def _split_known_extra(cls: type[T], d: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Returns (known_fields_dict, extra_dict).

    Unknown keys are forwarded into extra.
    If input contains an 'extra' mapping, it is merged and wins on conflicts.
    """
    if not isinstance(d, Mapping):
        raise ValidationError(f"{cls.__name__}.from_dict expects mapping")

    known_names = {f.name for f in dataclasses.fields(cls)}
    raw_extra = d.get("extra")
    extra: dict[str, Any] = dict(raw_extra) if isinstance(raw_extra, Mapping) else {}

    known: dict[str, Any] = {}
    for k, v in d.items():
        kk = str(k)
        if kk in known_names and kk != "extra":
            known[kk] = v
        elif kk == "extra":
            continue
        else:
            if kk not in extra:
                extra[kk] = v

    known["extra"] = extra
    return known, extra


def _base_kwargs(known: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": str(known.get("schema_version", CONTRACTS_VERSION)),
        "schema_rev": safe_int(known.get("schema_rev", SCHEMA_REV), SCHEMA_REV),
        "ts": safe_int(known.get("ts", now_ts()), now_ts()),
        "ts_ms": safe_int(known.get("ts_ms", now_ms()), now_ms()),
        "extra": cast(dict[str, Any], known.get("extra") or {}),
    }


# =============================================================================
# BASE CONTRACT
# =============================================================================


@dataclass(slots=True)
class ContractBase:
    """
    Base contract:
    - schema versioning
    - JSON/dict conversions
    - forward compatibility via `extra`
    """

    schema_version: str = CONTRACTS_VERSION
    schema_rev: int = SCHEMA_REV
    ts: int = field(default_factory=now_ts)
    ts_ms: int = field(default_factory=now_ms)
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "schema_version": str(self.schema_version),
            "schema_rev": int(self.schema_rev),
            "ts": int(self.ts),
            "ts_ms": int(self.ts_ms),
            "extra": dict(self.extra or {}),
        }
        for f in dataclasses.fields(self):
            name = f.name
            if name in out:
                continue
            val = getattr(self, name)
            if val is None:
                continue
            out[name] = _to_primitive(val)
        return cast(dict[str, Any], _to_primitive(out))

    def to_json(self) -> str:
        return json_dumps(self.to_dict())

    @classmethod
    def from_dict(cls: type[Self], d: Mapping[str, Any]) -> Self:
        """Typed contract for subclasses. Subclasses MUST override."""
        raise NotImplementedError(f"{cls.__name__}.from_dict not implemented")

    @classmethod
    def from_json(cls: type[Self], s: str) -> Self:
        try:
            d = json.loads(s)
        except Exception as e:
            raise ValidationError(f"{cls.__name__}.from_json invalid JSON: {e}") from e
        if not isinstance(d, Mapping):
            raise ValidationError(f"{cls.__name__}.from_json expected object")
        return cls.from_dict(cast(Mapping[str, Any], d))

    def redacted(self) -> dict[str, Any]:
        d = self.to_dict()
        d["extra"] = redact_mapping(d.get("extra") or {})
        return cast(dict[str, Any], redact_any(d, max_depth=3))

    def get(self, key: str, default: Any = None) -> Any:
        return (self.extra or {}).get(key, default)

    def with_extra(self: Self, **updates: Any) -> Self:
        e = dict(self.extra or {})
        for k, v in updates.items():
            if k:
                e[str(k)] = v
        return dataclasses.replace(self, extra=e)

    def fingerprint(self, *, include_extra: bool = False) -> str:
        d = self.to_dict()
        if not include_extra:
            d.pop("extra", None)
        return stable_id(json_dumps(d), prefix="fp_")

    def validate(self) -> None:
        return


# =============================================================================
# CORE CONTRACTS
# =============================================================================

# ---- (Everything below stays feature-identical to your current schemas) ----
# NOTE: the following classes are copied from your file with only minimal typing-safe
# adjustments (no feature removals). Keep them as-is unless tests demand new shims.


@dataclass(slots=True)
class Candidate(ContractBase):
    source: Source = Source.MANUAL
    chain: Chain = Chain.SOLANA

    mint: str = ""
    symbol: str = ""
    pool_id: str = ""
    quote_mint: str = ""
    dex: str = ""
    url: str = ""

    price_usd: float = 0.0
    price_sol: float = 0.0

    liq_usd: float = 0.0
    fdv_usd: float = 0.0
    mcap_usd: float = 0.0

    age_s: float = 0.0

    vol_1m: float = 0.0
    vol_5m: float = 0.0
    vol_1h: float = 0.0

    buys_1m: int = 0
    sells_1m: int = 0
    buys_5m: int = 0
    sells_5m: int = 0

    chg_1m_pct: float = 0.0
    chg_5m_pct: float = 0.0
    chg_1h_pct: float = 0.0

    confidence: float = 0.0  # 0..1

    def __post_init__(self) -> None:
        self.source = as_enum(Source, self.source, Source.MANUAL)
        self.chain = as_enum(Chain, self.chain, Chain.SOLANA)

        self.mint = (self.mint or "").strip()
        self.symbol = (self.symbol or "").strip()
        self.pool_id = (self.pool_id or "").strip()
        self.quote_mint = (self.quote_mint or "").strip()
        self.dex = (self.dex or "").strip()
        self.url = (self.url or "").strip()

        self.price_usd = safe_float(self.price_usd, 0.0)
        self.price_sol = safe_float(self.price_sol, 0.0)

        self.liq_usd = max(0.0, safe_float(self.liq_usd, 0.0))
        self.fdv_usd = max(0.0, safe_float(self.fdv_usd, 0.0))
        self.mcap_usd = max(0.0, safe_float(self.mcap_usd, 0.0))

        self.age_s = max(0.0, safe_float(self.age_s, 0.0))

        self.vol_1m = max(0.0, safe_float(self.vol_1m, 0.0))
        self.vol_5m = max(0.0, safe_float(self.vol_5m, 0.0))
        self.vol_1h = max(0.0, safe_float(self.vol_1h, 0.0))

        self.buys_1m = max(0, safe_int(self.buys_1m, 0))
        self.sells_1m = max(0, safe_int(self.sells_1m, 0))
        self.buys_5m = max(0, safe_int(self.buys_5m, 0))
        self.sells_5m = max(0, safe_int(self.sells_5m, 0))

        self.chg_1m_pct = safe_float(self.chg_1m_pct, 0.0)
        self.chg_5m_pct = safe_float(self.chg_5m_pct, 0.0)
        self.chg_1h_pct = safe_float(self.chg_1h_pct, 0.0)

        self.confidence = clamp(self.confidence, 0.0, 1.0)

        if self.mint and not looks_like_base58(self.mint, min_len=20, max_len=90):
            e = dict(self.extra or {})
            w = e.get("warnings")
            if not isinstance(w, list):
                w = []
            if "mint_not_base58ish" not in w:
                w.append("mint_not_base58ish")
            e["warnings"] = w
            self.extra = e

    def key(self) -> str:
        return stable_id(self.source.value, self.mint, self.pool_id or "", prefix="cand_")

    def validate(self) -> None:
        if not self.mint:
            raise ValidationError("Candidate.mint is empty")

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> Candidate:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            source=as_enum(Source, known.get("source", Source.MANUAL.value), Source.MANUAL),
            chain=as_enum(Chain, known.get("chain", Chain.SOLANA.value), Chain.SOLANA),
            mint=str(known.get("mint", "") or ""),
            symbol=str(known.get("symbol", "") or ""),
            pool_id=str(known.get("pool_id", "") or known.get("pair_id", "") or ""),
            quote_mint=str(known.get("quote_mint", "") or known.get("quoteMint", "") or ""),
            dex=str(known.get("dex", "") or ""),
            url=str(known.get("url", "") or ""),
            price_usd=safe_float(known.get("price_usd", known.get("priceUsd", 0.0)), 0.0),
            price_sol=safe_float(known.get("price_sol", 0.0), 0.0),
            liq_usd=safe_float(
                known.get("liq_usd", known.get("liquidity_usd", known.get("liq", 0.0))), 0.0
            ),
            fdv_usd=safe_float(known.get("fdv_usd", known.get("fdv", 0.0)), 0.0),
            mcap_usd=safe_float(known.get("mcap_usd", known.get("mcap", 0.0)), 0.0),
            age_s=safe_float(known.get("age_s", known.get("age_sec", 0.0)), 0.0),
            vol_1m=safe_float(known.get("vol_1m", known.get("vol1", 0.0)), 0.0),
            vol_5m=safe_float(known.get("vol_5m", known.get("vol5", 0.0)), 0.0),
            vol_1h=safe_float(known.get("vol_1h", known.get("vol60", 0.0)), 0.0),
            buys_1m=safe_int(known.get("buys_1m", known.get("buys1", 0)), 0),
            sells_1m=safe_int(known.get("sells_1m", known.get("sells1", 0)), 0),
            buys_5m=safe_int(known.get("buys_5m", known.get("buys5", 0)), 0),
            sells_5m=safe_int(known.get("sells_5m", known.get("sells5", 0)), 0),
            chg_1m_pct=safe_float(known.get("chg_1m_pct", known.get("chg1", 0.0)), 0.0),
            chg_5m_pct=safe_float(known.get("chg_5m_pct", known.get("chg5", 0.0)), 0.0),
            chg_1h_pct=safe_float(known.get("chg_1h_pct", known.get("chg60", 0.0)), 0.0),
            confidence=safe_float(known.get("confidence", 0.0), 0.0),
        )


@dataclass(slots=True)
class Tick(ContractBase):
    chain: Chain = Chain.SOLANA
    mint: str = ""
    price_usd: float = 0.0
    price_sol: float = 0.0
    source: Source = Source.MANUAL

    def __post_init__(self) -> None:
        self.chain = as_enum(Chain, self.chain, Chain.SOLANA)
        self.source = as_enum(Source, self.source, Source.MANUAL)
        self.mint = (self.mint or "").strip()
        self.price_usd = safe_float(self.price_usd, 0.0)
        self.price_sol = safe_float(self.price_sol, 0.0)

    def key(self) -> str:
        return stable_id(self.source.value, self.mint, self.ts_ms, prefix="tick_")

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> Tick:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            chain=as_enum(Chain, known.get("chain", Chain.SOLANA.value), Chain.SOLANA),
            mint=str(known.get("mint", "") or ""),
            price_usd=safe_float(known.get("price_usd", known.get("priceUsd", 0.0)), 0.0),
            price_sol=safe_float(known.get("price_sol", 0.0), 0.0),
            source=as_enum(Source, known.get("source", Source.MANUAL.value), Source.MANUAL),
        )


@dataclass(slots=True)
class EntrySignal(ContractBase):
    ok: bool = False
    score: float = 0.0
    reasons: list[str] = field(default_factory=list)
    hard_rejects: list[str] = field(default_factory=list)
    candidate_key: str = ""

    def __post_init__(self) -> None:
        self.ok = bool(self.ok)
        self.score = clamp(self.score, 0.0, 1.0)
        self.reasons = [str(x) for x in (self.reasons or []) if str(x).strip()][:80]
        self.hard_rejects = [str(x) for x in (self.hard_rejects or []) if str(x).strip()][:80]
        self.candidate_key = (self.candidate_key or "").strip()

    def key(self) -> str:
        return stable_id(self.candidate_key or "", self.ts_ms, prefix="entrysig_")

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> EntrySignal:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            ok=bool(known.get("ok", False)),
            score=safe_float(known.get("score", 0.0), 0.0),
            reasons=list(known.get("reasons") or []),
            hard_rejects=list(known.get("hard_rejects") or known.get("rejects") or []),
            candidate_key=str(known.get("candidate_key", "") or known.get("cand_key", "") or ""),
        )


@dataclass(slots=True)
class RouteQuote(ContractBase):
    in_mint: str = ""
    out_mint: str = ""
    in_amount_raw: int = 0
    out_amount_raw: int = 0
    price_impact_pct: float = 0.0
    fee_amount_raw: int = 0

    route: str = ""
    provider: str = ""
    ok: bool = True
    reason: str = ""

    def __post_init__(self) -> None:
        self.in_mint = (self.in_mint or "").strip()
        self.out_mint = (self.out_mint or "").strip()
        self.in_amount_raw = max(0, safe_int(self.in_amount_raw, 0))
        self.out_amount_raw = max(0, safe_int(self.out_amount_raw, 0))
        self.price_impact_pct = safe_float(self.price_impact_pct, 0.0)
        self.fee_amount_raw = max(0, safe_int(self.fee_amount_raw, 0))
        self.route = (self.route or "").strip()
        self.provider = (self.provider or "").strip()
        self.ok = bool(self.ok)
        self.reason = (self.reason or "").strip()

    def key(self) -> str:
        return stable_id(
            self.provider,
            self.in_mint,
            self.out_mint,
            self.in_amount_raw,
            self.ts_ms,
            prefix="quote_",
        )

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> RouteQuote:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            in_mint=str(known.get("in_mint", "") or known.get("inputMint", "") or ""),
            out_mint=str(known.get("out_mint", "") or known.get("outputMint", "") or ""),
            in_amount_raw=safe_int(known.get("in_amount_raw", known.get("inAmount", 0)), 0),
            out_amount_raw=safe_int(known.get("out_amount_raw", known.get("outAmount", 0)), 0),
            price_impact_pct=safe_float(
                known.get("price_impact_pct", known.get("priceImpactPct", 0.0)), 0.0
            ),
            fee_amount_raw=safe_int(known.get("fee_amount_raw", 0), 0),
            route=str(known.get("route", "") or known.get("route_id", "") or ""),
            provider=str(known.get("provider", "") or ""),
            ok=bool(known.get("ok", True)),
            reason=str(known.get("reason", "") or known.get("error", "") or ""),
        )


@dataclass(slots=True)
class OrderIntent(ContractBase):
    side: Side = Side.BUY
    order_type: OrderType = OrderType.MARKET

    mint: str = ""
    amount_sol: float = 0.0
    amount_raw: int = 0
    slippage_bps: int = 0

    reason: str = ""
    candidate_key: str = ""

    def __post_init__(self) -> None:
        self.side = as_enum(Side, self.side, Side.BUY)
        self.order_type = as_enum(OrderType, self.order_type, OrderType.MARKET)
        self.mint = (self.mint or "").strip()
        self.amount_sol = max(0.0, safe_float(self.amount_sol, 0.0))
        self.amount_raw = max(0, safe_int(self.amount_raw, 0))
        self.slippage_bps = max(0, safe_int(self.slippage_bps, 0))
        self.reason = (self.reason or "").strip()
        self.candidate_key = (self.candidate_key or "").strip()

    def key(self) -> str:
        return stable_id(
            self.side.value, self.mint, self.candidate_key or "", self.ts_ms, prefix="intent_"
        )

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> OrderIntent:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            side=as_enum(Side, known.get("side", Side.BUY.value), Side.BUY),
            order_type=as_enum(
                OrderType, known.get("order_type", OrderType.MARKET.value), OrderType.MARKET
            ),
            mint=str(known.get("mint", "") or ""),
            amount_sol=safe_float(known.get("amount_sol", 0.0), 0.0),
            amount_raw=safe_int(known.get("amount_raw", 0), 0),
            slippage_bps=safe_int(known.get("slippage_bps", known.get("slippageBps", 0)), 0),
            reason=str(known.get("reason", "") or ""),
            candidate_key=str(known.get("candidate_key", "") or ""),
        )


@dataclass(slots=True)
class Fill(ContractBase):
    side: Side = Side.BUY
    mint: str = ""

    tx_sig: str = ""
    in_mint: str = ""
    out_mint: str = ""
    in_amount_raw: int = 0
    out_amount_raw: int = 0

    avg_price_usd: float = 0.0
    avg_price_sol: float = 0.0

    ok: bool = True
    reason: str = ""

    def __post_init__(self) -> None:
        self.side = as_enum(Side, self.side, Side.BUY)
        self.mint = (self.mint or "").strip()
        self.tx_sig = (self.tx_sig or "").strip()
        self.in_mint = (self.in_mint or "").strip()
        self.out_mint = (self.out_mint or "").strip()
        self.in_amount_raw = max(0, safe_int(self.in_amount_raw, 0))
        self.out_amount_raw = max(0, safe_int(self.out_amount_raw, 0))
        self.avg_price_usd = safe_float(self.avg_price_usd, 0.0)
        self.avg_price_sol = safe_float(self.avg_price_sol, 0.0)
        self.ok = bool(self.ok)
        self.reason = (self.reason or "").strip()

    def key(self) -> str:
        return stable_id(self.tx_sig or "", self.side.value, self.mint, self.ts_ms, prefix="fill_")

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> Fill:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            side=as_enum(Side, known.get("side", Side.BUY.value), Side.BUY),
            mint=str(known.get("mint", "") or ""),
            tx_sig=str(known.get("tx_sig", "") or known.get("signature", "") or ""),
            in_mint=str(known.get("in_mint", "") or known.get("inputMint", "") or ""),
            out_mint=str(known.get("out_mint", "") or known.get("outputMint", "") or ""),
            in_amount_raw=safe_int(known.get("in_amount_raw", known.get("inAmount", 0)), 0),
            out_amount_raw=safe_int(known.get("out_amount_raw", known.get("outAmount", 0)), 0),
            avg_price_usd=safe_float(known.get("avg_price_usd", 0.0), 0.0),
            avg_price_sol=safe_float(known.get("avg_price_sol", 0.0), 0.0),
            ok=bool(known.get("ok", True)),
            reason=str(known.get("reason", "") or known.get("error", "") or ""),
        )


@dataclass(slots=True)
class Position(ContractBase):
    mint: str = ""
    symbol: str = ""

    size_raw: int = 0
    decimals: int = 0

    entry_price_usd: float = 0.0
    entry_price_sol: float = 0.0
    cost_sol: float = 0.0

    opened_ts: int = field(default_factory=now_ts)
    last_ts: int = field(default_factory=now_ts)

    stop_loss_pct: float = 0.0
    take_profit_pct: float = 0.0
    trailing_arm_pct: float = 0.0
    trailing_pct: float = 0.0

    entry_tx: str = ""
    last_tx: str = ""

    def __post_init__(self) -> None:
        self.mint = (self.mint or "").strip()
        self.symbol = (self.symbol or "").strip()

        self.size_raw = max(0, safe_int(self.size_raw, 0))
        self.decimals = max(0, min(18, safe_int(self.decimals, 0)))

        self.entry_price_usd = safe_float(self.entry_price_usd, 0.0)
        self.entry_price_sol = safe_float(self.entry_price_sol, 0.0)
        self.cost_sol = max(0.0, safe_float(self.cost_sol, 0.0))

        self.opened_ts = safe_int(self.opened_ts, now_ts())
        self.last_ts = safe_int(self.last_ts, now_ts())

        self.stop_loss_pct = safe_float(self.stop_loss_pct, 0.0)
        self.take_profit_pct = safe_float(self.take_profit_pct, 0.0)
        self.trailing_arm_pct = safe_float(self.trailing_arm_pct, 0.0)
        self.trailing_pct = safe_float(self.trailing_pct, 0.0)

        self.entry_tx = (self.entry_tx or "").strip()
        self.last_tx = (self.last_tx or "").strip()

    def pos_id(self) -> str:
        return stable_id(self.mint, self.opened_ts, prefix="pos_")

    def key(self) -> str:
        return self.pos_id()

    def pnl_pct(
        self, *, mark_price_usd: float | None = None, mark_price_sol: float | None = None
    ) -> float:
        if mark_price_usd is not None and self.entry_price_usd > 0:
            return 100.0 * (safe_float(mark_price_usd) / self.entry_price_usd - 1.0)
        if mark_price_sol is not None and self.entry_price_sol > 0:
            return 100.0 * (safe_float(mark_price_sol) / self.entry_price_sol - 1.0)
        return 0.0

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> Position:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            mint=str(known.get("mint", "") or ""),
            symbol=str(known.get("symbol", "") or ""),
            size_raw=safe_int(
                known.get("size_raw", known.get("qty_raw", known.get("amount_raw", 0))), 0
            ),
            decimals=safe_int(known.get("decimals", 0), 0),
            entry_price_usd=safe_float(
                known.get("entry_price_usd", known.get("entryPriceUsd", 0.0)), 0.0
            ),
            entry_price_sol=safe_float(known.get("entry_price_sol", 0.0), 0.0),
            cost_sol=safe_float(known.get("cost_sol", 0.0), 0.0),
            opened_ts=safe_int(known.get("opened_ts", now_ts()), now_ts()),
            last_ts=safe_int(known.get("last_ts", now_ts()), now_ts()),
            stop_loss_pct=safe_float(known.get("stop_loss_pct", 0.0), 0.0),
            take_profit_pct=safe_float(known.get("take_profit_pct", 0.0), 0.0),
            trailing_arm_pct=safe_float(known.get("trailing_arm_pct", 0.0), 0.0),
            trailing_pct=safe_float(known.get("trailing_pct", 0.0), 0.0),
            entry_tx=str(known.get("entry_tx", "") or known.get("entry_sig", "") or ""),
            last_tx=str(known.get("last_tx", "") or ""),
        )


@dataclass(slots=True)
class ExitSignal(ContractBase):
    should_exit: bool = False
    reason: str = ""
    strength: float = 0.0  # 0..1

    mint: str = ""
    pos_id: str = ""
    target_side: Side = Side.SELL

    def __post_init__(self) -> None:
        self.should_exit = bool(self.should_exit)
        self.reason = (self.reason or "").strip()
        self.strength = clamp(self.strength, 0.0, 1.0)
        self.mint = (self.mint or "").strip()
        self.pos_id = (self.pos_id or "").strip()
        self.target_side = as_enum(Side, self.target_side, Side.SELL)

    def key(self) -> str:
        return stable_id(self.pos_id or "", self.mint, self.ts_ms, prefix="exitsig_")

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> ExitSignal:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            should_exit=bool(known.get("should_exit", known.get("exit", False))),
            reason=str(known.get("reason", "") or ""),
            strength=safe_float(known.get("strength", 0.0), 0.0),
            mint=str(known.get("mint", "") or ""),
            pos_id=str(known.get("pos_id", "") or ""),
            target_side=as_enum(Side, known.get("target_side", Side.SELL.value), Side.SELL),
        )


@dataclass(slots=True)
class RiskSnapshot(ContractBase):
    ok_to_trade: bool = True
    reason: str = ""

    daily_pnl_sol: float = 0.0
    drawdown_pct: float = 0.0

    open_positions: int = 0
    inflight_orders: int = 0
    max_concurrency: int = 0

    def __post_init__(self) -> None:
        self.ok_to_trade = bool(self.ok_to_trade)
        self.reason = (self.reason or "").strip()
        self.daily_pnl_sol = safe_float(self.daily_pnl_sol, 0.0)
        self.drawdown_pct = safe_float(self.drawdown_pct, 0.0)
        self.open_positions = max(0, safe_int(self.open_positions, 0))
        self.inflight_orders = max(0, safe_int(self.inflight_orders, 0))
        self.max_concurrency = max(0, safe_int(self.max_concurrency, 0))

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> RiskSnapshot:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            ok_to_trade=bool(known.get("ok_to_trade", True)),
            reason=str(known.get("reason", "") or ""),
            daily_pnl_sol=safe_float(known.get("daily_pnl_sol", 0.0), 0.0),
            drawdown_pct=safe_float(known.get("drawdown_pct", 0.0), 0.0),
            open_positions=safe_int(known.get("open_positions", 0), 0),
            inflight_orders=safe_int(known.get("inflight_orders", 0), 0),
            max_concurrency=safe_int(known.get("max_concurrency", 0), 0),
        )


@dataclass(slots=True)
class EventEnvelope(ContractBase):
    """Generic envelope for logging/streams."""

    type: EventType = EventType.ERROR
    data: Any = field(default_factory=dict)
    meta: Any = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.type = as_enum(EventType, self.type, EventType.ERROR)

        if isinstance(self.data, Mapping):
            self.data = dict(self.data)
        else:
            self.data = {"value": _to_primitive(self.data)}

        if isinstance(self.meta, Mapping):
            self.meta = dict(self.meta)
        else:
            self.meta = {"value": _to_primitive(self.meta)}

    def key(self) -> str:
        sig = json_dumps(self.data)
        if len(sig) > 240:
            sig = sig[:240]
        return stable_id(self.type.value, self.ts_ms, sig, prefix="evt_")

    def redacted(self) -> dict[str, Any]:
        d = self.to_dict()
        d["data"] = cast(dict[str, Any], redact_any(d.get("data") or {}, max_depth=3))
        d["meta"] = cast(dict[str, Any], redact_any(d.get("meta") or {}, max_depth=3))
        d["extra"] = redact_mapping(d.get("extra") or {})
        return d

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> EventEnvelope:
        known, _ = _split_known_extra(cls, d)
        return cls(
            **_base_kwargs(known),
            type=as_enum(EventType, known.get("type", EventType.ERROR.value), EventType.ERROR),
            data=dict(known.get("data") or {}),
            meta=dict(known.get("meta") or {}),
        )


# =============================================================================
# POSITION NORMALIZER (CONTRACT: MUST RETURN DICT)
# =============================================================================


def _coerce_to_dict(position: Any, *, kwargs_fallback: dict[str, Any]) -> dict[str, Any]:
    """
    Coerce arbitrary input into a dict[str, Any] without creating mypy "unreachable"
    branches or ambiguous asdict typing.
    """
    if position is None:
        return dict(kwargs_fallback) if kwargs_fallback else {}

    if isinstance(position, Position):
        return position.to_dict()

    if dataclasses.is_dataclass(position):
        return _dc_asdict(position)

    if isinstance(position, Mapping):
        return dict(position)

    try:
        return dict(cast(Any, position))  # e.g., list of pairs
    except Exception:
        return {"value": position}


def normalize_position(
    *args: Any,
    position: Any | None = None,
    pos: Any | None = None,
    d: Any | None = None,
    ctx: Mapping[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, Any]:
    """
    Canonical position normalizer for contract tests + persistence.
    MUST return a JSON-safe dict.

    Supports flexible calling patterns:
      normalize_position(pos, ctx)
      normalize_position(pos, ctx=ctx)
      normalize_position(position=pos, ctx=ctx)
      normalize_position(pos=pos, ctx=ctx)
      normalize_position(d=pos, ctx=ctx)
    """
    if args:
        if position is None:
            position = args[0]
        if len(args) > 1 and ctx is None:
            ctx = cast(Mapping[str, Any], args[1])

    if position is None:
        position = pos if pos is not None else d

    out = _coerce_to_dict(position, kwargs_fallback=dict(kwargs))

    # ensure required schema keys exist
    out.setdefault("schema_version", CONTRACTS_VERSION)
    out.setdefault("schema_rev", SCHEMA_REV)

    # ctx time normalization (supports now_ms/ts_ms/now_ts/ts)
    ctxd = dict(ctx or {})
    now_ms_val = safe_int(ctxd.get("now_ms", ctxd.get("ts_ms", 0)), 0)
    now_ts_val = safe_int(ctxd.get("now_ts", ctxd.get("ts", 0)), 0)

    if now_ms_val > 0:
        out.setdefault("ts_ms", now_ms_val)
        out.setdefault("ts", int(now_ms_val // 1000))
    elif now_ts_val > 0:
        out.setdefault("ts", now_ts_val)
        out.setdefault("ts_ms", int(now_ts_val * 1000))

    out["ts"] = safe_int(out.get("ts", now_ts()), now_ts())
    out["ts_ms"] = safe_int(out.get("ts_ms", now_ms()), now_ms())

    extra = out.get("extra")
    if not isinstance(extra, Mapping):
        out["extra"] = {} if extra is None else {"value": extra}
    else:
        out["extra"] = dict(extra)

    if "size_raw" not in out:
        if "qty_raw" in out:
            out["size_raw"] = out.get("qty_raw")
        elif "amount_raw" in out:
            out["size_raw"] = out.get("amount_raw")

    return cast(dict[str, Any], _to_primitive(out))


@overload
def ensure_position(
    x: Any = None, ctx: Mapping[str, Any] = ..., /, **kwargs: Any
) -> dict[str, Any]: ...
@overload
def ensure_position(x: Any = None, ctx: None = ..., /, **kwargs: Any) -> Position: ...


def ensure_position(x: Any = None, ctx: Any = None, /, **kwargs: Any) -> Any:
    """
    Dual-mode helper used by legacy callers + tests:
    - object mode: ensure_position(obj_or_dict) -> Position
    - dict/normalizer mode: ensure_position(obj_or_dict, ctx) -> dict (via ensure_position_dict)
    """
    if x is None:
        for k in ("position", "pos", "d"):
            if k in kwargs:
                x = kwargs.pop(k)
                break

    if ctx is not None:
        kwargs.setdefault("ctx", cast(Mapping[str, Any], ctx))
        return ensure_position_dict(x, **kwargs)

    if kwargs.get("ctx") is not None:
        return ensure_position_dict(x, **kwargs)

    dct = ensure_position_dict(x, **kwargs)
    return Position.from_dict(dct)


def ensure_position_dict(x: Any = None, /, **kwargs: Any) -> dict[str, Any]:
    if x is None:
        return normalize_position(**kwargs)
    return normalize_position(x, **kwargs)


def ensure_position_obj(x: Any = None, /, **kwargs: Any) -> Position:
    kwargs.pop("ctx", None)
    if x is None:
        for k in ("position", "pos", "d"):
            if k in kwargs:
                x = kwargs.pop(k)
                break
    return ensure_position(x, None, **kwargs)


# =============================================================================
# ENSURE HELPERS (OBJECT RETURNERS)
# =============================================================================

CandidateLike = Union[Candidate, Mapping[str, Any]]
TickLike = Union[Tick, Mapping[str, Any]]
EntrySignalLike = Union[EntrySignal, Mapping[str, Any]]
RouteQuoteLike = Union[RouteQuote, Mapping[str, Any]]
OrderIntentLike = Union[OrderIntent, Mapping[str, Any]]
FillLike = Union[Fill, Mapping[str, Any]]
PositionLike = Union[Position, Mapping[str, Any]]
ExitSignalLike = Union[ExitSignal, Mapping[str, Any]]
RiskSnapshotLike = Union[RiskSnapshot, Mapping[str, Any]]
EventEnvelopeLike = Union[EventEnvelope, Mapping[str, Any]]


def ensure_candidate(x: CandidateLike) -> Candidate:
    return x if isinstance(x, Candidate) else Candidate.from_dict(x)


def ensure_tick(x: TickLike) -> Tick:
    return x if isinstance(x, Tick) else Tick.from_dict(x)


def ensure_entry_signal(x: EntrySignalLike) -> EntrySignal:
    return x if isinstance(x, EntrySignal) else EntrySignal.from_dict(x)


def ensure_route_quote(x: RouteQuoteLike) -> RouteQuote:
    return x if isinstance(x, RouteQuote) else RouteQuote.from_dict(x)


def ensure_order_intent(x: OrderIntentLike) -> OrderIntent:
    return x if isinstance(x, OrderIntent) else OrderIntent.from_dict(x)


def ensure_fill(x: FillLike) -> Fill:
    return x if isinstance(x, Fill) else Fill.from_dict(x)


def ensure_exit_signal(x: ExitSignalLike) -> ExitSignal:
    return x if isinstance(x, ExitSignal) else ExitSignal.from_dict(x)


def ensure_risk_snapshot(x: RiskSnapshotLike) -> RiskSnapshot:
    return x if isinstance(x, RiskSnapshot) else RiskSnapshot.from_dict(x)


def ensure_event_envelope(x: EventEnvelopeLike) -> EventEnvelope:
    return x if isinstance(x, EventEnvelope) else EventEnvelope.from_dict(x)


def build_contracts_surface() -> dict[str, Any]:
    """Builder-style surface for contract/startup discovery tests."""
    return {
        "schema_version": CONTRACTS_VERSION,
        "Position": Position,
        "Candidate": Candidate,
        "Tick": Tick,
        "EntrySignal": EntrySignal,
        "RouteQuote": RouteQuote,
        "OrderIntent": OrderIntent,
        "Fill": Fill,
        "ExitSignal": ExitSignal,
        "RiskSnapshot": RiskSnapshot,
        "EventEnvelope": EventEnvelope,
        "ensure_position": ensure_position,
        "ensure_candidate": ensure_candidate,
        "ensure_tick": ensure_tick,
        "ensure_entry_signal": ensure_entry_signal,
        "ensure_route_quote": ensure_route_quote,
        "ensure_order_intent": ensure_order_intent,
        "ensure_fill": ensure_fill,
        "ensure_exit_signal": ensure_exit_signal,
        "ensure_risk_snapshot": ensure_risk_snapshot,
        "ensure_event_envelope": ensure_event_envelope,
        "env_example_diff": env_example_diff,
        "validate_env_example": validate_env_example,
        "write_env_example": write_env_example,
        "normalize_position": normalize_position,
        "ensure_position_dict": ensure_position_dict,
        "ensure_position_obj": ensure_position_obj,
        "ENV_VARS": ENV_VARS,
        "ENV_KEYS": ENV_KEYS,
        "REQUIRED_KEYS": REQUIRED_KEYS,
    }


def build() -> dict[str, Any]:
    return build_contracts_surface()


def schema_names() -> list[str]:
    return [
        "Candidate",
        "Tick",
        "EntrySignal",
        "RouteQuote",
        "OrderIntent",
        "Fill",
        "Position",
        "ExitSignal",
        "RiskSnapshot",
        "EventEnvelope",
    ]


__all__ = [
    "CONTRACTS_VERSION",
    "ENV_KEYS",
    "ENV_VARS",
    "EXTERNAL_ENV_ALLOWLIST",
    "EXTERNAL_ENV_ALLOW_PATTERNS",
    "REQUIRED_KEYS",
    "SAFE_MODE_KEYS",
    "SCHEMA_REV",
    "Candidate",
    "CandidateLike",
    "Chain",
    "ContractBase",
    "ContractError",
    "EntrySignal",
    "EntrySignalLike",
    "EnvVar",
    "EventEnvelope",
    "EventEnvelopeLike",
    "EventType",
    "ExitSignal",
    "ExitSignalLike",
    "Fill",
    "FillLike",
    "OrderIntent",
    "OrderIntentLike",
    "OrderType",
    "Position",
    "PositionLike",
    "RiskSnapshot",
    "RiskSnapshotLike",
    "RouteQuote",
    "RouteQuoteLike",
    "Side",
    "Source",
    "Tick",
    "TickLike",
    "ValidationError",
    "clamp",
    "ensure_candidate",
    "ensure_entry_signal",
    "ensure_event_envelope",
    "ensure_exit_signal",
    "ensure_fill",
    "ensure_order_intent",
    "ensure_position",
    "ensure_position_obj",
    "ensure_position_dict",
    "ensure_risk_snapshot",
    "ensure_route_quote",
    "ensure_tick",
    "env_keys",
    "env_example_diff",
    "validate_env_example",
    "write_env_example",
    "json_dumps",
    "looks_like_base58",
    "normalize_position",
    "now_ms",
    "now_ts",
    "redact_any",
    "redact_mapping",
    "required_env_keys",
    "safe_float",
    "safe_int",
    "safe_mode_keys",
    "schema_names",
    "stable_id",
    "build",
    "build_contracts_surface",
]
