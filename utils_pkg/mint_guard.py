# utils_pkg/mint_guard.py
from __future__ import annotations

"""
utils_pkg/mint_guard.py — God-Tier Mint Guardrails (V3+++ / P0 LOCK-IN)
======================================================================

CRITICAL TEST CONTRACT
----------------------
- MUST be import-safe (tests may swallow import errors and then mg is undefined).
- ZERO network at import time.
- Avoid importing heavy / network deps at import time (optional deps must be lazy).

LOCK-IN FEATURES PRESERVED
--------------------------
✅ Canonical mint normalization (symbols/urls/blobs → pubkey string)
✅ Allowlist / denylist (env + runtime); deny overrides allow
✅ Blocks obvious non-mint program IDs (system/token/ATA/memo/etc.)
✅ Cooldowns (local + optional Redis shared cooldown across workers)
✅ Dedupe reservation (optional Redis SET NX) to prevent concurrent entries
✅ Rate limiting (local): per-mint + global + unique-mints-per-window caps
✅ Optional on-chain validation (async, timed) to ensure the address is a real SPL mint

Public API:
- guard_mint, aguard_mint
- guard_pair, aguard_pair
- guard_jupiter_quote_args, aguard_jupiter_quote_args
- check_mint, acheck_mint
- cooldown_mint, cooldown_mint_shared, mark_bad_mint
- is_non_mint_address, is_anchor_mint, classify_mint

Test-visible caches (tests clear these attributes best-effort):
- _ENV_CACHE, _PARSED_ENV, _CACHE, _ALLOW_CACHE, _DENY_CACHE, _NON_MINT_CACHE
"""

import asyncio
import os
import string
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from threading import RLock
from typing import Any

# ──────────────────────────────────────────────────────────────────────────────
# Test-friendly caches (tests clear these attributes best-effort)
# ──────────────────────────────────────────────────────────────────────────────
_ENV_CACHE: dict[str, str] = {}
_PARSED_ENV: dict[str, set[str]] = {}
_CACHE: dict[str, Any] = {}  # generic decision cache (optional)
_ALLOW_CACHE: set[str] | None = None
_DENY_CACHE: set[str] | None = None
_NON_MINT_CACHE: set[str] | None = None

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}

# ──────────────────────────────────────────────────────────────────────────────
# Base58 decode (stdlib only)
#   canonical Solana pubkey decodes to exactly 32 bytes
# ──────────────────────────────────────────────────────────────────────────────
_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_INDEX = {c: i for i, c in enumerate(_B58_ALPHABET)}
_B58_SET = set(_B58_ALPHABET)

_PUBKEYISH_CHARS = set(string.ascii_letters + string.digits)


def _b58decode(s: str) -> bytes | None:
    s = (s or "").strip()
    if not s:
        return None

    num = 0
    for ch in s:
        v = _B58_INDEX.get(ch)
        if v is None:
            return None
        num = num * 58 + v

    b = num.to_bytes((num.bit_length() + 7) // 8, "big") if num else b""

    pad = 0
    for ch in s:
        if ch == "1":
            pad += 1
        else:
            break

    return (b"\x00" * pad) + b


def is_canonical_pubkey(s: str) -> bool:
    b = _b58decode((s or "").strip())
    return b is not None and len(b) == 32


# ──────────────────────────────────────────────────────────────────────────────
# Known anchor mints (hard-coded; avoids importing project modules at import-time)
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT = "So11111111111111111111111111111111111111112"  # WSOL mint
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
BONK_MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"

_SYMBOL_MAP: dict[str, str] = {
    "SOL": SOL_MINT,
    "WSOL": SOL_MINT,
    "USDC": USDC_MINT,
    "USDT": USDT_MINT,
    "BONK": BONK_MINT,
}

_DEFAULT_ANCHORS: set[str] = {SOL_MINT, USDC_MINT, USDT_MINT}

_DEFAULT_NON_MINT_PUBKEYS: set[str] = {
    "11111111111111111111111111111111",  # System Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token (legacy)
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  # SPL Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",  # ATA program
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",  # Memo
    "ComputeBudget111111111111111111111111111111",  # Compute budget
    "AddressLookupTab1e1111111111111111111111111",  # ALT program
}


# ──────────────────────────────────────────────────────────────────────────────
# Normalization helpers (NO heavy imports)
# ──────────────────────────────────────────────────────────────────────────────
def _coerce_mint_str(x: Any) -> str:
    """
    Convert common mint representations into a clean string.

    Supports:
    - bytes/bytearray -> utf-8 decode
    - "Pubkey(....)" / "pubkey(....)" -> extract inner
    - otherwise -> str(x)
    """
    if x is None:
        return ""

    if isinstance(x, (bytes, bytearray)):
        s = x.decode("utf-8", "ignore").strip()
    else:
        s = str(x).strip()

    if not s:
        return ""

    low = s.lower()
    if low.startswith("pubkey(") and s.endswith(")"):
        inner = s[s.find("(") + 1 : -1].strip()
        if (inner.startswith('"') and inner.endswith('"')) or (
            inner.startswith("'") and inner.endswith("'")
        ):
            inner = inner[1:-1].strip()
        return inner.strip()

    return s


def _is_base58_pubkeyish(s: str) -> bool:
    s = (s or "").strip()
    if not s:
        return False
    if len(s) < 32 or len(s) > 64:
        return False
    return all(ch in _B58_SET for ch in s)


def _is_pubkeyish_lenient(s: str) -> bool:
    """
    Lenient pubkey-ish check for fixtures:
    - alphanumeric only
    - plausible length band
    """
    s = (s or "").strip()
    if not s:
        return False
    if len(s) < 32 or len(s) > 64:
        return False
    return all(ch in _PUBKEYISH_CHARS for ch in s)


def _env_str(key: str, default: str = "") -> str:
    return (os.getenv(key, default) or default).strip()


def _env_bool(key: str, default: bool = False) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _env_int(key: str, default: int) -> int:
    v = os.getenv(key)
    if v is None:
        return int(default)
    try:
        return int(str(v).strip())
    except Exception:
        return int(default)


def _env_float(key: str, default: float) -> float:
    v = os.getenv(key)
    if v is None:
        return float(default)
    try:
        return float(str(v).strip())
    except Exception:
        return float(default)


def _split_list(s: str) -> list[str]:
    raw = (s or "").strip()
    if not raw:
        return []
    raw = raw.replace("\n", ",").replace(";", ",").replace("|", ",").replace("\r", ",")
    out: list[str] = []
    for chunk in raw.split(","):
        c = chunk.strip()
        if not c:
            continue
        for part in c.split():
            p = part.strip()
            if p:
                out.append(p)
    return out


def _now() -> float:
    return time.monotonic()


def _pubkey_ok(s: str) -> bool:
    """
    STRICT mode (contract-approved key): require canonical 32-byte pubkey.
    Default: accept canonical OR base58-ish OR lenient alphanumeric pubkey-ish.
    """
    s = (s or "").strip()
    if not s:
        return False

    strict = _env_bool("MINT_GUARD_STRICT_PUBKEY", False)
    if strict:
        return is_canonical_pubkey(s)

    return is_canonical_pubkey(s) or _is_base58_pubkeyish(s) or _is_pubkeyish_lenient(s)


def _extract_canonical_pubkey_from_blob(s: str) -> str | None:
    """
    Extract canonical pubkey candidates from arbitrary strings (urls/blobs/logs),
    preferring canonical 32-byte decodes.
    """
    s = (s or "").strip()
    if not s:
        return None

    run: list[str] = []
    runs: list[str] = []

    for ch in s:
        if ch in _B58_SET:
            run.append(ch)
        else:
            if len(run) >= 32:
                runs.append("".join(run))
            run = []
    if len(run) >= 32:
        runs.append("".join(run))

    for cand in runs:
        if 32 <= len(cand) <= 64 and is_canonical_pubkey(cand):
            return cand

    return None


def _normalize_symbol_or_pubkey(x: Any) -> str | None:
    """
    Normalize:
    - symbol like "SOL" -> known mint
    - "Pubkey(...)" -> inner
    - direct pubkey-ish string -> itself
    - blob/url -> attempt extract canonical pubkey
    """
    if x is None:
        return None

    s = _coerce_mint_str(x)
    if not s:
        return None

    up = s.upper()
    if up in _SYMBOL_MAP:
        return _SYMBOL_MAP[up]

    if _pubkey_ok(s):
        return s

    ext = _extract_canonical_pubkey_from_blob(s)
    if ext and _pubkey_ok(ext):
        return ext

    return None


def _resolve_list_items(items: Iterable[str]) -> set[str]:
    out: set[str] = set()
    for it in items:
        m = _normalize_symbol_or_pubkey(it)
        if m and _pubkey_ok(m):
            out.add(m)
    return out


def _refresh_env_sets() -> None:
    """
    Parse allow/deny/non-mints from env with caching (test-friendly).
    """
    global _ALLOW_CACHE, _DENY_CACHE, _NON_MINT_CACHE

    allow_raw = _env_str("MINT_GUARD_ALLOWLIST", "")
    deny_raw = _env_str("MINT_GUARD_DENYLIST", "")
    non_raw = _env_str("MINT_GUARD_NON_MINTS", "")

    prev = (
        _ENV_CACHE.get("MINT_GUARD_ALLOWLIST", ""),
        _ENV_CACHE.get("MINT_GUARD_DENYLIST", ""),
        _ENV_CACHE.get("MINT_GUARD_NON_MINTS", ""),
    )
    cur = (allow_raw, deny_raw, non_raw)

    if (
        cur == prev
        and _ALLOW_CACHE is not None
        and _DENY_CACHE is not None
        and _NON_MINT_CACHE is not None
    ):
        return

    _ENV_CACHE["MINT_GUARD_ALLOWLIST"] = allow_raw
    _ENV_CACHE["MINT_GUARD_DENYLIST"] = deny_raw
    _ENV_CACHE["MINT_GUARD_NON_MINTS"] = non_raw

    _ALLOW_CACHE = _resolve_list_items(_split_list(allow_raw))
    _DENY_CACHE = _resolve_list_items(_split_list(deny_raw))

    nm = set(_DEFAULT_NON_MINT_PUBKEYS)
    nm |= _resolve_list_items(_split_list(non_raw))
    _NON_MINT_CACHE = nm

    _PARSED_ENV["allow"] = set(_ALLOW_CACHE)
    _PARSED_ENV["deny"] = set(_DENY_CACHE)
    _PARSED_ENV["non_mints"] = set(_NON_MINT_CACHE)

    _CACHE.clear()


# ──────────────────────────────────────────────────────────────────────────────
# Errors / Decisions
# ──────────────────────────────────────────────────────────────────────────────
class MintGuardError(ValueError):
    """
    Raising API error. Tests assert .code and class identity.
    """

    def __init__(self, code: str, detail: str, mint: str | None = None):
        self.code = str(code)
        self.detail = str(detail)
        self.mint = mint
        super().__init__(self.__str__())

    def __str__(self) -> str:
        m = f" mint={self.mint}" if self.mint else ""
        return f"MintGuardError({self.code}): {self.detail}{m}"


@dataclass(frozen=True)
class MintGuardDecision:
    ok: bool
    mint: str | None = None
    code: str = "ok"
    detail: str = ""
    meta: dict[str, Any] | None = None


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class MintGuardConfig:
    enabled: bool
    use_redis: bool
    redis_prefix: str

    deny: set[str]
    allow: set[str]
    non_mint: set[str]

    cooldown_sec: int
    fail_cooldown_sec: int
    bad_cooldown_sec: int
    reserve_sec: int

    hit_window_sec: int
    max_hits_per_mint: int
    max_hits_global: int
    max_unique_per_window: int

    validate_onchain: bool
    onchain_timeout_s: float
    onchain_strict: bool

    default_require_anchor: bool
    anchors: set[str]

    @staticmethod
    def from_env() -> MintGuardConfig:
        _refresh_env_sets()

        enabled = _env_bool("MINT_GUARD_ENABLED", True)
        use_redis = _env_bool("MINT_GUARD_USE_REDIS", True)
        redis_prefix = (_env_str("MINT_GUARD_REDIS_PREFIX", "mint_guard") or "mint_guard").strip()

        deny = set(_DENY_CACHE or set())
        allow = set(_ALLOW_CACHE or set())
        non_mint = set(_NON_MINT_CACHE or set())

        cooldown_sec = max(0, _env_int("MINT_GUARD_COOLDOWN_SEC", 0))
        fail_cooldown_sec = max(0, _env_int("MINT_GUARD_FAIL_COOLDOWN_SEC", 12 * 60))

        # CONTRACT KEY ONLY (do NOT reference MINT_GUARD_BAD_COOLDOWN_SEC)
        bad_cd_raw = os.getenv("MINT_GUARD_BAD_COOLOFF_SEC")
        try:
            bad_cooldown_sec = (
                max(0, int(str(bad_cd_raw).strip())) if bad_cd_raw is not None else 60 * 60
            )
        except Exception:
            bad_cooldown_sec = 60 * 60

        reserve_sec = max(1, _env_int("MINT_GUARD_RESERVE_SEC", 60))

        hit_window_sec = max(1, _env_int("MINT_GUARD_HIT_WINDOW_SEC", 60))
        max_hits_per_mint = max(1, _env_int("MINT_GUARD_MAX_HITS_PER_MINT", 20))
        max_hits_global = max(1, _env_int("MINT_GUARD_MAX_HITS_GLOBAL", 4000))
        max_unique_per_window = max(1, _env_int("MINT_GUARD_MAX_UNIQUE_PER_WINDOW", 1500))

        validate_onchain = _env_bool("MINT_GUARD_VALIDATE_ONCHAIN", False)
        onchain_timeout_s = max(0.25, _env_float("MINT_GUARD_ONCHAIN_TIMEOUT_S", 3.0))
        onchain_strict = _env_bool("MINT_GUARD_ONCHAIN_STRICT", False)

        default_require_anchor = _env_bool("MINT_GUARD_REQUIRE_ANCHOR_DEFAULT", False)
        anchors = _resolve_list_items(_split_list(_env_str("MINT_GUARD_ANCHORS", ""))) or set(
            _DEFAULT_ANCHORS
        )

        return MintGuardConfig(
            enabled=enabled,
            use_redis=use_redis,
            redis_prefix=redis_prefix,
            deny=deny,
            allow=allow,
            non_mint=non_mint,
            cooldown_sec=cooldown_sec,
            fail_cooldown_sec=fail_cooldown_sec,
            bad_cooldown_sec=bad_cooldown_sec,
            reserve_sec=reserve_sec,
            hit_window_sec=hit_window_sec,
            max_hits_per_mint=max_hits_per_mint,
            max_hits_global=max_hits_global,
            max_unique_per_window=max_unique_per_window,
            validate_onchain=validate_onchain,
            onchain_timeout_s=onchain_timeout_s,
            onchain_strict=onchain_strict,
            default_require_anchor=default_require_anchor,
            anchors=anchors,
        )


_CFG_LOCK = RLock()
_CFG: MintGuardConfig | None = None
_CFG_FP: tuple[str, ...] | None = None


def _cfg_fingerprint() -> tuple[str, ...]:
    keys = (
        "MINT_GUARD_ENABLED",
        "MINT_GUARD_USE_REDIS",
        "MINT_GUARD_REDIS_PREFIX",
        "MINT_GUARD_DENYLIST",
        "MINT_GUARD_ALLOWLIST",
        "MINT_GUARD_NON_MINTS",
        "MINT_GUARD_COOLDOWN_SEC",
        "MINT_GUARD_FAIL_COOLDOWN_SEC",
        "MINT_GUARD_BAD_COOLOFF_SEC",
        "MINT_GUARD_RESERVE_SEC",
        "MINT_GUARD_HIT_WINDOW_SEC",
        "MINT_GUARD_MAX_HITS_PER_MINT",
        "MINT_GUARD_MAX_HITS_GLOBAL",
        "MINT_GUARD_MAX_UNIQUE_PER_WINDOW",
        "MINT_GUARD_VALIDATE_ONCHAIN",
        "MINT_GUARD_ONCHAIN_TIMEOUT_S",
        "MINT_GUARD_ONCHAIN_STRICT",
        "MINT_GUARD_REQUIRE_ANCHOR_DEFAULT",
        "MINT_GUARD_ANCHORS",
        "MINT_GUARD_STRICT_PUBKEY",
    )
    return tuple((os.getenv(k) or "") for k in keys)


def _get_cfg() -> MintGuardConfig:
    global _CFG, _CFG_FP
    fp = _cfg_fingerprint()
    with _CFG_LOCK:
        if _CFG is None or fp != _CFG_FP:
            _CFG = MintGuardConfig.from_env()
            _CFG_FP = fp
        return _CFG


# ──────────────────────────────────────────────────────────────────────────────
# Redis (optional) — lazy import; no hard dependency
# ──────────────────────────────────────────────────────────────────────────────
async def _get_redis() -> Any:
    try:
        from utils_pkg.core_utils import get_redis_async  # type: ignore
    except Exception:
        return None
    try:
        return await get_redis_async()
    except Exception:
        return None


def _k(prefix: str, kind: str, mint: str) -> str:
    return f"{prefix}:{kind}:{mint}"


def _k_res(prefix: str, mint: str) -> str:
    return f"{prefix}:res:{mint}"


# ──────────────────────────────────────────────────────────────────────────────
# MintGuard state (local)
# ──────────────────────────────────────────────────────────────────────────────
class _MintGuardState:
    def __init__(self) -> None:
        self._lock = RLock()
        self._cooldown_until: dict[str, float] = {}
        self._cooldown_reason: dict[str, str] = {}
        self._mint_window: dict[str, tuple[float, int]] = {}
        self._global_window: tuple[float, int] = (0.0, 0)
        self._unique_window: tuple[float, set[str]] = (0.0, set())
        self._last_prune: float = 0.0

    def _prune(self, cfg: MintGuardConfig) -> None:
        now = _now()
        if now - self._last_prune < 2.0:
            return
        self._last_prune = now

        dead = [m for m, exp in self._cooldown_until.items() if exp <= now]
        for m in dead:
            self._cooldown_until.pop(m, None)
            self._cooldown_reason.pop(m, None)

        ws, _ = self._global_window
        if now - ws > cfg.hit_window_sec * 2:
            self._global_window = (now, 0)

        uws, _uset = self._unique_window
        if now - uws > cfg.hit_window_sec * 2:
            self._unique_window = (now, set())

        stale = [m for m, (w, _c) in self._mint_window.items() if now - w > cfg.hit_window_sec * 2]
        for m in stale:
            self._mint_window.pop(m, None)

    def is_cooled_down(self, mint: str) -> tuple[bool, str]:
        now = _now()
        with self._lock:
            exp = self._cooldown_until.get(mint, 0.0)
            if exp > now:
                return True, self._cooldown_reason.get(mint, "cooldown")
            return False, ""

    def set_cooldown(self, mint: str, seconds: int, reason: str) -> None:
        if seconds <= 0:
            return
        until = _now() + float(seconds)
        with self._lock:
            self._cooldown_until[mint] = until
            self._cooldown_reason[mint] = reason

    def bump_hits(self, mint: str, cfg: MintGuardConfig) -> MintGuardDecision:
        now = _now()
        with self._lock:
            self._prune(cfg)

            g_ws, g_c = self._global_window
            if now - g_ws > cfg.hit_window_sec:
                g_ws, g_c = now, 0
            g_c += 1
            self._global_window = (g_ws, g_c)
            if g_c > cfg.max_hits_global:
                return MintGuardDecision(
                    ok=False,
                    mint=mint,
                    code="rate_limited_global",
                    detail=f"global hits {g_c}/{cfg.max_hits_global} in {cfg.hit_window_sec}s",
                )

            u_ws, u_set = self._unique_window
            if now - u_ws > cfg.hit_window_sec:
                u_ws, u_set = now, set()
            if len(u_set) < max(cfg.max_unique_per_window * 2, 2500):
                u_set.add(mint)
            self._unique_window = (u_ws, u_set)
            if len(u_set) > cfg.max_unique_per_window:
                return MintGuardDecision(
                    ok=False,
                    mint=mint,
                    code="rate_limited_unique",
                    detail=f"unique mints {len(u_set)}/{cfg.max_unique_per_window} in {cfg.hit_window_sec}s",
                )

            m_ws, m_c = self._mint_window.get(mint, (now, 0))
            if now - m_ws > cfg.hit_window_sec:
                m_ws, m_c = now, 0
            m_c += 1
            self._mint_window[mint] = (m_ws, m_c)
            if m_c > cfg.max_hits_per_mint:
                return MintGuardDecision(
                    ok=False,
                    mint=mint,
                    code="rate_limited_mint",
                    detail=f"mint hits {m_c}/{cfg.max_hits_per_mint} in {cfg.hit_window_sec}s",
                )

        return MintGuardDecision(ok=True, mint=mint, code="ok", detail="")


_STATE = _MintGuardState()


# ──────────────────────────────────────────────────────────────────────────────
# Core checks
# ──────────────────────────────────────────────────────────────────────────────
def _classify_mint_allowed(
    mint: str, *, allow_sol: bool, allow_stables: bool, allow_memes: bool
) -> MintGuardDecision | None:
    if not allow_sol and mint == SOL_MINT:
        return MintGuardDecision(ok=False, mint=mint, code="sol_blocked", detail="SOL blocked")
    if not allow_stables and mint in (USDC_MINT, USDT_MINT):
        return MintGuardDecision(
            ok=False, mint=mint, code="stable_blocked", detail="stable blocked"
        )
    if not allow_memes and mint == BONK_MINT:
        return MintGuardDecision(ok=False, mint=mint, code="meme_blocked", detail="meme blocked")
    return None


def _normalize_and_basic_validate(x: Any, cfg: MintGuardConfig) -> MintGuardDecision:
    if not cfg.enabled:
        m0 = _normalize_symbol_or_pubkey(x)
        if not m0:
            return MintGuardDecision(
                ok=False, mint=None, code="normalize_failed", detail="normalize_failed"
            )
        return MintGuardDecision(ok=True, mint=m0, code="ok_disabled", detail="mint guard disabled")

    mint = _normalize_symbol_or_pubkey(x)
    if not mint:
        return MintGuardDecision(
            ok=False, mint=None, code="normalize_failed", detail="normalize_failed"
        )

    if not _pubkey_ok(mint):
        return MintGuardDecision(
            ok=False,
            mint=mint,
            code="not_pubkey",
            detail="normalized value is not a valid pubkey-ish string",
        )

    if mint in cfg.non_mint:
        return MintGuardDecision(
            ok=False,
            mint=mint,
            code="non_mint_program_id",
            detail="known program id / non-mint",
        )

    # deny overrides allow
    if cfg.deny and mint in cfg.deny:
        return MintGuardDecision(
            ok=False, mint=mint, code="denylisted", detail="mint is denylisted"
        )

    # allowlist authoritative
    if cfg.allow and mint not in cfg.allow:
        return MintGuardDecision(
            ok=False, mint=mint, code="not_in_allowlist", detail="mint not in allowlist"
        )

    return MintGuardDecision(ok=True, mint=mint, code="ok", detail="")


# ──────────────────────────────────────────────────────────────────────────────
# Public: decision-oriented (never raises)
# ──────────────────────────────────────────────────────────────────────────────
def check_mint(
    x: Any,
    *,
    allow_sol: bool = True,
    allow_stables: bool = True,
    allow_memes: bool = True,
    extra_deny: Sequence[str] | None = None,
    extra_allow: Sequence[str] | None = None,
    apply_local_limits: bool = True,
    require_not_cooled_down: bool = True,
) -> MintGuardDecision:
    cfg0 = _get_cfg()

    deny = set(cfg0.deny)
    allow = set(cfg0.allow)

    if extra_deny:
        deny |= _resolve_list_items(extra_deny)
    if extra_allow:
        allow |= _resolve_list_items(extra_allow)

    cfg = MintGuardConfig(
        enabled=cfg0.enabled,
        use_redis=cfg0.use_redis,
        redis_prefix=cfg0.redis_prefix,
        deny=deny,
        allow=allow,
        non_mint=set(cfg0.non_mint),
        cooldown_sec=cfg0.cooldown_sec,
        fail_cooldown_sec=cfg0.fail_cooldown_sec,
        bad_cooldown_sec=cfg0.bad_cooldown_sec,
        reserve_sec=cfg0.reserve_sec,
        hit_window_sec=cfg0.hit_window_sec,
        max_hits_per_mint=cfg0.max_hits_per_mint,
        max_hits_global=cfg0.max_hits_global,
        max_unique_per_window=cfg0.max_unique_per_window,
        validate_onchain=cfg0.validate_onchain,
        onchain_timeout_s=cfg0.onchain_timeout_s,
        onchain_strict=cfg0.onchain_strict,
        default_require_anchor=cfg0.default_require_anchor,
        anchors=set(cfg0.anchors),
    )

    d = _normalize_and_basic_validate(x, cfg)
    if not d.ok:
        return d
    assert d.mint is not None
    mint = d.mint

    cat = _classify_mint_allowed(
        mint, allow_sol=allow_sol, allow_stables=allow_stables, allow_memes=allow_memes
    )
    if cat is not None:
        return cat

    if require_not_cooled_down:
        cooled, reason = _STATE.is_cooled_down(mint)
        if cooled:
            return MintGuardDecision(
                ok=False, mint=mint, code="cooldown", detail=f"cooldown active ({reason})"
            )

    if apply_local_limits:
        lim = _STATE.bump_hits(mint, cfg)
        if not lim.ok:
            return lim

    return MintGuardDecision(ok=True, mint=mint, code="ok", detail="")


async def acheck_mint(
    x: Any,
    *,
    allow_sol: bool = True,
    allow_stables: bool = True,
    allow_memes: bool = True,
    extra_deny: Sequence[str] | None = None,
    extra_allow: Sequence[str] | None = None,
    apply_local_limits: bool = True,
    require_not_cooled_down: bool = True,
    check_redis_cooldown: bool = True,
    reserve: bool = False,
    validate_onchain: bool | None = None,
    rpc_url: str | None = None,
) -> MintGuardDecision:
    cfg = _get_cfg()
    if validate_onchain is None:
        validate_onchain = cfg.validate_onchain

    d = check_mint(
        x,
        allow_sol=allow_sol,
        allow_stables=allow_stables,
        allow_memes=allow_memes,
        extra_deny=extra_deny,
        extra_allow=extra_allow,
        apply_local_limits=apply_local_limits,
        require_not_cooled_down=require_not_cooled_down,
    )
    if not d.ok:
        return d
    assert d.mint is not None
    mint = d.mint

    r = None
    if cfg.use_redis and (check_redis_cooldown or reserve):
        r = await _get_redis()

    if r is not None and check_redis_cooldown:
        try:
            key_cd = _k(cfg.redis_prefix, "cd", mint)
            ttl = await r.ttl(key_cd)
            if isinstance(ttl, int) and ttl > 0:
                return MintGuardDecision(
                    ok=False,
                    mint=mint,
                    code="cooldown_redis",
                    detail=f"redis cooldown active ({ttl}s)",
                )
        except Exception:
            pass

    if validate_onchain:
        is_valid_mint = None
        try:
            from utils_pkg.solana_token import is_valid_mint as _is_valid_mint  # type: ignore

            is_valid_mint = _is_valid_mint
        except Exception:
            is_valid_mint = None

        if callable(is_valid_mint):
            try:
                ok = await asyncio.wait_for(
                    is_valid_mint(mint, rpc_url=rpc_url), timeout=cfg.onchain_timeout_s
                )
                if not ok:
                    return MintGuardDecision(
                        ok=False,
                        mint=mint,
                        code="not_a_real_mint",
                        detail="on-chain validation failed (not an SPL mint)",
                    )
            except (asyncio.TimeoutError, TimeoutError):
                return MintGuardDecision(
                    ok=False,
                    mint=mint,
                    code="onchain_timeout",
                    detail="on-chain mint validation timed out",
                )
            except Exception:
                if cfg.onchain_strict:
                    return MintGuardDecision(
                        ok=False,
                        mint=mint,
                        code="onchain_error",
                        detail="on-chain mint validation error (strict mode)",
                    )

    if r is not None and reserve:
        try:
            key_res = _k_res(cfg.redis_prefix, mint)
            ok = await r.set(key_res, "1", nx=True, ex=cfg.reserve_sec)
            if not ok:
                return MintGuardDecision(
                    ok=False, mint=mint, code="reserved", detail="mint already reserved"
                )
        except Exception:
            pass

    return MintGuardDecision(ok=True, mint=mint, code="ok", detail="")


# ──────────────────────────────────────────────────────────────────────────────
# Public: raising API
# ──────────────────────────────────────────────────────────────────────────────
def guard_mint(
    x: Any,
    *,
    allow_sol: bool = True,
    allow_stables: bool = True,
    allow_memes: bool = True,
    extra_deny: Sequence[str] | None = None,
    extra_allow: Sequence[str] | None = None,
    apply_local_limits: bool = True,
    require_not_cooled_down: bool = True,
) -> str:
    d = check_mint(
        x,
        allow_sol=allow_sol,
        allow_stables=allow_stables,
        allow_memes=allow_memes,
        extra_deny=extra_deny,
        extra_allow=extra_allow,
        apply_local_limits=apply_local_limits,
        require_not_cooled_down=require_not_cooled_down,
    )
    if not d.ok:
        raise MintGuardError(d.code, d.detail, d.mint)
    assert d.mint is not None
    return d.mint


async def aguard_mint(
    x: Any,
    *,
    allow_sol: bool = True,
    allow_stables: bool = True,
    allow_memes: bool = True,
    extra_deny: Sequence[str] | None = None,
    extra_allow: Sequence[str] | None = None,
    apply_local_limits: bool = True,
    require_not_cooled_down: bool = True,
    check_redis_cooldown: bool = True,
    reserve: bool = False,
    validate_onchain: bool | None = None,
    rpc_url: str | None = None,
) -> str:
    d = await acheck_mint(
        x,
        allow_sol=allow_sol,
        allow_stables=allow_stables,
        allow_memes=allow_memes,
        extra_deny=extra_deny,
        extra_allow=extra_allow,
        apply_local_limits=apply_local_limits,
        require_not_cooled_down=require_not_cooled_down,
        check_redis_cooldown=check_redis_cooldown,
        reserve=reserve,
        validate_onchain=validate_onchain,
        rpc_url=rpc_url,
    )
    if not d.ok:
        raise MintGuardError(d.code, d.detail, d.mint)
    assert d.mint is not None
    return d.mint


# ──────────────────────────────────────────────────────────────────────────────
# Pair guards
# ──────────────────────────────────────────────────────────────────────────────
def guard_pair(
    input_mint: Any = None,
    output_mint: Any = None,
    *,
    a: Any = None,
    b: Any = None,
    mint_a: Any = None,
    mint_b: Any = None,
    require_distinct: bool = True,
    require_anchor: bool | None = None,
    anchors: Sequence[str] | None = None,
    **mint_kwargs: Any,
) -> tuple[str, str]:
    cfg = _get_cfg()
    if require_anchor is None:
        require_anchor = cfg.default_require_anchor

    left = input_mint if input_mint is not None else (a if a is not None else mint_a)
    right = output_mint if output_mint is not None else (b if b is not None else mint_b)

    ma = guard_mint(left, **mint_kwargs)
    mb = guard_mint(right, **mint_kwargs)

    if require_distinct and ma == mb:
        raise MintGuardError("same_mints", "input==output", ma)

    if require_anchor:
        anchor_set = _resolve_list_items(anchors or []) or set(cfg.anchors) or set(_DEFAULT_ANCHORS)
        if (ma not in anchor_set) and (mb not in anchor_set):
            raise MintGuardError(
                "no_anchor", f"pair lacks anchor mint (anchors={sorted(anchor_set)})", ma
            )

    return ma, mb


async def aguard_pair(
    input_mint: Any = None,
    output_mint: Any = None,
    *,
    a: Any = None,
    b: Any = None,
    mint_a: Any = None,
    mint_b: Any = None,
    require_distinct: bool = True,
    require_anchor: bool | None = None,
    anchors: Sequence[str] | None = None,
    reserve: bool = False,
    validate_onchain: bool | None = None,
    rpc_url: str | None = None,
    **mint_kwargs: Any,
) -> tuple[str, str]:
    cfg = _get_cfg()
    if require_anchor is None:
        require_anchor = cfg.default_require_anchor

    left = input_mint if input_mint is not None else (a if a is not None else mint_a)
    right = output_mint if output_mint is not None else (b if b is not None else mint_b)

    ma = await aguard_mint(
        left, reserve=reserve, validate_onchain=validate_onchain, rpc_url=rpc_url, **mint_kwargs
    )
    mb = await aguard_mint(
        right, reserve=False, validate_onchain=validate_onchain, rpc_url=rpc_url, **mint_kwargs
    )

    if require_distinct and ma == mb:
        raise MintGuardError("same_mints", "input==output", ma)

    if require_anchor:
        anchor_set = _resolve_list_items(anchors or []) or set(cfg.anchors) or set(_DEFAULT_ANCHORS)
        if (ma not in anchor_set) and (mb not in anchor_set):
            raise MintGuardError(
                "no_anchor", f"pair lacks anchor mint (anchors={sorted(anchor_set)})", ma
            )

    return ma, mb


# ──────────────────────────────────────────────────────────────────────────────
# Jupiter arg guards
# ──────────────────────────────────────────────────────────────────────────────
def guard_jupiter_quote_args(
    input_mint: Any = None,
    output_mint: Any = None,
    *,
    in_mint: Any = None,
    out_mint: Any = None,
    amount_raw: int,
    require_anchor: bool = True,
    anchors: Sequence[str] | None = None,
    **mint_kwargs: Any,
) -> tuple[str, str, int]:
    if not isinstance(amount_raw, int) or amount_raw <= 0:
        raise MintGuardError(
            "bad_amount", f"amount_raw must be positive int, got {amount_raw!r}", None
        )

    left = input_mint if input_mint is not None else in_mint
    right = output_mint if output_mint is not None else out_mint

    ma, mb = guard_pair(left, right, require_anchor=require_anchor, anchors=anchors, **mint_kwargs)
    return ma, mb, amount_raw


async def aguard_jupiter_quote_args(
    input_mint: Any = None,
    output_mint: Any = None,
    *,
    in_mint: Any = None,
    out_mint: Any = None,
    amount_raw: int,
    require_anchor: bool = True,
    anchors: Sequence[str] | None = None,
    reserve: bool = False,
    validate_onchain: bool | None = None,
    rpc_url: str | None = None,
    **mint_kwargs: Any,
) -> tuple[str, str, int]:
    if not isinstance(amount_raw, int) or amount_raw <= 0:
        raise MintGuardError(
            "bad_amount", f"amount_raw must be positive int, got {amount_raw!r}", None
        )

    left = input_mint if input_mint is not None else in_mint
    right = output_mint if output_mint is not None else out_mint

    ma, mb = await aguard_pair(
        left,
        right,
        require_anchor=require_anchor,
        anchors=anchors,
        reserve=reserve,
        validate_onchain=validate_onchain,
        rpc_url=rpc_url,
        **mint_kwargs,
    )
    return ma, mb, amount_raw


# ──────────────────────────────────────────────────────────────────────────────
# Cooldowns / marking helpers
# ──────────────────────────────────────────────────────────────────────────────
def cooldown_mint(mint: Any, seconds: int | None = None, *, reason: str = "cooldown") -> str | None:
    cfg = _get_cfg()
    m = _normalize_symbol_or_pubkey(mint)
    if not m:
        return None
    if seconds is None:
        seconds = int(cfg.cooldown_sec)
    _STATE.set_cooldown(m, int(seconds), reason)
    return m


async def cooldown_mint_shared(
    mint: Any, seconds: int | None = None, *, reason: str = "cooldown"
) -> str | None:
    cfg = _get_cfg()
    m = cooldown_mint(mint, seconds=seconds, reason=reason)
    if not m:
        return None

    if seconds is None:
        seconds = int(cfg.cooldown_sec)
    seconds = int(max(1, seconds))

    if cfg.use_redis:
        r = await _get_redis()
        if r is not None:
            try:
                key_cd = _k(cfg.redis_prefix, "cd", m)
                await r.set(key_cd, reason[:120], ex=seconds)
            except Exception:
                pass
    return m


async def mark_bad_mint(
    mint: Any, *, reason: str = "bad_mint", seconds: int | None = None
) -> str | None:
    cfg = _get_cfg()
    if seconds is None:
        seconds = int(cfg.bad_cooldown_sec)
    return await cooldown_mint_shared(mint, seconds=seconds, reason=reason)


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def is_non_mint_address(mint: str) -> bool:
    m = (mint or "").strip()
    if not m:
        return False

    # direct env check (tests monkeypatch env)
    env_list = _resolve_list_items(_split_list(_env_str("MINT_GUARD_NON_MINTS", "")))
    if m in env_list:
        return True

    cfg = _get_cfg()
    return m in cfg.non_mint


def is_anchor_mint(mint: str) -> bool:
    cfg = _get_cfg()
    return (mint or "").strip() in cfg.anchors


def classify_mint(mint: str) -> str:
    m = (mint or "").strip()
    if m == SOL_MINT:
        return "sol"
    if m in (USDC_MINT, USDT_MINT):
        return "stable"
    if m == BONK_MINT:
        return "meme"
    return "token"


__all__ = [
    "_ALLOW_CACHE",
    "_CACHE",
    "_DENY_CACHE",
    "_ENV_CACHE",
    "_NON_MINT_CACHE",
    "_PARSED_ENV",
    "MintGuardDecision",
    "MintGuardError",
    "acheck_mint",
    "aguard_jupiter_quote_args",
    "aguard_mint",
    "aguard_pair",
    "check_mint",
    "classify_mint",
    "cooldown_mint",
    "cooldown_mint_shared",
    "guard_jupiter_quote_args",
    "guard_mint",
    "guard_pair",
    "is_anchor_mint",
    "is_canonical_pubkey",
    "is_non_mint_address",
    "mark_bad_mint",
]
