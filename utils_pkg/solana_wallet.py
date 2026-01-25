from __future__ import annotations

"""
utils_pkg/solana_wallet.py — God-Tier Solana Wallet + RPC + TX Diagnostics (V3+++ LOCK-IN)
========================================================================================

Primary responsibilities
------------------------
✅ Load Keypair safely (env/file) with caching + async-safety (never logs secrets)
✅ RPC read/send separation with failover + retries + backoff + jitter (NO network at import)
✅ Budget-aware RPC accounting (before_rpc gate; after_rpc only on success)
✅ Sign + send Jupiter base64 txs (legacy + v0/versioned) reliably:
   - Decode tx_b64 -> wire bytes
   - Replace signature[0] with wallet signature over message bytes
   - sendTransaction via JSON-RPC (base64) (works for both legacy & v0)
✅ Optional diagnostics:
   - simulateTransaction on failure, surface logs/err/units (robust parsing)
   - confirmSignature polling helper
   - getTransaction + realized deltas extraction (SOL + token deltas) for PnL

Backwards compatibility exports
-------------------------------
- RPC_URL
- load_keypair(), load_keypair_async(), reset_keypair_cache()
- get_pubkey(), get_pubkey_str()
- get_balance_lamports(), get_balance_sol(), get_sol_balance
- sign_and_send_b64_tx()
- _send_b64_with_diag()   (preferred by lightning_impl when present)
- confirm_signature()
- get_tx_deltas(), get_tx_deltas_async()

Notes
-----
- Blockhash refresh for Jupiter-built txs is not feasible client-side without rebuilding.
  If we detect blockhash-expired errors, we classify them so the caller can re-quote/rebuild.
- No network at import time. (Creating requests.Session is safe.)
"""

import asyncio
import base64
import contextlib
import json
import os
import random
import time
from dataclasses import dataclass
from typing import Any

import requests

# ──────────────────────────────────────────────────────────────────────────────
# Optional base58 decode (local fallback)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from base58 import b58decode as _b58decode  # type: ignore
except Exception:  # pragma: no cover
    _B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    _B58_IDX = {c: i for i, c in enumerate(_B58_ALPHABET)}

    def _b58decode(s: str) -> bytes:
        s = (s or "").strip()
        if not s:
            return b""
        n = 0
        for ch in s:
            i = _B58_IDX.get(ch)
            if i is None:
                raise ValueError("invalid base58 char")
            n = n * 58 + i
        out = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
        pad = len(s) - len(s.lstrip("1"))
        return b"\x00" * pad + out


# ──────────────────────────────────────────────────────────────────────────────
# solders (required in your repo)
# ──────────────────────────────────────────────────────────────────────────────
from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature

# ──────────────────────────────────────────────────────────────────────────────
# Logger shim (no hard dependency)
# ──────────────────────────────────────────────────────────────────────────────
try:  # pragma: no cover
    from logging_setup import get_logger  # type: ignore

    log = get_logger(__name__)  # type: ignore
except Exception:  # pragma: no cover
    import logging

    log = logging.getLogger("solana_wallet")  # type: ignore


def _short(x: Any, n: int = 260) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


if not hasattr(log, "event"):

    def _event(name: str, **kw: Any) -> None:  # pragma: no cover
        with contextlib.suppress(Exception):
            log.info("%s %s", name, kw)

    log.event = _event  # type: ignore[attr-defined]


if not hasattr(log, "every"):

    def _every(
        key: str, n: int = 30, msg: str | None = None, **kw: Any
    ) -> None:  # pragma: no cover
        """Throttle by seconds. n is seconds."""
        try:
            now = time.monotonic()
            store = getattr(_every, "_store", {})  # type: ignore[attr-defined]
            last = float(store.get(key, 0.0))
            if (now - last) >= float(max(1, int(n))):
                store[key] = now
                _every._store = store  # type: ignore[attr-defined]
                log.event(msg or key, **kw)
        except Exception:
            return None

    log.every = _every  # type: ignore[attr-defined]


# ──────────────────────────────────────────────────────────────────────────────
# Optional budget hooks (RPC throttling + accounting)
# ──────────────────────────────────────────────────────────────────────────────
try:
    from utils_pkg import budget  # type: ignore
except Exception:  # pragma: no cover

    class _BudgetNoop:
        def before_rpc(self, method: str, *, weight: int | None = None) -> bool:
            return True

        def after_rpc(self, method: str, *, weight: int | None = None) -> None:
            return None

    budget = _BudgetNoop()  # type: ignore


def _budget_before(method: str, weight: int) -> bool:
    with contextlib.suppress(Exception):
        return bool(budget.before_rpc(method, weight=weight))
    return True


def _budget_after(method: str, weight: int) -> None:
    with contextlib.suppress(Exception):
        budget.after_rpc(method, weight=weight)


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    try:
        v = os.getenv(name)
        if v is None or str(v).strip() == "":
            return int(default)
        return int(float(str(v).strip()))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        v = os.getenv(name)
        if v is None or str(v).strip() == "":
            return float(default)
        return float(str(v).strip())
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return bool(default)
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(default)


def _sanitize_url(u: str | None) -> str:
    s = (u or "").strip()
    if s.startswith("${") and s.endswith("}"):  # templated var not expanded
        return ""
    if s in ("", "None", "null"):
        return ""
    return s.rstrip("/")


def _norm_commitment(c: str) -> str:
    c2 = (c or "").strip().lower()
    if c2 in ("processed", "confirmed", "finalized"):
        return c2
    return "confirmed"


def _dedupe_preserve(items: list[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for it in items:
        s = (it or "").strip()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _mask_pubkey(pk: str) -> str:
    s = (pk or "").strip()
    if len(s) <= 10:
        return s
    return s[:4] + "…" + s[-4:]


# ──────────────────────────────────────────────────────────────────────────────
# Constants / headers
# ──────────────────────────────────────────────────────────────────────────────
LAMPORTS_PER_SOL = 1_000_000_000
_PUBLIC_RPC = "https://api.mainnet-beta.solana.com"

_UA = {
    "user-agent": _env_str("RPC_USER_AGENT", "pnlsniper.wallet/v3"),
    "accept": "application/json",
    "content-type": "application/json",
}

# Reuse TCP connections (requests keep-alive). Safe at import time (no network).
_SESSION = requests.Session()

# ──────────────────────────────────────────────────────────────────────────────
# RPC selection + failover (SEND vs READ)
# ──────────────────────────────────────────────────────────────────────────────
_SEND_RPC_URL = (
    _sanitize_url(os.getenv("TX_RPC_URL"))
    or _sanitize_url(os.getenv("SOL_RPC_TX_URL"))
    or _sanitize_url(os.getenv("RPC_URL"))
    or _sanitize_url(os.getenv("SOL_RPC_URL"))
    or _sanitize_url(os.getenv("SOLANA_RPC_URL"))
    or _sanitize_url(os.getenv("HEALTHCHECK_RPC_URL"))
    or _PUBLIC_RPC
)

_READ_RPC_URL = (
    _sanitize_url(os.getenv("READ_RPC_URL"))
    or _sanitize_url(os.getenv("RPC_URL"))
    or _sanitize_url(os.getenv("SOL_RPC_URL"))
    or _sanitize_url(os.getenv("SOLANA_RPC_URL"))
    or _SEND_RPC_URL
)

_SEND_RPC_FALLBACKS = [
    u.strip() for u in _env_str("TX_RPC_FALLBACK_URLS", "").split(",") if u.strip()
]
_READ_RPC_FALLBACKS = [
    u.strip() for u in _env_str("READ_RPC_FALLBACK_URLS", "").split(",") if u.strip()
]

RPC_URL = _SEND_RPC_URL  # back-compat alias

# Timeouts / retries
_RPC_TIMEOUT_SEC = max(
    2.0, _env_float("ROUTER_RPC_TIMEOUT_SEC", _env_float("RPC_TIMEOUT_SEC", 12.0))
)
_RPC_RETRIES = max(0, _env_int("RPC_RETRIES", 2))
_RPC_BACKOFF_BASE = max(0.05, _env_float("RPC_BACKOFF_BASE", 0.35))
_RPC_BACKOFF_MAX = max(_RPC_BACKOFF_BASE, _env_float("RPC_BACKOFF_MAX", 2.5))
_RPC_JITTER = max(0.0, _env_float("RPC_BACKOFF_JITTER", 0.20))

# Concurrency guards for async callers
_RPC_READ_MAX_CONCURRENCY = max(1, _env_int("RPC_READ_MAX_CONCURRENCY", 24))
_RPC_SEND_MAX_CONCURRENCY = max(1, _env_int("RPC_SEND_MAX_CONCURRENCY", 12))
_READ_SEMA = asyncio.Semaphore(_RPC_READ_MAX_CONCURRENCY)
_SEND_SEMA = asyncio.Semaphore(_RPC_SEND_MAX_CONCURRENCY)

# send knobs
_TX_COMMITMENT_RAW = _norm_commitment(
    _env_str("PF_TX_SEND_COMMITMENT", "") or _env_str("SOL_COMMITMENT", "") or "confirmed"
)
_TX_SEND_MAX_RETRIES = max(0, _env_int("PF_TX_SEND_MAX_RETRIES", 3))
_TX_SEND_SKIP_PREFLIGHT_DEFAULT = _env_bool(
    "PF_TX_SKIP_PREFLIGHT", _env_bool("SOLANA_SKIP_PREFLIGHT", True)
)

# diag controls
SIMULATE_ON_SEND_FAIL = _env_bool("PF_TX_SIM_ON_FAIL", True)
CONFIRM_AFTER_SEND = _env_bool("PF_TX_CONFIRM_AFTER_SEND", False)
CONFIRM_TIMEOUT_S = max(5.0, _env_float("PF_TX_CONFIRM_TIMEOUT_S", 45.0))
CONFIRM_POLL_S = max(0.2, _env_float("PF_TX_CONFIRM_POLL_S", 0.8))

# getTransaction parsing knobs
GETTX_MAX_SUPPORTED_VERSION = max(0, _env_int("PF_GETTX_MAX_SUPPORTED_TX_VERSION", 0))

# Budget “weights”
W_READ = max(1, _env_int("RPC_WEIGHT_READ", 1))
W_SEND = max(1, _env_int("RPC_WEIGHT_SEND", 2))
W_HEAVY = max(W_READ, _env_int("RPC_WEIGHT_HEAVY", 4))

# Per-URL health / cooldown (sticky + cooldown to avoid thrash)
RPC_BASE_FAIL_LIMIT = max(1, _env_int("RPC_BASE_FAIL_LIMIT", 3))
RPC_BASE_COOLDOWN_S = max(2.0, _env_float("RPC_BASE_COOLDOWN_S", 10.0))

# Global breaker
RPC_BREAKER_ENABLED = _env_bool("RPC_BREAKER_ENABLED", True)
_RPC_FAIL_COUNT = 0
_RPC_FAIL_LIMIT = max(1, _env_int("RPC_FAIL_LIMIT", 12))
_RPC_BREAKER_COOLDOWN_S = max(5.0, _env_float("RPC_BREAKER_COOLDOWN_S", 25.0))
_RPC_BREAKER_UNTIL = 0.0

# per-kind base tracking
_RPC_BASE_FAILS_SEND: dict[str, int] = {}
_RPC_BASE_UNTIL_SEND: dict[str, float] = {}
_RPC_BASE_SCORE_SEND: dict[str, int] = {}
_RPC_LAST_GOOD_SEND: str | None = None

_RPC_BASE_FAILS_READ: dict[str, int] = {}
_RPC_BASE_UNTIL_READ: dict[str, float] = {}
_RPC_BASE_SCORE_READ: dict[str, int] = {}
_RPC_LAST_GOOD_READ: str | None = None


def _rpc_urls(kind: str) -> list[str]:
    """
    Build candidate URLs (NO network here).

    SEND: TX_RPC_URL + TX_RPC_FALLBACK_URLS
    READ: READ_RPC_URL + READ_RPC_FALLBACK_URLS

    Also (best-effort) pull from utils_pkg.rpc_failover.gather_http_candidates()
    because your repo centralizes env naming there.
    """
    urls = (
        ([_SEND_RPC_URL, *_SEND_RPC_FALLBACKS])
        if kind == "send"
        else ([_READ_RPC_URL, *_READ_RPC_FALLBACKS])
    )

    # Best-effort harvest from rpc_failover (must remain env-only; no network)
    with contextlib.suppress(Exception):
        from utils_pkg.rpc_failover import gather_http_candidates  # type: ignore

        more = gather_http_candidates()  # type: ignore[misc]
        if isinstance(more, list) and more:
            urls.extend([str(x) for x in more])

    urls = _dedupe_preserve([_sanitize_url(u) for u in urls if _sanitize_url(u)])
    if _PUBLIC_RPC not in urls:
        urls.append(_PUBLIC_RPC)
    return urls


def _base_maps(kind: str) -> tuple[dict[str, int], dict[str, float], dict[str, int]]:
    if kind == "send":
        return _RPC_BASE_FAILS_SEND, _RPC_BASE_UNTIL_SEND, _RPC_BASE_SCORE_SEND
    return _RPC_BASE_FAILS_READ, _RPC_BASE_UNTIL_READ, _RPC_BASE_SCORE_READ


def _last_good(kind: str) -> str | None:
    return _RPC_LAST_GOOD_SEND if kind == "send" else _RPC_LAST_GOOD_READ


def _remember_last_good(kind: str, url: str) -> None:
    global _RPC_LAST_GOOD_SEND, _RPC_LAST_GOOD_READ
    if kind == "send":
        _RPC_LAST_GOOD_SEND = url
    else:
        _RPC_LAST_GOOD_READ = url


def _base_allowed(kind: str, base: str) -> bool:
    _, until_map, _ = _base_maps(kind)
    return time.time() >= float(until_map.get(base, 0.0) or 0.0)


def _base_note_ok(kind: str, base: str) -> None:
    fails, until_map, score = _base_maps(kind)
    fails[base] = 0
    until_map.pop(base, None)
    score[base] = int(score.get(base, 0)) + 1
    _remember_last_good(kind, base)


def _base_note_fail(kind: str, base: str) -> None:
    fails, until_map, score = _base_maps(kind)
    n = int(fails.get(base, 0)) + 1
    fails[base] = n
    score[base] = int(score.get(base, 0)) - 1
    if n >= int(RPC_BASE_FAIL_LIMIT):
        until_map[base] = time.time() + float(RPC_BASE_COOLDOWN_S)
        log.every(
            "RPC_BASE_COOLDOWN",
            n=10,
            msg="RPC_BASE_COOLDOWN",
            kind=kind,
            base=base,
            fails=n,
            cooldown_s=RPC_BASE_COOLDOWN_S,
        )


def _bases_in_preferred_order(kind: str) -> list[str]:
    urls = _rpc_urls(kind)
    _, _, score = _base_maps(kind)
    allowed = [u for u in urls if _base_allowed(kind, u)]
    blocked = [u for u in urls if u not in allowed]
    allowed.sort(key=lambda u: int(score.get(u, 0)), reverse=True)

    sticky = _last_good(kind)
    if sticky and sticky in allowed:
        allowed = [sticky] + [u for u in allowed if u != sticky]

    return allowed + blocked


def _sleep_backoff(attempt: int) -> None:
    base = min(_RPC_BACKOFF_MAX, _RPC_BACKOFF_BASE * (2 ** max(0, int(attempt))))
    jitter = 0.0
    if _RPC_JITTER > 0:
        jitter = _RPC_JITTER * (0.5 + random.random())
    time.sleep(base + jitter)


@dataclass(frozen=True)
class RpcError(RuntimeError):
    method: str
    kind: str
    url: str
    detail: str
    code: int | None = None

    def __str__(self) -> str:
        return f"RpcError(method={self.method}, kind={self.kind}, url={self.url}, code={self.code}, detail={self.detail})"


def _rpc_breaker_check() -> None:
    if not RPC_BREAKER_ENABLED:
        return
    if time.time() < float(_RPC_BREAKER_UNTIL):
        raise RuntimeError("rpc_breaker_active")


def _rpc_breaker_success() -> None:
    global _RPC_FAIL_COUNT, _RPC_BREAKER_UNTIL
    if _RPC_FAIL_COUNT > 0:
        log.every("RPC_BREAKER_RESET", n=10, msg="RPC_BREAKER_RESET", fails=_RPC_FAIL_COUNT)
    _RPC_FAIL_COUNT = 0
    _RPC_BREAKER_UNTIL = 0.0


def _rpc_breaker_fail() -> None:
    global _RPC_FAIL_COUNT, _RPC_BREAKER_UNTIL
    _RPC_FAIL_COUNT += 1
    if int(_RPC_FAIL_LIMIT) <= _RPC_FAIL_COUNT:
        _RPC_BREAKER_UNTIL = time.time() + float(_RPC_BREAKER_COOLDOWN_S)
        log.every(
            "RPC_BREAKER_TRIP",
            n=10,
            msg="RPC_BREAKER_TRIP",
            until=_RPC_BREAKER_UNTIL,
            fails=_RPC_FAIL_COUNT,
        )


def _rpc_weight(method: str, kind: str) -> int:
    m = (method or "").strip()
    if m in ("getProgramAccounts", "getTransaction", "getBlock", "getSignaturesForAddress"):
        return W_HEAVY
    return W_SEND if kind == "send" else W_READ


def _rpc_post_raw(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    r = _SESSION.post(url, json=payload, timeout=_RPC_TIMEOUT_SEC, headers=_UA)
    r.raise_for_status()
    try:
        js = r.json()
    except Exception:
        raise RuntimeError(f"RPC JSON decode failed: {(r.text or '')[:400]}") from None
    if not isinstance(js, dict):
        raise RuntimeError(f"RPC non-dict JSON: {type(js)}")
    return js


def _parse_rpc_error(err: Any) -> tuple[int | None, str]:
    try:
        if isinstance(err, dict):
            code = err.get("code")
            msg = err.get("message") or err.get("err") or err.get("data") or err
            return (int(code) if code is not None else None), str(msg)
        return None, str(err)
    except Exception:
        return None, "rpc_error"


def _rpc_unwrap(js: dict[str, Any]) -> Any:
    if not isinstance(js, dict):
        raise RuntimeError(f"RPC bad response type: {type(js)}")
    err = js.get("error")
    if err:
        code, msg = _parse_rpc_error(err)
        raise RpcError(method="?", kind="?", url="?", detail=str(msg), code=code)
    return js.get("result")


def _rpc_call_failover(method: str, params: list, *, kind: str) -> Any:
    """
    Sync JSON-RPC call with URL failover + retry rounds + health scoring.

    Budget accounting:
      - before_rpc(): gate once per call
      - after_rpc(): called ONLY on success (valid response with no "error")
    """
    _rpc_breaker_check()

    payload = {
        "jsonrpc": "2.0",
        "id": int(time.time() * 1000) % 1_000_000_000,
        "method": method,
        "params": params,
    }
    weight = _rpc_weight(method, kind)

    if not _budget_before(method, weight):
        raise RuntimeError(f"RPC blocked by budget: {method}")

    rounds = max(1, _RPC_RETRIES + 1)
    last_err: str | None = None

    for attempt in range(rounds):
        for url in _bases_in_preferred_order(kind):
            try:
                js = _rpc_post_raw(url, payload)
                # unwrap errors as failover-able
                if js.get("error"):
                    code, msg = _parse_rpc_error(js["error"])
                    raise RpcError(method=method, kind=kind, url=url, detail=str(msg), code=code)
                res = js.get("result")
                _rpc_breaker_success()
                _base_note_ok(kind, url)
                _budget_after(method, weight)
                return res
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"
                _base_note_fail(kind, url)
                log.every(
                    "RPC_CALL_FAIL",
                    n=10,
                    msg="RPC_CALL_FAIL",
                    method=method,
                    kind=kind,
                    url=url,
                    err=_short(last_err, 240),
                )
                continue

        _rpc_breaker_fail()
        if attempt < rounds - 1:
            _sleep_backoff(attempt)

    raise RuntimeError(f"RPC {method} failed after retries; last_err={last_err}")


async def _rpc_call_failover_async(method: str, params: list, *, kind: str) -> Any:
    sema = _SEND_SEMA if kind == "send" else _READ_SEMA
    async with sema:
        return await asyncio.to_thread(_rpc_call_failover, method, params, kind=kind)


# ──────────────────────────────────────────────────────────────────────────────
# Keypair loading + caching (never logs secrets)
# ──────────────────────────────────────────────────────────────────────────────
_KP: Keypair | None = None
_KP_LOCK = asyncio.Lock()


def _parse_json_secret(raw: str, *, label: str) -> bytes:
    try:
        arr = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"{label} invalid JSON: {e!r}") from e
    if not isinstance(arr, list) or len(arr) != 64:
        raise RuntimeError(f"{label} must be a JSON array of 64 ints")
    out = bytearray()
    for i, v in enumerate(arr):
        if not isinstance(v, int) or v < 0 or v > 255:
            raise RuntimeError(f"{label}[{i}] must be int 0..255")
        out.append(v)
    return bytes(out)


def _load_keypair_from_env_or_file() -> Keypair:
    """
    Load a keypair from (first match wins):
      - WALLET_PRIVKEY_B58                  (base58 64-byte secret)
      - WALLET_PRIVKEY_B64                  (base64 64-byte secret)
      - WALLET_PRIVKEY_JSON                 (JSON array of 64 ints)
      - WALLET_KEYPAIR_JSON / KEYPAIR_JSON / WALLET_JSON (JSON array of 64 ints)
      - WALLET_PRIVATE_KEY / WALLET_KEYPAIR_PATH / KEYPAIR_PATH (file path OR json array string)
      - default: ~/.config/solana/id.json
    """
    b58 = _env_str("WALLET_PRIVKEY_B58", "")
    if b58:
        sk = _b58decode(b58)
        if len(sk) != 64:
            raise RuntimeError("WALLET_PRIVKEY_B58 must be base58 of a 64-byte secret key")
        return Keypair.from_bytes(sk)

    b64 = _env_str("WALLET_PRIVKEY_B64", "")
    if b64:
        try:
            sk = base64.b64decode(b64)
        except Exception as e:
            raise RuntimeError(f"WALLET_PRIVKEY_B64 bad base64: {e!r}") from e
        if len(sk) != 64:
            raise RuntimeError("WALLET_PRIVKEY_B64 must be base64 of a 64-byte secret key")
        return Keypair.from_bytes(sk)

    for key in ("WALLET_PRIVKEY_JSON", "WALLET_KEYPAIR_JSON", "KEYPAIR_JSON", "WALLET_JSON"):
        raw = _env_str(key, "")
        if raw:
            sk_bytes = _parse_json_secret(raw, label=key)
            return Keypair.from_bytes(sk_bytes)

    raw_or_path = (
        _env_str("WALLET_PRIVATE_KEY", "")
        or _env_str("WALLET_KEYPAIR_PATH", "")
        or _env_str("KEYPAIR_PATH", "")
        or os.path.expanduser("~/.config/solana/id.json")
    ).strip()

    # allow inline JSON array for WALLET_PRIVATE_KEY
    if raw_or_path.startswith("[") and raw_or_path.endswith("]"):
        sk_bytes = _parse_json_secret(raw_or_path, label="WALLET_PRIVATE_KEY")
        return Keypair.from_bytes(sk_bytes)

    path = os.path.expanduser(raw_or_path)
    with open(path, encoding="utf-8") as f:
        arr = json.load(f)
    if not isinstance(arr, list) or len(arr) != 64:
        raise RuntimeError(f"Keypair at {path} must contain a JSON array of 64 bytes")
    sk_bytes = bytearray()
    for i, v in enumerate(arr):
        if not isinstance(v, int) or v < 0 or v > 255:
            raise RuntimeError(f"Keypair at {path}: byte[{i}] must be int 0..255")
        sk_bytes.append(v)
    return Keypair.from_bytes(bytes(sk_bytes))


def load_keypair() -> Keypair:
    """Sync entrypoint (back-compat)."""
    global _KP
    if _KP is None:
        _KP = _load_keypair_from_env_or_file()
        with contextlib.suppress(Exception):
            log.event("WALLET_LOADED", pubkey=_mask_pubkey(str(_KP.pubkey())))
    return _KP


async def load_keypair_async() -> Keypair:
    """Async-safe loader (prevents duplicate reads in concurrent boots)."""
    global _KP
    if _KP is not None:
        return _KP
    async with _KP_LOCK:
        if _KP is None:
            _KP = _load_keypair_from_env_or_file()
            with contextlib.suppress(Exception):
                log.event("WALLET_LOADED", pubkey=_mask_pubkey(str(_KP.pubkey())))
        return _KP


def reset_keypair_cache() -> None:
    global _KP
    _KP = None


# ──────────────────────────────────────────────────────────────────────────────
# Pubkey helpers
# ──────────────────────────────────────────────────────────────────────────────
def get_pubkey(kp: Keypair) -> Pubkey:
    return kp.pubkey()


def get_pubkey_str(kp: Keypair | None = None) -> str:
    if kp is None:
        kp = load_keypair()
    return str(kp.pubkey())


# ──────────────────────────────────────────────────────────────────────────────
# Balances
# ──────────────────────────────────────────────────────────────────────────────
def get_balance_lamports(owner: str | None = None) -> int:
    """
    Sync getter (safe for callers that wrap in asyncio.to_thread()).
    Returns 0 on failure (do not throw in hot paths).
    """
    try:
        if not owner:
            owner = get_pubkey_str(load_keypair())
        res = _rpc_call_failover(
            "getBalance", [owner, {"commitment": _TX_COMMITMENT_RAW}], kind="read"
        )
        if isinstance(res, dict):
            return int(res.get("value") or 0)
        return 0
    except Exception as e:
        log.every("BAL_FAIL", n=15, msg="BAL_FAIL", err=_short(e, 220))
        return 0


async def get_balance_sol(owner: str | None = None) -> float:
    lamports = await asyncio.to_thread(get_balance_lamports, owner)
    return float(lamports) / float(LAMPORTS_PER_SOL)


# Back-compat alias
get_sol_balance = get_balance_sol


# ──────────────────────────────────────────────────────────────────────────────
# Shortvec + signing (legacy + v0 versioned)
# ──────────────────────────────────────────────────────────────────────────────
def _read_shortvec(data: bytes, offset: int = 0) -> tuple[int, int]:
    value = 0
    shift = 0
    i = offset
    while True:
        if i >= len(data):
            raise RuntimeError("shortvec decode overflow")
        b = data[i]
        i += 1
        value |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
        if shift > 63:
            raise RuntimeError("shortvec too large")
    return value, i


def _is_versioned_message(msg_bytes: bytes) -> bool:
    # Versioned message has top bit set in first byte (0x80 | version)
    return bool(msg_bytes and (msg_bytes[0] & 0x80) != 0)


def _sign_wire_tx(raw: bytes, kp: Keypair) -> tuple[bytes, str, bool, int]:
    """
    Transaction wire format:
      - shortvec sig_count
      - sig_count * 64 signature bytes
      - message bytes (legacy or versioned)

    Replace signature[0] with kp.sign_message(message_bytes).

    Returns:
      (signed_wire_bytes, signature_base58, is_versioned, sig_count)
    """
    sig_count, pos = _read_shortvec(raw, 0)
    if int(sig_count) <= 0:
        raise RuntimeError("bad tx wire format (sig_count <= 0)")

    sig_start = pos
    sig_bytes_len = 64 * int(sig_count)
    msg_start = sig_start + sig_bytes_len

    if msg_start > len(raw):
        raise RuntimeError("bad tx wire format (msg_start past end)")

    msg_bytes = raw[msg_start:]
    if not msg_bytes:
        raise RuntimeError("bad tx wire format (empty message bytes)")

    is_v = _is_versioned_message(msg_bytes)

    sig_obj: Signature = kp.sign_message(msg_bytes)
    sig0 = bytes(sig_obj)

    out = bytearray(raw)
    out[sig_start : sig_start + 64] = sig0

    sig_str = str(Signature.from_bytes(sig0))
    return bytes(out), sig_str, bool(is_v), int(sig_count)


# ──────────────────────────────────────────────────────────────────────────────
# JSON-RPC send/sim helpers (base64)
# ──────────────────────────────────────────────────────────────────────────────
def _rpc_send_transaction_b64(
    tx_b64_signed: str,
    *,
    skip_preflight: bool,
    commitment: str,
    max_retries: int,
) -> str:
    opts: dict[str, Any] = {
        "encoding": "base64",
        "skipPreflight": bool(skip_preflight),
        "preflightCommitment": _norm_commitment(commitment),
    }
    if max_retries > 0:
        opts["maxRetries"] = int(max_retries)

    res = _rpc_call_failover("sendTransaction", [tx_b64_signed, opts], kind="send")
    if not isinstance(res, str) or not res:
        raise RuntimeError(f"sendTransaction returned non-sig: {res}")
    return res


def _rpc_simulate_transaction_b64(tx_b64_signed: str) -> dict[str, Any]:
    params = [
        tx_b64_signed,
        {
            "encoding": "base64",
            "sigVerify": True,
            "commitment": _TX_COMMITMENT_RAW,
            "replaceRecentBlockhash": False,
        },
    ]
    res = _rpc_call_failover("simulateTransaction", params, kind="read")
    return res if isinstance(res, dict) else {"value": res}


def _sim_extract(sim_result: dict[str, Any]) -> tuple[Any, list[str], int | None]:
    """
    simulateTransaction result shape:
      { "context": {...}, "value": { "err": ..., "logs": [...], "unitsConsumed": n, ... } }
    Some nodes may return the value fields at top-level; handle both.
    """
    val = sim_result.get("value")
    if isinstance(val, dict):
        err = val.get("err")
        logs = val.get("logs") or []
        units = val.get("unitsConsumed")
    else:
        err = sim_result.get("err")
        logs = sim_result.get("logs") or []
        units = sim_result.get("unitsConsumed")

    if not isinstance(logs, list):
        logs = [str(logs)]
    logs2: list[str] = []
    for x in logs[:220]:
        s = str(x)
        logs2.append(s if len(s) <= 280 else (s[:280] + "…"))
    units_i = None
    with contextlib.suppress(Exception):
        units_i = int(units) if units is not None else None
    return err, logs2, units_i


def _looks_like_already_processed(err_text: str) -> bool:
    t = (err_text or "").lower()
    return any(
        k in t
        for k in (
            "already processed",
            "already in use",
            "already in ledger",
            "already confirmed",
            "transaction already exists",
        )
    )


def _classify_send_error_text(s: str) -> str:
    t = (s or "").lower()
    if "blockhashnotfound" in t or "blockhash not found" in t:
        return "blockhash_not_found"
    if "transactionexpired" in t or "block height exceeded" in t or "blockheight exceeded" in t:
        return "blockhash_expired"
    if "already processed" in t or "already in ledger" in t or "transaction already exists" in t:
        return "already_processed"
    if "insufficient funds" in t:
        return "insufficient_funds"
    if "account in use" in t:
        return "account_in_use"
    if "too many requests" in t or "429" in t:
        return "rate_limited"
    if "node is behind" in t or "behind" in t:
        return "node_behind"
    return "rpc_send_failed"


async def sign_and_send_b64_tx(
    tx_b64: str,
    kp: Keypair,
    *,
    skip_preflight: bool | None = None,
) -> str:
    """
    Decode base64 Jupiter transaction (legacy OR v0), sign it, and submit.
    Returns base58 signature. Never fakes success.

    On send failure (optional), runs simulateTransaction and surfaces useful logs.
    Also classifies common send failures (blockhash expired, rate limit, etc.).
    """
    try:
        raw = base64.b64decode(tx_b64, validate=False)
    except Exception as e:
        raise RuntimeError(f"bad tx_b64 decode: {e!r}") from e

    commitment = _TX_COMMITMENT_RAW
    sp = _TX_SEND_SKIP_PREFLIGHT_DEFAULT if skip_preflight is None else bool(skip_preflight)

    def _send_sync() -> str:
        signed_raw, sig_str, is_v, sig_count = _sign_wire_tx(raw, kp)
        signed_b64 = base64.b64encode(signed_raw).decode("utf-8")

        if sig_count > 1:
            log.every(
                "TX_MULTI_SIG_WARN",
                n=30,
                msg="TX_MULTI_SIG_WARN",
                sig_count=sig_count,
                versioned=int(is_v),
            )

        try:
            sig = _rpc_send_transaction_b64(
                signed_b64,
                skip_preflight=bool(sp),
                commitment=commitment,
                max_retries=int(_TX_SEND_MAX_RETRIES),
            )
            return sig or sig_str
        except Exception as e:
            if _looks_like_already_processed(str(e)):
                return sig_str
            raise

    try:
        sig = await asyncio.to_thread(_send_sync)
        with contextlib.suppress(Exception):
            log.event("TX_SENT", sig=sig, commitment=commitment, skip_preflight=int(bool(sp)))
        if CONFIRM_AFTER_SEND:
            ok = await confirm_signature(
                sig,
                commitment=commitment,
                timeout_s=CONFIRM_TIMEOUT_S,
                poll_interval_s=CONFIRM_POLL_S,
            )
            with contextlib.suppress(Exception):
                log.event("TX_CONFIRM_POSTSEND", sig=sig, ok=int(bool(ok)))
        return sig
    except Exception as e:
        cls = _classify_send_error_text(str(e))
        if SIMULATE_ON_SEND_FAIL:
            try:
                signed_raw, sig_str, _is_v, _sig_count = _sign_wire_tx(raw, kp)
                signed_b64 = base64.b64encode(signed_raw).decode("utf-8")
                sim = await asyncio.to_thread(_rpc_simulate_transaction_b64, signed_b64)
                err, logs, units = _sim_extract(sim if isinstance(sim, dict) else {"value": sim})
                raise RuntimeError(
                    f"sendTransaction failed ({cls}): {e!r}; simulate err={err}; units={units}; logs={logs}"
                ) from e
            except Exception as e2:
                raise RuntimeError(
                    f"sendTransaction failed ({cls}): {e!r}; simulate secondary error: {e2!r}"
                ) from e
        raise RuntimeError(f"sendTransaction failed ({cls}): {e!r}") from e


# Preferred diagnostic sender (used by lightning_impl if present)
async def _send_b64_with_diag(tx_b64: str, *, skip_preflight: bool | None = None) -> str:
    kp = load_keypair()
    return await sign_and_send_b64_tx(tx_b64, kp, skip_preflight=skip_preflight)


# ──────────────────────────────────────────────────────────────────────────────
# Confirm helpers
# ──────────────────────────────────────────────────────────────────────────────
def _status_from_sig(sig: str) -> dict[str, Any]:
    # Some nodes need searchTransactionHistory for older sigs
    try:
        res = _rpc_call_failover(
            "getSignatureStatuses", [[sig], {"searchTransactionHistory": True}], kind="read"
        )
    except Exception:
        res = _rpc_call_failover("getSignatureStatuses", [[sig]], kind="read")

    if not isinstance(res, dict):
        return {}
    val = res.get("value") or []
    return val[0] if isinstance(val, list) and val else {}


def _is_confirmed_ok(status: dict[str, Any], want: str) -> bool:
    if not status:
        return False
    if status.get("err"):
        return False

    want = _norm_commitment(want)
    lvl = (status.get("confirmationStatus") or "").lower()

    if lvl == "finalized":
        return want in ("processed", "confirmed", "finalized")
    if lvl == "confirmed":
        return want in ("processed", "confirmed")
    if lvl == "processed":
        return want == "processed"

    conf = status.get("confirmations")
    if conf is None:
        return want in ("processed", "confirmed", "finalized")

    with contextlib.suppress(Exception):
        conf_i = int(conf)
        if want == "processed":
            return conf_i >= 1
        return conf_i >= 2

    return False


async def confirm_signature(
    signature: str,
    *,
    commitment: str = "confirmed",
    timeout_s: float = 60.0,
    poll_interval_s: float = 0.8,
) -> bool:
    want = _norm_commitment(commitment)
    deadline = time.monotonic() + float(timeout_s)
    while time.monotonic() < deadline:
        try:
            st = await asyncio.to_thread(_status_from_sig, signature)
            if st and st.get("err"):
                return False
            if _is_confirmed_ok(st, want):
                return True
        except Exception:
            pass
        await asyncio.sleep(float(poll_interval_s))
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Tx inspection helpers (PnL-grade deltas)
# ──────────────────────────────────────────────────────────────────────────────
def _rpc_get_transaction(sig: str) -> dict[str, Any] | None:
    params = [
        sig,
        {
            "encoding": "jsonParsed",
            "commitment": _TX_COMMITMENT_RAW,
            "maxSupportedTransactionVersion": int(GETTX_MAX_SUPPORTED_VERSION),
        },
    ]
    res = _rpc_call_failover("getTransaction", params, kind="read")
    return res if isinstance(res, dict) else None


def _extract_account_keys(tx: dict[str, Any]) -> list[str]:
    try:
        msg = ((tx.get("transaction") or {}).get("message")) or {}
        keys = msg.get("accountKeys") or []
        out: list[str] = []
        for k in keys:
            if isinstance(k, str):
                out.append(k)
            elif isinstance(k, dict) and k.get("pubkey"):
                out.append(str(k["pubkey"]))
        return out
    except Exception:
        return []


def get_tx_deltas(signature: str, owner: str | None = None) -> dict[str, Any]:
    """
    Best-effort realized deltas:
      - sol_delta_lamports/sol_delta_sol for owner
      - fee_lamports
      - token_deltas by mint (SUMMED delta_ui across all owner token accounts)
    """
    owner = (owner or get_pubkey_str(load_keypair())).strip()
    tx = _rpc_get_transaction(signature)
    if not tx:
        return {"ok": False, "sig": signature, "owner": owner, "err": "no_tx"}

    meta = tx.get("meta") or {}
    keys = _extract_account_keys(tx)

    out: dict[str, Any] = {
        "ok": True,
        "sig": signature,
        "owner": owner,
        "slot": tx.get("slot"),
        "blockTime": tx.get("blockTime"),
        "meta_err": meta.get("err"),
        "fee_lamports": meta.get("fee"),
    }

    # SOL delta (includes fee + transfers)
    try:
        pre = meta.get("preBalances") or []
        post = meta.get("postBalances") or []
        idx = keys.index(owner) if owner in keys else -1
        if idx >= 0 and idx < len(pre) and idx < len(post):
            sol_delta = int(post[idx]) - int(pre[idx])
            out["sol_delta_lamports"] = sol_delta
            out["sol_delta_sol"] = sol_delta / float(LAMPORTS_PER_SOL)
        else:
            out["sol_delta_lamports"] = None
            out["sol_delta_sol"] = None
    except Exception:
        out["sol_delta_lamports"] = None
        out["sol_delta_sol"] = None

    def _ui_amt(x: dict[str, Any]) -> float:
        ui = x.get("uiTokenAmount") or {}
        s = ui.get("uiAmountString")
        if s is not None:
            with contextlib.suppress(Exception):
                return float(s)
        v = ui.get("uiAmount")
        with contextlib.suppress(Exception):
            return float(v) if v is not None else 0.0
        return 0.0

    def _by_owner(lst: Any) -> list[dict[str, Any]]:
        if not isinstance(lst, list):
            return []
        res: list[dict[str, Any]] = []
        for it in lst:
            if isinstance(it, dict) and (it.get("owner") == owner) and it.get("mint"):
                res.append(it)
        return res

    pre_tb = _by_owner(meta.get("preTokenBalances"))
    post_tb = _by_owner(meta.get("postTokenBalances"))

    def _key(it: dict[str, Any]) -> tuple[str, int]:
        mint = str(it.get("mint"))
        ai = it.get("accountIndex")
        with contextlib.suppress(Exception):
            ai = int(ai)
        return mint, int(ai) if ai is not None else -1

    pre_map: dict[tuple[str, int], dict[str, Any]] = {_key(x): x for x in pre_tb}
    post_map: dict[tuple[str, int], dict[str, Any]] = {_key(x): x for x in post_tb}
    keys2 = sorted(set(pre_map.keys()) | set(post_map.keys()))

    detailed: dict[str, Any] = {}
    summed: dict[str, Any] = {}

    for k2 in keys2:
        a = pre_map.get(k2) or {}
        b = post_map.get(k2) or {}
        mint, acct_idx = k2
        pre_ui = _ui_amt(a)
        post_ui = _ui_amt(b)
        dec = (b.get("uiTokenAmount") or {}).get("decimals") or (a.get("uiTokenAmount") or {}).get(
            "decimals"
        )
        d = {
            "mint": mint,
            "accountIndex": acct_idx,
            "pre_ui": pre_ui,
            "post_ui": post_ui,
            "delta_ui": (post_ui - pre_ui),
            "decimals": dec,
        }
        detailed[f"{mint}:{acct_idx}"] = d

        srec = summed.get(mint) or {"delta_ui": 0.0, "pre_ui": 0.0, "post_ui": 0.0, "decimals": dec}
        srec["pre_ui"] = float(srec.get("pre_ui", 0.0)) + float(pre_ui)
        srec["post_ui"] = float(srec.get("post_ui", 0.0)) + float(post_ui)
        srec["delta_ui"] = float(srec.get("delta_ui", 0.0)) + float(post_ui - pre_ui)
        if srec.get("decimals") is None:
            srec["decimals"] = dec
        summed[mint] = srec

    out["token_deltas_detailed"] = detailed
    out["token_deltas"] = summed
    return out


async def get_tx_deltas_async(signature: str, owner: str | None = None) -> dict[str, Any]:
    return await asyncio.to_thread(get_tx_deltas, signature, owner)


__all__ = [
    "LAMPORTS_PER_SOL",
    "RPC_URL",
    "_send_b64_with_diag",
    "confirm_signature",
    "get_balance_lamports",
    "get_balance_sol",
    "get_pubkey",
    "get_pubkey_str",
    "get_sol_balance",
    "get_tx_deltas",
    "get_tx_deltas_async",
    "load_keypair",
    "load_keypair_async",
    "reset_keypair_cache",
    "sign_and_send_b64_tx",
]
