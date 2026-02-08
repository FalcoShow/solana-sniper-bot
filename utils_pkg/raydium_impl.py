from __future__ import annotations

"""
utils_pkg/raydium_impl.py — Raydium Trade API Swapper (V3+++ God-Tier Execution Layer) — LOCK-IN
================================================================================================

Purpose
-------
Direct Raydium execution fallback (and optional primary) behind utils_pkg/router.py.

Implements Raydium "Trade API" (transaction-v1) flow:
  1) GET  /compute/swap-base-in   (or swap-base-out)
  2) POST /transaction/swap-base-in (build unsigned txs)
  3) Sign + send tx(s)

LOCK-IN Guarantees
------------------
✅ No network at import time (lazy clients).
✅ Async HTTP via httpx; retries + QPS gate + host failover (soft circuit breaker).
✅ DRYRUN-safe:
      - DRYRUN=1 and RAY_DRYRUN_BUILD_TX=0 => never builds tx, never sends
      - DRYRUN=1 and RAY_DRYRUN_BUILD_TX=1 => builds tx but never sends
✅ Backwards compatible entrypoints/signatures:
      - buy_via_raydium(...)  -> Optional[str] unless receipt/meta requested
      - sell_via_raydium(...) -> Optional[str] unless receipt requested
      - return_receipt=True OR RAY_RETURN_RECEIPTS=1 -> receipt dict
      - return_meta=True (buy only) -> (sig, meta_dict)
✅ Router-friendly quote helpers:
      - get_buy_quote(...) / get_sell_quote(...) return compute response dict
✅ SELL inputAccount resolution:
      - env override RAY_INPUT_ACCOUNT first
      - ATA derivation if solders available (optional verify)
      - fallback RPC getTokenAccountsByOwner

Notes
-----
- Raydium Trade API returns one or more base64 transactions. We sign+send sequentially.
- BUY: SOL -> token (wrapSol=True)
- SELL: token -> SOL (unwrapSol=True)
- We accept several response shapes (payload can be nested under "data", etc.)
"""

import asyncio
import contextlib
import inspect
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Protocol, cast

import httpx


# -----------------------------------------------------------------------------
# Logging (no monkey-patching; repo-compatible; never required)
# -----------------------------------------------------------------------------
class _LogLike(Protocol):
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def info(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def warning(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def error(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> Any: ...


def _maybe_import_attr(module_name: str, attr: str) -> Any | None:
    try:
        import importlib

        mod = importlib.import_module(module_name)
        return getattr(mod, attr, None)
    except Exception:
        return None


def _build_logger() -> _LogLike:
    import logging

    base: Any = logging.getLogger("utils.raydium_impl")

    # Prefer project logger shims if present
    for mod_name in ("utils_pkg.log_compat", "utils_pkg.logging_setup", "logging_setup"):
        get_logger = _maybe_import_attr(mod_name, "get_logger")
        if callable(get_logger):
            try:
                base = get_logger("utils.raydium_impl")
                break
            except Exception:
                base = logging.getLogger("utils.raydium_impl")

    # Optional EventLogger wrapper (if available)
    get_event_logger = _maybe_import_attr("utils_pkg.event_logger", "get_event_logger")
    if callable(get_event_logger):
        try:
            wrapped = get_event_logger(base)
            return cast(_LogLike, wrapped)
        except Exception:
            return cast(_LogLike, base)

    return cast(_LogLike, base)


LOG: _LogLike = _build_logger()
_EVERY_STORE: dict[str, float] = {}


def _mono() -> float:
    return time.monotonic()


def _short(x: Any, n: int = 280) -> str:
    try:
        s = str(x)
        return s if len(s) <= n else (s[:n] + "…")
    except Exception:
        return "<?>"


def _log_event(name: str, **kw: Any) -> None:
    """
    Prefer EventLogger .event() if present; else log.info.
    Never raises.
    """
    try:
        ev = getattr(LOG, "event", None)
        if callable(ev):
            ev(name, **kw)
            return
    except Exception:
        pass
    try:
        LOG.info("%s %s", name, kw)
    except Exception:
        return


def _every(key: str, n: int = 30, msg: str | None = None, **kw: Any) -> None:
    """
    Rate-limited event emission without monkey patching the logger.
    """
    try:
        now = _mono()
        last = float(_EVERY_STORE.get(key, 0.0))
        if (now - last) >= float(max(1, int(n))):
            _EVERY_STORE[key] = now
            _log_event(msg or key, **kw)
    except Exception:
        return


def _warn(name: str, **kw: Any) -> None:
    _log_event(name, **kw)


# -----------------------------------------------------------------------------
# Optional budget hooks (HTTP/RPC throttling)
# -----------------------------------------------------------------------------
class _BudgetLike(Protocol):
    def before_quote(self, units: int = 1) -> bool: ...
    def after_quote(self, units: int = 1) -> None: ...
    def before_rpc(self, method: str, *, weight: int | None = None) -> bool: ...
    def after_rpc(self, method: str, *, weight: int | None = None) -> None: ...


class _BudgetNoop:
    def before_quote(self, units: int = 1) -> bool:
        return True

    def after_quote(self, units: int = 1) -> None:
        return None

    def before_rpc(self, method: str, *, weight: int | None = None) -> bool:
        return True

    def after_rpc(self, method: str, *, weight: int | None = None) -> None:
        return None


# IMPORTANT: keep `budget` typed as _BudgetLike, never as a Module.
try:
    from utils_pkg import budget as _budget_mod  # type: ignore

    budget: _BudgetLike = cast(_BudgetLike, _budget_mod)
except Exception:  # pragma: no cover
    budget = _BudgetNoop()


# -----------------------------------------------------------------------------
# Mint normalization (optional)
# -----------------------------------------------------------------------------
try:
    from utils_pkg.mints import normalize_mint as normalize_mint  # type: ignore
except Exception:  # pragma: no cover

    def normalize_mint(x: str) -> str:  # type: ignore
        return (x or "").strip()


# -----------------------------------------------------------------------------
# Solana wallet utilities (expected to exist in-repo)
# -----------------------------------------------------------------------------
from utils_pkg.solana_wallet import get_pubkey_str, load_keypair, sign_and_send_b64_tx

try:  # pragma: no cover
    from utils_pkg.solana_wallet import _send_b64_with_diag as _send_b64_with_diag  # type: ignore
except Exception:  # pragma: no cover
    _send_b64_with_diag = None  # type: ignore


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v).strip()


def _env_int(name: str, default: int) -> int:
    try:
        return int(float((_env_str(name, str(default)) or str(default)).strip()))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float((_env_str(name, str(default)) or str(default)).strip())
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env_str(name, "")
    if not v:
        return default
    s = v.strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return default


def _clamp_i(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x


async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
LAMPORTS_PER_SOL = 1_000_000_000

SOL_MINT = _env_str("SOL_MINT", "So11111111111111111111111111111111111111112")
USDC_MINT = _env_str("USDC_MINT", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")

RAY_BASE_HOST = (
    _env_str("RAY_BASE_HOST", _env_str("RAYDIUM_BASE_HOST", "https://api-v3.raydium.io"))
).rstrip("/")

RAY_SWAP_HOST = (
    _env_str("RAY_SWAP_HOST", _env_str("RAYDIUM_SWAP_HOST", "https://transaction-v1.raydium.io"))
).rstrip("/")
RAY_ALT_SWAP_HOSTS_RAW = _env_str("RAY_ALT_SWAP_HOSTS", "")
_RAY_HOSTS = [RAY_SWAP_HOST] + [
    u.strip().rstrip("/")
    for u in (RAY_ALT_SWAP_HOSTS_RAW.split(",") if RAY_ALT_SWAP_HOSTS_RAW else [])
    if u.strip()
]
RAY_SWAP_HOSTS: list[str] = list(dict.fromkeys([h for h in _RAY_HOSTS if h])) or [RAY_SWAP_HOST]

RAY_SWAP_MODE = _env_str("RAY_SWAP_MODE", "base_in").lower().strip()
if RAY_SWAP_MODE not in ("base_in", "base_out"):
    RAY_SWAP_MODE = "base_in"

RAY_TX_VERSION = _env_str("RAY_TX_VERSION", "V0").upper().strip()
if RAY_TX_VERSION not in ("V0", "LEGACY"):
    RAY_TX_VERSION = "V0"

DEFAULT_SLIPPAGE_BPS = _clamp_i(_env_int("PF_SLIPPAGE_BPS", _env_int("SLIPPAGE_BPS", 30)), 1, 5000)

RAY_CU_PRICE_MICRO = _env_str("RAY_CU_PRICE_MICRO", _env_str("PF_COMPUTE_UNIT_PRICE_MICRO", ""))
RAY_PRIORITY_TIER = _env_str("RAY_PRIORITY_TIER", "h").lower().strip()

HTTP_TIMEOUT_SEC = max(5.0, _env_float("RAY_HTTP_TIMEOUT_SEC", 20.0))
HTTP_RETRY_MAX = max(0, _env_int("RAY_HTTP_RETRY_MAX", 2))
HTTP_RETRY_BACKOFF_SEC = max(0.05, _env_float("RAY_HTTP_RETRY_BACKOFF_SEC", 0.8))
HTTP_RETRY_JITTER_SEC = max(0.0, _env_float("RAY_HTTP_RETRY_JITTER_SEC", 0.25))

RAY_MAX_QPS = max(1, _env_int("RAY_MAX_QPS", 2))
_RAY_MIN_INTERVAL = 1.0 / float(RAY_MAX_QPS)
_RATE_LOCK = asyncio.Lock()
_LAST_MONO: float = 0.0

_RAY_HTTP_CONCURRENCY = _clamp_i(_env_int("RAY_HTTP_CONCURRENCY", 8), 1, 64)
_HTTP_SEMA = asyncio.Semaphore(_RAY_HTTP_CONCURRENCY)

DRY_RUN = _env_bool("DRYRUN", False) or _env_bool("DRY_RUN", False) or _env_bool("IS_DRYRUN", False)
RAY_RETURN_RECEIPTS = _env_bool("RAY_RETURN_RECEIPTS", False)
RAY_INCLUDE_SWAP_RESPONSE = _env_bool("RAY_INCLUDE_SWAP_RESPONSE", True)
RAY_DRYRUN_BUILD_TX = _env_bool("RAY_DRYRUN_BUILD_TX", False)

DISABLE_BUYS = _env_bool("DISABLE_BUYS", False)
DISABLE_SELLS = _env_bool("DISABLE_SELLS", False)
KILL_SWITCH = _env_bool("KILL_SWITCH", False)

SEND_SKIP_PREFLIGHT = _env_bool("SOLANA_SKIP_PREFLIGHT", True)

SOLANA_RPC_URL = _env_str(
    "SOLANA_RPC_URL", _env_str("SOL_RPC_URL", _env_str("RPC_URL", _env_str("SOLANA_RPC", "")))
)
RAY_INPUT_ACCOUNT_OVERRIDE = _env_str("RAY_INPUT_ACCOUNT", "")
RAY_ATA_VERIFY_EXISTS = _env_bool("RAY_ATA_VERIFY_EXISTS", False)

_PRIORITY_CACHE_TTL = max(5.0, _env_float("RAY_PRIORITY_CACHE_TTL_SEC", 20.0))
_priority_cache: tuple[float, str | None] = (0.0, None)

RPC_TIMEOUT_SEC = max(3.0, _env_float("RAY_RPC_TIMEOUT_SEC", 12.0))
RPC_RETRY_MAX = max(0, _env_int("RAY_RPC_RETRY_MAX", 1))
RPC_RETRY_BACKOFF_SEC = max(0.05, _env_float("RAY_RPC_RETRY_BACKOFF_SEC", 0.6))


# -----------------------------------------------------------------------------
# HTTP clients (lazy singletons, race-safe)
# -----------------------------------------------------------------------------
_client: httpx.AsyncClient | None = None
_rpc_client: httpx.AsyncClient | None = None
_client_lock = asyncio.Lock()
_rpc_lock = asyncio.Lock()


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is not None and not _client.is_closed:
        return _client
    async with _client_lock:
        if _client is None or _client.is_closed:
            limits = httpx.Limits(max_keepalive_connections=24, max_connections=80)
            _client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=5.0, read=HTTP_TIMEOUT_SEC, write=HTTP_TIMEOUT_SEC, pool=5.0
                ),
                limits=limits,
                headers={"accept": "application/json", "user-agent": "pnlsniper/raydium"},
                follow_redirects=True,
            )
        return _client


async def _get_rpc_client() -> httpx.AsyncClient:
    global _rpc_client
    if _rpc_client is not None and not _rpc_client.is_closed:
        return _rpc_client
    async with _rpc_lock:
        if _rpc_client is None or _rpc_client.is_closed:
            limits = httpx.Limits(max_keepalive_connections=8, max_connections=24)
            _rpc_client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=5.0, read=RPC_TIMEOUT_SEC, write=RPC_TIMEOUT_SEC, pool=5.0
                ),
                limits=limits,
                headers={"content-type": "application/json", "user-agent": "pnlsniper/raydium-rpc"},
                follow_redirects=True,
            )
        return _rpc_client


async def close_http() -> None:
    global _client, _rpc_client
    if _client is not None and not _client.is_closed:
        with contextlib.suppress(Exception):
            await _client.aclose()
    _client = None
    if _rpc_client is not None and not _rpc_client.is_closed:
        with contextlib.suppress(Exception):
            await _rpc_client.aclose()
    _rpc_client = None


# -----------------------------------------------------------------------------
# Host failover (soft circuit breaker)
# -----------------------------------------------------------------------------
@dataclass
class _HostState:
    fails: int = 0
    banned_until_mono: float = 0.0


_HOST_STATE: dict[str, _HostState] = {}
_HOST_LOCK = asyncio.Lock()


async def _host_is_banned(host: str) -> bool:
    async with _HOST_LOCK:
        st = _HOST_STATE.get(host)
        if not st:
            return False
        return st.banned_until_mono > _mono()


async def _host_mark_ok(host: str) -> None:
    async with _HOST_LOCK:
        st = _HOST_STATE.get(host)
        if not st:
            _HOST_STATE[host] = _HostState(fails=0, banned_until_mono=0.0)
            return
        st.fails = 0
        st.banned_until_mono = 0.0


async def _host_mark_fail(host: str, *, why: str = "") -> None:
    ban: float
    fails: int
    async with _HOST_LOCK:
        st = _HOST_STATE.get(host)
        if not st:
            st = _HostState()
            _HOST_STATE[host] = st
        st.fails = int(st.fails) + 1
        ban = min(60.0, 1.5 * (2.0 ** min(6, st.fails)))
        st.banned_until_mono = _mono() + ban
        fails = st.fails
    _every("RAY_HOST_BAN", n=10, msg="RAY_HOST_BAN", host=host, fails=fails, ban_s=ban, why=why)


# -----------------------------------------------------------------------------
# Rate limiting + retry helpers
# -----------------------------------------------------------------------------
async def _qps_gate() -> None:
    global _LAST_MONO
    async with _RATE_LOCK:
        now = _mono()
        wait_for = _RAY_MIN_INTERVAL - (now - _LAST_MONO)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _LAST_MONO = _mono()


def _is_transient_status(code: int) -> bool:
    return int(code) in (408, 425, 429, 500, 502, 503, 504)


async def _sleep_backoff(attempt: int, *, base: float, jitter: float) -> None:
    b = float(base) * (2.0 ** max(0, int(attempt)))
    j = 0.0
    if jitter > 0:
        j = float(jitter) * (0.6 + (os.urandom(1)[0] / 255.0) * 0.8)
    await asyncio.sleep(b + j)


def _safe_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except Exception:
        with contextlib.suppress(Exception):
            return {"text": (r.text or "")[:2000]}
        return {"text": ""}


async def _http_json(
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    await _qps_gate()
    c = await _get_client()

    for attempt in range(0, int(HTTP_RETRY_MAX) + 1):
        try:
            async with _HTTP_SEMA:
                if method.upper() == "GET":
                    r = await c.get(url, params=params)
                else:
                    r = await c.post(url, json=json_body)

            body = _safe_json(r)

            if r.status_code in (200, 201):
                if isinstance(body, dict):
                    return body
                return {"data": body}

            if _is_transient_status(r.status_code) and attempt < int(HTTP_RETRY_MAX):
                _warn(
                    "RAY_HTTP_RETRY",
                    url=url,
                    status=r.status_code,
                    attempt=attempt,
                    body=_short(body, 240),
                )
                await _sleep_backoff(
                    attempt, base=HTTP_RETRY_BACKOFF_SEC, jitter=HTTP_RETRY_JITTER_SEC
                )
                continue

            raise RuntimeError(f"ray_http_{r.status_code}: {_short(body, 520)}")

        except (httpx.TimeoutException, httpx.NetworkError) as e:
            if attempt < int(HTTP_RETRY_MAX):
                _warn("RAY_NET_RETRY", url=url, attempt=attempt, err=_short(e, 240))
                await _sleep_backoff(
                    attempt, base=HTTP_RETRY_BACKOFF_SEC, jitter=HTTP_RETRY_JITTER_SEC
                )
                continue
            raise

    raise RuntimeError("ray_http_failed")


# -----------------------------------------------------------------------------
# Keypair cache
# -----------------------------------------------------------------------------
_KP_LOCK = asyncio.Lock()
_KP: Any | None = None
_PUBKEY_STR: str | None = None


async def _get_keypair_cached() -> tuple[Any, str]:
    global _KP, _PUBKEY_STR
    if _KP is not None and _PUBKEY_STR:
        return _KP, _PUBKEY_STR
    async with _KP_LOCK:
        if _KP is not None and _PUBKEY_STR:
            return _KP, _PUBKEY_STR
        _KP = load_keypair()
        _PUBKEY_STR = get_pubkey_str(_KP)
        return _KP, _PUBKEY_STR


# -----------------------------------------------------------------------------
# Response-shape normalizers (robust across Raydium variants)
# -----------------------------------------------------------------------------
def _unwrap_data(js: dict[str, Any]) -> dict[str, Any]:
    data = js.get("data")
    if isinstance(data, dict):
        return data
    return js


def _extract_swap_response(compute_js: dict[str, Any]) -> dict[str, Any]:
    """
    Transaction endpoint expects `swapResponse` object.
    Some compute responses are already the swapResponse; some wrap under data/swapResponse.
    """
    inner = _unwrap_data(compute_js)
    sr = inner.get("swapResponse")
    if isinstance(sr, dict):
        return sr
    return inner


def _extract_txs(build_js: dict[str, Any]) -> list[str]:
    data = build_js.get("data")
    if isinstance(data, list):
        out: list[str] = []
        for item in data:
            if isinstance(item, dict) and item.get("transaction"):
                out.append(str(item["transaction"]))
        if out:
            return out

    txs = build_js.get("transactions")
    if isinstance(txs, list):
        out2 = [str(x) for x in txs if x]
        if out2:
            return out2

    inner = _unwrap_data(build_js)
    if isinstance(inner, dict):
        data2 = inner.get("data")
        if isinstance(data2, list):
            out3: list[str] = []
            for item in data2:
                if isinstance(item, dict) and item.get("transaction"):
                    out3.append(str(item["transaction"]))
            if out3:
                return out3

    raise RuntimeError(f"ray_build_missing_txs: {_short(build_js, 520)}")


# -----------------------------------------------------------------------------
# Priority fee (optional helper)
# -----------------------------------------------------------------------------
async def _fetch_priority_fee_micro() -> str | None:
    global _priority_cache

    if RAY_CU_PRICE_MICRO:
        return str(RAY_CU_PRICE_MICRO)

    now = _mono()
    exp, cached = _priority_cache
    if cached is not None and exp >= now:
        return cached

    url = f"{RAY_BASE_HOST}/priority-fee"
    try:
        js = await _http_json("GET", url)
        inner = _unwrap_data(js)
        default = inner.get("default") if isinstance(inner, dict) else None
        if isinstance(default, dict):
            val = (
                default.get(RAY_PRIORITY_TIER)
                or default.get("h")
                or default.get("m")
                or default.get("vh")
            )
            if val is not None:
                micro = str(val)
                _priority_cache = (now + float(_PRIORITY_CACHE_TTL), micro)
                return micro
    except Exception as e:
        _warn("RAY_PRIORITY_FEE_FAIL", err=_short(e, 220))

    _priority_cache = (now + float(_PRIORITY_CACHE_TTL), None)
    return None


# -----------------------------------------------------------------------------
# RPC helpers (token account resolution for SELL)
# -----------------------------------------------------------------------------
async def _rpc(method: str, params: list[Any]) -> Any:
    if not SOLANA_RPC_URL:
        raise RuntimeError("missing_SOLANA_RPC_URL")

    if not budget.before_rpc(method):
        raise RuntimeError("rpc blocked by budget")

    c = await _get_rpc_client()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}

    ok = False
    try:
        for attempt in range(0, int(RPC_RETRY_MAX) + 1):
            try:
                r = await c.post(SOLANA_RPC_URL, json=payload)
            except (httpx.TimeoutException, httpx.NetworkError) as err:
                if attempt < int(RPC_RETRY_MAX):
                    await _sleep_backoff(
                        attempt, base=RPC_RETRY_BACKOFF_SEC, jitter=HTTP_RETRY_JITTER_SEC
                    )
                    continue
                raise RuntimeError(f"rpc_transport: {_short(err, 220)}") from err

            js = _safe_json(r)
            if not isinstance(js, dict):
                raise RuntimeError("rpc_bad_json")

            if js.get("error"):
                raise RuntimeError(f"rpc_error: {_short(js.get('error'), 520)}")

            ok = True
            return js.get("result")

        raise RuntimeError("rpc_failed")
    finally:
        if ok:
            with contextlib.suppress(Exception):
                budget.after_rpc(method)


def _try_derive_ata(owner: str, mint: str) -> str | None:
    try:
        from solders.pubkey import Pubkey  # type: ignore

        associated_token_program_id = Pubkey.from_string(
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
        )
        token_program_id = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        owner_pk = Pubkey.from_string(owner)
        mint_pk = Pubkey.from_string(mint)

        ata, _bump = Pubkey.find_program_address(
            [bytes(owner_pk), bytes(token_program_id), bytes(mint_pk)],
            associated_token_program_id,
        )
        return str(ata)
    except Exception:
        return None


async def _rpc_account_exists(pubkey: str) -> bool:
    try:
        res = await _rpc("getAccountInfo", [pubkey, {"encoding": "base64"}])
        if isinstance(res, dict) and res.get("value") is not None:
            return True
    except Exception:
        pass
    return False


async def _resolve_input_token_account(owner: str, mint: str) -> str:
    if RAY_INPUT_ACCOUNT_OVERRIDE:
        return RAY_INPUT_ACCOUNT_OVERRIDE

    ata = _try_derive_ata(owner, mint)
    if ata:
        if not RAY_ATA_VERIFY_EXISTS:
            return ata
        if await _rpc_account_exists(ata):
            return ata

    res = await _rpc(
        "getTokenAccountsByOwner",
        [owner, {"mint": mint}, {"encoding": "jsonParsed"}],
    )
    value = (res or {}).get("value") if isinstance(res, dict) else None
    if isinstance(value, list) and value:
        pub = value[0].get("pubkey") if isinstance(value[0], dict) else None
        if pub:
            return str(pub)

    raise RuntimeError("could_not_resolve_input_account")


# -----------------------------------------------------------------------------
# Trade API endpoints
# -----------------------------------------------------------------------------
def _compute_path() -> str:
    return "swap-base-in" if RAY_SWAP_MODE == "base_in" else "swap-base-out"


def _tx_path() -> str:
    return "swap-base-in" if RAY_SWAP_MODE == "base_in" else "swap-base-out"


async def _compute_swap(
    *,
    host: str,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int,
    tx_version: str,
) -> dict[str, Any]:
    url = f"{host}/compute/{_compute_path()}"
    params = {
        "inputMint": str(input_mint),
        "outputMint": str(output_mint),
        "amount": int(amount),
        "slippageBps": int(slippage_bps),
        "txVersion": str(tx_version),
    }
    return await _http_json("GET", url, params=params)


async def _build_swap_txs(
    *,
    host: str,
    compute_response: dict[str, Any],
    wallet: str,
    tx_version: str,
    compute_unit_price_micro: str,
    wrap_sol: bool,
    unwrap_sol: bool,
    input_account: str | None,
    output_account: str | None,
) -> tuple[dict[str, Any], list[str]]:
    url = f"{host}/transaction/{_tx_path()}"
    swap_response = _extract_swap_response(compute_response)

    body: dict[str, Any] = {
        "computeUnitPriceMicroLamports": str(compute_unit_price_micro),
        "swapResponse": swap_response,
        "txVersion": str(tx_version),
        "wallet": str(wallet),
        "wrapSol": bool(wrap_sol),
        "unwrapSol": bool(unwrap_sol),
        "inputAccount": None if input_account is None else str(input_account),
        "outputAccount": None if output_account is None else str(output_account),
    }
    js = await _http_json("POST", url, json_body=body)
    txs = _extract_txs(js)
    return js, txs


# -----------------------------------------------------------------------------
# Send helpers
# -----------------------------------------------------------------------------
async def _send_tx_b64(tx_b64: str) -> str:
    if _send_b64_with_diag is not None:
        fn = _send_b64_with_diag
        for kwargs in (
            {"skip_preflight": SEND_SKIP_PREFLIGHT},
            {"skip_preflight": bool(SEND_SKIP_PREFLIGHT)},
            {"skipPreflight": bool(SEND_SKIP_PREFLIGHT)},
            {},
        ):
            try:
                return str(await _maybe_await(fn(tx_b64, **kwargs)))  # type: ignore[misc]
            except TypeError:
                continue
            except Exception:
                break

    kp, _ = await _get_keypair_cached()
    return str(
        await _maybe_await(sign_and_send_b64_tx(tx_b64, kp, skip_preflight=SEND_SKIP_PREFLIGHT))
    )


# -----------------------------------------------------------------------------
# Receipts / meta
# -----------------------------------------------------------------------------
def _mk_receipt(
    *,
    ok: bool,
    side: str,
    input_mint: str,
    output_mint: str,
    amount_in: int,
    slippage_bps: int,
    tx_version: str,
    swap_host: str,
    tx_count: int = 0,
    sigs: list[str] | None = None,
    err: str | None = None,
    meta: dict[str, Any] | None = None,
    compute: dict[str, Any] | None = None,
    build: dict[str, Any] | None = None,
) -> dict[str, Any]:
    r: dict[str, Any] = {
        "ok": bool(ok),
        "side": str(side).upper(),
        "inputMint": str(input_mint),
        "outputMint": str(output_mint),
        "amount_in": int(amount_in),
        "slippage_bps": int(slippage_bps),
        "txVersion": str(tx_version),
        "swap_host": str(swap_host),
        "tx_count": int(tx_count),
        "sigs": sigs or [],
        "sig": (sigs[-1] if sigs else None),
        "err": err,
        "ts": float(time.time()),
    }
    if meta:
        r["meta"] = meta
    if RAY_INCLUDE_SWAP_RESPONSE:
        if compute is not None:
            r["compute"] = compute
        if build is not None:
            r["build"] = build
    return r


def _variant_flags() -> dict[str, Any]:
    return {
        "base_host": RAY_BASE_HOST,
        "swap_hosts": RAY_SWAP_HOSTS,
        "swap_mode": RAY_SWAP_MODE,
        "tx_version": RAY_TX_VERSION,
        "default_slippage_bps": DEFAULT_SLIPPAGE_BPS,
        "priority_tier": RAY_PRIORITY_TIER,
        "cu_price_micro_env_set": bool(RAY_CU_PRICE_MICRO),
        "timeout_sec": HTTP_TIMEOUT_SEC,
        "retry_max": HTTP_RETRY_MAX,
        "qps": RAY_MAX_QPS,
        "http_concurrency": _RAY_HTTP_CONCURRENCY,
        "dryrun": DRY_RUN,
        "dryrun_build_tx": RAY_DRYRUN_BUILD_TX,
        "return_receipts_default": RAY_RETURN_RECEIPTS,
        "disable_buys": DISABLE_BUYS,
        "disable_sells": DISABLE_SELLS,
        "kill_switch": KILL_SWITCH,
        "skip_preflight": SEND_SKIP_PREFLIGHT,
        "rpc_url_set": bool(SOLANA_RPC_URL),
        "ata_verify_exists": RAY_ATA_VERIFY_EXISTS,
    }


# -----------------------------------------------------------------------------
# Router-friendly quote helpers
# -----------------------------------------------------------------------------
async def get_buy_quote(
    *, mint: str, lamports_in: int, slippage_bps: int = DEFAULT_SLIPPAGE_BPS
) -> dict[str, Any]:
    sbps = _clamp_i(int(slippage_bps), 1, 5000)
    m = normalize_mint(mint)

    if lamports_in <= 0:
        raise RuntimeError("lamports_in<=0")

    if not budget.before_quote(units=1):
        raise RuntimeError("quote blocked by budget")

    try:
        last_err: Exception | None = None
        for host in RAY_SWAP_HOSTS:
            if await _host_is_banned(host):
                continue
            try:
                js = await _compute_swap(
                    host=host,
                    input_mint=SOL_MINT,
                    output_mint=m,
                    amount=int(lamports_in),
                    slippage_bps=sbps,
                    tx_version=RAY_TX_VERSION,
                )
                await _host_mark_ok(host)
                return js
            except Exception as e:
                last_err = e
                await _host_mark_fail(host, why=_short(e, 180))
                continue
        raise last_err or RuntimeError("ray_compute_failed")
    finally:
        with contextlib.suppress(Exception):
            budget.after_quote(units=1)


async def get_sell_quote(
    *, mint: str, amount_raw: int, slippage_bps: int = DEFAULT_SLIPPAGE_BPS
) -> dict[str, Any]:
    sbps = _clamp_i(int(slippage_bps), 1, 5000)
    m = normalize_mint(mint)

    if amount_raw <= 0:
        raise RuntimeError("amount_raw<=0")

    if not budget.before_quote(units=1):
        raise RuntimeError("quote blocked by budget")

    try:
        last_err: Exception | None = None
        for host in RAY_SWAP_HOSTS:
            if await _host_is_banned(host):
                continue
            try:
                js = await _compute_swap(
                    host=host,
                    input_mint=m,
                    output_mint=SOL_MINT,
                    amount=int(amount_raw),
                    slippage_bps=sbps,
                    tx_version=RAY_TX_VERSION,
                )
                await _host_mark_ok(host)
                return js
            except Exception as e:
                last_err = e
                await _host_mark_fail(host, why=_short(e, 180))
                continue
        raise last_err or RuntimeError("ray_compute_failed")
    finally:
        with contextlib.suppress(Exception):
            budget.after_quote(units=1)


# -----------------------------------------------------------------------------
# Public API (router-compatible)
# -----------------------------------------------------------------------------
async def buy_via_raydium(
    *,
    mint: str,
    amount_sol: float,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    return_meta: bool = False,
    return_receipt: bool = False,
    prequote: dict[str, Any] | None = None,
    quote: dict[str, Any] | None = None,
    prefetched_quote: dict[str, Any] | None = None,
    **_ignored: Any,
) -> str | None | tuple[str | None, dict[str, Any]] | dict[str, Any]:
    if KILL_SWITCH or DISABLE_BUYS:
        rec = _mk_receipt(
            ok=False,
            side="BUY",
            input_mint=SOL_MINT,
            output_mint=str(mint),
            amount_in=0,
            slippage_bps=int(slippage_bps),
            tx_version=RAY_TX_VERSION,
            swap_host=RAY_SWAP_HOSTS[0],
            err="buys_disabled",
        )
        if return_receipt or RAY_RETURN_RECEIPTS:
            return rec
        if return_meta:
            return None, {"blocked": True, "reason": "buys_disabled"}
        return None

    sbps = _clamp_i(int(slippage_bps), 1, 5000)
    amt_sol = float(amount_sol)
    m = normalize_mint(mint)

    if not math.isfinite(amt_sol) or amt_sol <= 0.0:
        rec = _mk_receipt(
            ok=False,
            side="BUY",
            input_mint=SOL_MINT,
            output_mint=m,
            amount_in=0,
            slippage_bps=sbps,
            tx_version=RAY_TX_VERSION,
            swap_host=RAY_SWAP_HOSTS[0],
            err="amount_sol<=0",
        )
        if return_receipt or RAY_RETURN_RECEIPTS:
            return rec
        if return_meta:
            return None, {"impl": "raydium", "side": "BUY", "err": "amount_sol<=0"}
        return None

    lamports_in = max(1, int(amt_sol * float(LAMPORTS_PER_SOL)))

    supplied = prequote or quote or prefetched_quote
    compute_resp: dict[str, Any] | None = (
        supplied if isinstance(supplied, dict) and supplied else None
    )

    meta: dict[str, Any] = {"impl": "raydium", "side": "BUY"}

    try:
        _kp, owner = await _get_keypair_cached()

        if DRY_RUN and not RAY_DRYRUN_BUILD_TX:
            _every(
                "RAY_ENTRY_DRYRUN",
                n=15,
                msg="RAY_ENTRY_DRYRUN",
                mint=m,
                lamports_in=lamports_in,
                sbps=sbps,
            )
            rec = _mk_receipt(
                ok=True,
                side="BUY",
                input_mint=SOL_MINT,
                output_mint=m,
                amount_in=lamports_in,
                slippage_bps=sbps,
                tx_version=RAY_TX_VERSION,
                swap_host=RAY_SWAP_HOSTS[0],
                meta={**meta, "dryrun": True, "built": False},
                compute=compute_resp,
            )
            if return_receipt or RAY_RETURN_RECEIPTS:
                return rec
            if return_meta:
                return "DRYRUN", {**meta, "dryrun": True, "built": False}
            return None

        micro = await _fetch_priority_fee_micro()
        if not micro:
            micro = "0"
        meta["cu_price_micro"] = micro

        last_err: Exception | None = None

        for host in RAY_SWAP_HOSTS:
            if await _host_is_banned(host):
                continue
            try:
                t0 = _mono()

                if compute_resp is None:
                    compute_resp = await _compute_swap(
                        host=host,
                        input_mint=SOL_MINT,
                        output_mint=m,
                        amount=lamports_in,
                        slippage_bps=sbps,
                        tx_version=RAY_TX_VERSION,
                    )

                build_js, txs = await _build_swap_txs(
                    host=host,
                    compute_response=compute_resp,
                    wallet=owner,
                    tx_version=RAY_TX_VERSION,
                    compute_unit_price_micro=micro,
                    wrap_sol=True,
                    unwrap_sol=False,
                    input_account=None,
                    output_account=None,
                )

                if DRY_RUN:
                    _every(
                        "RAY_ENTRY_DRYRUN_BUILT",
                        n=15,
                        msg="RAY_ENTRY_DRYRUN_BUILT",
                        mint=m,
                        tx_count=len(txs),
                        host=host,
                    )
                    rec = _mk_receipt(
                        ok=True,
                        side="BUY",
                        input_mint=SOL_MINT,
                        output_mint=m,
                        amount_in=lamports_in,
                        slippage_bps=sbps,
                        tx_version=RAY_TX_VERSION,
                        swap_host=host,
                        tx_count=len(txs),
                        meta={**meta, "dryrun": True, "built": True},
                        compute=compute_resp,
                        build=build_js,
                    )
                    if return_receipt or RAY_RETURN_RECEIPTS:
                        return rec
                    if return_meta:
                        return "DRYRUN", {
                            **meta,
                            "dryrun": True,
                            "built": True,
                            "tx_count": len(txs),
                            "host": host,
                        }
                    return None

                sigs: list[str] = []
                for tx_b64 in txs:
                    sigs.append(await _send_tx_b64(tx_b64))

                dt_ms = int(1000.0 * (_mono() - t0))
                await _host_mark_ok(host)

                _log_event(
                    "RAY_ENTRY_OK",
                    mint=m,
                    sig=sigs[-1],
                    tx_count=len(sigs),
                    host=host,
                    lamports_in=lamports_in,
                    ms=dt_ms,
                )

                if return_receipt or RAY_RETURN_RECEIPTS:
                    return _mk_receipt(
                        ok=True,
                        side="BUY",
                        input_mint=SOL_MINT,
                        output_mint=m,
                        amount_in=lamports_in,
                        slippage_bps=sbps,
                        tx_version=RAY_TX_VERSION,
                        swap_host=host,
                        tx_count=len(sigs),
                        sigs=sigs,
                        meta={**meta, "host": host, "ms": dt_ms},
                        compute=compute_resp,
                        build=build_js,
                    )

                if return_meta:
                    return sigs[-1], {**meta, "host": host, "tx_count": len(sigs), "ms": dt_ms}

                return sigs[-1]

            except Exception as e:
                last_err = e
                await _host_mark_fail(host, why=_short(e, 180))
                compute_resp = None
                continue

        raise last_err or RuntimeError("ray_buy_failed_all_hosts")

    except Exception as e:
        _warn("RAY_ENTRY_FAIL", err=_short(e, 260), mint=m)
        rec = _mk_receipt(
            ok=False,
            side="BUY",
            input_mint=SOL_MINT,
            output_mint=m,
            amount_in=lamports_in,
            slippage_bps=sbps,
            tx_version=RAY_TX_VERSION,
            swap_host=RAY_SWAP_HOSTS[0],
            err=str(e),
            meta=meta,
            compute=compute_resp,
        )
        if return_receipt or RAY_RETURN_RECEIPTS:
            return rec
        if return_meta:
            return None, {**meta, "err": str(e)}
        return None


async def sell_via_raydium(
    *,
    mint: str,
    token_program: str | None = None,
    amount_raw: int,
    slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    return_receipt: bool = False,
    prequote: dict[str, Any] | None = None,
    quote: dict[str, Any] | None = None,
    prefetched_quote: dict[str, Any] | None = None,
    **_ignored: Any,
) -> str | None | dict[str, Any]:
    del token_program

    if KILL_SWITCH or DISABLE_SELLS:
        rec = _mk_receipt(
            ok=False,
            side="SELL",
            input_mint=str(mint),
            output_mint=SOL_MINT,
            amount_in=int(amount_raw),
            slippage_bps=int(slippage_bps),
            tx_version=RAY_TX_VERSION,
            swap_host=RAY_SWAP_HOSTS[0],
            err="sells_disabled",
        )
        return rec if (return_receipt or RAY_RETURN_RECEIPTS) else None

    sbps = _clamp_i(int(slippage_bps), 1, 5000)
    amt = int(amount_raw)
    m = normalize_mint(mint)

    if amt <= 0:
        rec = _mk_receipt(
            ok=False,
            side="SELL",
            input_mint=m,
            output_mint=SOL_MINT,
            amount_in=amt,
            slippage_bps=sbps,
            tx_version=RAY_TX_VERSION,
            swap_host=RAY_SWAP_HOSTS[0],
            err="amount_raw<=0",
        )
        return rec if (return_receipt or RAY_RETURN_RECEIPTS) else None

    supplied = prequote or quote or prefetched_quote
    compute_resp: dict[str, Any] | None = (
        supplied if isinstance(supplied, dict) and supplied else None
    )

    meta: dict[str, Any] = {"impl": "raydium", "side": "SELL"}

    try:
        _kp, owner = await _get_keypair_cached()

        if DRY_RUN and not RAY_DRYRUN_BUILD_TX:
            _every(
                "RAY_EXIT_DRYRUN", n=15, msg="RAY_EXIT_DRYRUN", mint=m, amount_raw=amt, sbps=sbps
            )
            rec = _mk_receipt(
                ok=True,
                side="SELL",
                input_mint=m,
                output_mint=SOL_MINT,
                amount_in=amt,
                slippage_bps=sbps,
                tx_version=RAY_TX_VERSION,
                swap_host=RAY_SWAP_HOSTS[0],
                meta={**meta, "dryrun": True, "built": False},
                compute=compute_resp,
            )
            return rec if (return_receipt or RAY_RETURN_RECEIPTS) else None

        micro = await _fetch_priority_fee_micro()
        if not micro:
            micro = "0"
        meta["cu_price_micro"] = micro

        input_acc = await _resolve_input_token_account(owner=owner, mint=m)
        meta["inputAccount"] = input_acc

        last_err: Exception | None = None

        for host in RAY_SWAP_HOSTS:
            if await _host_is_banned(host):
                continue
            try:
                t0 = _mono()

                if compute_resp is None:
                    compute_resp = await _compute_swap(
                        host=host,
                        input_mint=m,
                        output_mint=SOL_MINT,
                        amount=amt,
                        slippage_bps=sbps,
                        tx_version=RAY_TX_VERSION,
                    )

                build_js, txs = await _build_swap_txs(
                    host=host,
                    compute_response=compute_resp,
                    wallet=owner,
                    tx_version=RAY_TX_VERSION,
                    compute_unit_price_micro=micro,
                    wrap_sol=False,
                    unwrap_sol=True,
                    input_account=input_acc,
                    output_account=None,
                )

                if DRY_RUN:
                    _every(
                        "RAY_EXIT_DRYRUN_BUILT",
                        n=15,
                        msg="RAY_EXIT_DRYRUN_BUILT",
                        mint=m,
                        tx_count=len(txs),
                        host=host,
                    )
                    rec = _mk_receipt(
                        ok=True,
                        side="SELL",
                        input_mint=m,
                        output_mint=SOL_MINT,
                        amount_in=amt,
                        slippage_bps=sbps,
                        tx_version=RAY_TX_VERSION,
                        swap_host=host,
                        tx_count=len(txs),
                        meta={**meta, "dryrun": True, "built": True, "host": host},
                        compute=compute_resp,
                        build=build_js,
                    )
                    return rec if (return_receipt or RAY_RETURN_RECEIPTS) else None

                sigs: list[str] = []
                for tx_b64 in txs:
                    sigs.append(await _send_tx_b64(tx_b64))

                dt_ms = int(1000.0 * (_mono() - t0))
                await _host_mark_ok(host)

                _log_event(
                    "RAY_EXIT_OK",
                    mint=m,
                    sig=sigs[-1],
                    tx_count=len(sigs),
                    host=host,
                    amount_raw=amt,
                    ms=dt_ms,
                )

                if return_receipt or RAY_RETURN_RECEIPTS:
                    return _mk_receipt(
                        ok=True,
                        side="SELL",
                        input_mint=m,
                        output_mint=SOL_MINT,
                        amount_in=amt,
                        slippage_bps=sbps,
                        tx_version=RAY_TX_VERSION,
                        swap_host=host,
                        tx_count=len(sigs),
                        sigs=sigs,
                        meta={**meta, "host": host, "ms": dt_ms},
                        compute=compute_resp,
                        build=build_js,
                    )

                return sigs[-1]

            except Exception as e:
                last_err = e
                await _host_mark_fail(host, why=_short(e, 180))
                compute_resp = None
                continue

        raise last_err or RuntimeError("ray_sell_failed_all_hosts")

    except Exception as e:
        _warn("RAY_EXIT_FAIL", err=_short(e, 260), mint=m)
        rec = _mk_receipt(
            ok=False,
            side="SELL",
            input_mint=m,
            output_mint=SOL_MINT,
            amount_in=amt,
            slippage_bps=sbps,
            tx_version=RAY_TX_VERSION,
            swap_host=RAY_SWAP_HOSTS[0],
            err=str(e),
            meta=meta,
            compute=compute_resp,
        )
        return rec if (return_receipt or RAY_RETURN_RECEIPTS) else None


# -----------------------------------------------------------------------------
# Debug helpers (Render shell smoke tests)
# -----------------------------------------------------------------------------
async def debug_compute_sol_to_usdc(
    amount_sol: float = 0.002, slippage_bps: int = DEFAULT_SLIPPAGE_BPS
) -> dict[str, Any]:
    lamports = max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))
    return await get_buy_quote(
        mint=USDC_MINT, lamports_in=lamports, slippage_bps=_clamp_i(int(slippage_bps), 1, 5000)
    )


async def debug_build_only_sol_to_usdc(
    amount_sol: float = 0.002, slippage_bps: int = DEFAULT_SLIPPAGE_BPS
) -> tuple[int, int, str]:
    lamports = max(1, int(float(amount_sol) * float(LAMPORTS_PER_SOL)))
    _kp, owner = await _get_keypair_cached()
    micro = await _fetch_priority_fee_micro()
    if not micro:
        micro = "0"

    for host in RAY_SWAP_HOSTS:
        if await _host_is_banned(host):
            continue
        compute = await _compute_swap(
            host=host,
            input_mint=SOL_MINT,
            output_mint=USDC_MINT,
            amount=lamports,
            slippage_bps=_clamp_i(int(slippage_bps), 1, 5000),
            tx_version=RAY_TX_VERSION,
        )
        _build_js, txs = await _build_swap_txs(
            host=host,
            compute_response=compute,
            wallet=owner,
            tx_version=RAY_TX_VERSION,
            compute_unit_price_micro=micro,
            wrap_sol=True,
            unwrap_sol=False,
            input_account=None,
            output_account=None,
        )
        await _host_mark_ok(host)
        return lamports, len(txs), host

    raise RuntimeError("no_hosts")


__all__ = [
    "DEFAULT_SLIPPAGE_BPS",
    "RAY_BASE_HOST",
    "RAY_SWAP_HOSTS",
    "RAY_SWAP_MODE",
    "RAY_TX_VERSION",
    "_variant_flags",
    "buy_via_raydium",
    "close_http",
    "debug_build_only_sol_to_usdc",
    "debug_compute_sol_to_usdc",
    "get_buy_quote",
    "get_sell_quote",
    "sell_via_raydium",
]
