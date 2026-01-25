from __future__ import annotations

"""
scripts/check_router.py — God-Tier Router Precheck (Repo-clean FINAL v3.1)
=========================================================================

Repo-clean contract (LOCKED)
----------------------------
- Exit codes: 0=OK, 1=FAIL, 130=CTRL_C
- When --json-only (or CHECK_ROUTER_JSON_ONLY=1): emit ONLY JSON to stdout.
- Never return rc=2/3/etc (those break repo-clean preflight).

Key upgrades vs v3
------------------
- Preflight-safe: auto total timeout under PREFLIGHT=1 if unset/0.
- Deadline-aware: every call clamps to time remaining.
- Variant explosion control: minimal set by default; use --debug or CHECK_ROUTER_EXHAUSTIVE=1.
- Strict-auth relaxation (precheck only): may set ROUTER_STRICT_AUTH=0 when no valid JUP key.
- Quote-only: no swaps/tx signing/sending.

Everything else kept:
- repo bootstrap for stable imports
- safe redaction, JSON summary, trace events
- signature-drift tolerant calling
"""

import argparse
import asyncio
import contextlib
import dataclasses
import inspect
import json
import os
import re
import sys
import time
import traceback
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

RC_OK = 0
RC_FATAL = 1
RC_CTRL_C = 130

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


# =============================================================================
# repo-root bootstrap (TOP OF FILE)
# =============================================================================
def _bootstrap_repo_root() -> str:
    """Insert repo root (and repo_root/src if present) into sys.path for stable imports."""
    try:
        here = Path(__file__).resolve()
    except Exception:
        return ""

    candidates = [here.parent, *list(here.parents)]
    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    root: Path | None = None

    for p in candidates:
        try:
            if any((p / m).exists() for m in markers):
                root = p
                break
            if (p / "src").is_dir() and any(
                (p / "src" / x).exists() for x in ("utils_pkg", "scanner", "scripts", "sniper")
            ):
                root = p
                break
        except Exception:
            continue

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    root_str = str(root)
    if root_str and root_str not in sys.path:
        sys.path.insert(0, root_str)

    src = root / "src"
    if src.is_dir():
        src_str = str(src)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)

    os.environ.setdefault("REPO_ROOT", root_str)
    return root_str


_REPO_ROOT = _bootstrap_repo_root()


def _load_dotenv_best_effort(repo_root: str) -> None:
    """
    Best-effort .env loader (NO dependency on python-dotenv).
    - Loads repo_root/.env if present
    - Never overwrites already-set environment variables
    - Skips on Render by default
    """
    try:
        if os.getenv("RENDER", "").strip() in _TRUE:
            return
        if os.getenv("CHECK_ROUTER_LOAD_DOTENV", "1").strip().lower() in _FALSE:
            return

        p = Path(repo_root) / ".env"
        if not p.exists() or not p.is_file():
            return

        text = p.read_text(encoding="utf-8", errors="ignore")
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("export "):
                line = line[7:].lstrip()
            if "=" not in line:
                continue

            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()

            if not k or k in os.environ:
                continue

            # strip surrounding quotes
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                v = v[1:-1]

            # remove trailing inline comment only if it's clearly separated
            if " #" in v:
                v = v.split(" #", 1)[0].rstrip()

            os.environ[k] = v
    except Exception:
        # best effort only — never crash precheck because of dotenv parsing
        return


# =============================================================================
# env helpers
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    v = _envs(k, "")
    if v == "":
        return bool(d)
    s = v.lower().strip()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envi(k: str, d: int) -> int:
    try:
        return int(float(_envs(k, str(d)).replace("_", "")))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now_ms() -> int:
    return int(time.time() * 1000)


# =============================================================================
# IO safety
# =============================================================================
def _safe_print(s: str, *, file) -> None:
    try:
        file.write(s + ("" if s.endswith("\n") else "\n"))
        file.flush()
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = s.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            file.buffer.write(b + b"\n")  # type: ignore[attr-defined]
            file.buffer.flush()  # type: ignore[attr-defined]
    except Exception:
        pass


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_print(f"[check_router] {_ts()} {msg}", file=sys.stderr)


# =============================================================================
# Redaction / safety
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")
_QUERY_KEY_RE = re.compile(
    r"(?i)([?&](?:api[_-]?key|apikey|token|key|secret|sig|signature|auth|authorization|access[_-]?token)=)([^&\s]+)"
)

_SENSITIVE_ENV_KEYS = [
    "JUP_PRO_KEY",
    "JUP_API_KEY",
    "OPENAI_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "DISCORD_TOKEN",
    "NOTION_TOKEN",
    "BIRDEYE_API_KEY",
    "HELIUS_API_KEY",
    "WALLET_JSON",
    "WALLET_JSON_B64",
    "WALLET_PRIVATE_KEY",
    "KEYPAIR_B64",
    "KEYPAIR_PATH",
    "DATABASE_URL",
    "POSTGRES_DSN",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "REDIS_URL",
    "RPC_URL",
    "SOL_RPC_URL",
    "SOL_RPC_URLS",
]


def _mask_long_tokens_enabled() -> bool:
    return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)


def _sanitize_text(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_HDR_RE.sub(r"\1***", s)
    s = _QUERY_KEY_RE.sub(r"\1***", s)

    for k in _SENSITIVE_ENV_KEYS:
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _mask_long_tokens_enabled():
        s = _LONG_TOKENISH_RE.sub("***", s)
    return s


def _short_exc(e: BaseException, n: int = 360) -> str:
    s = _sanitize_text(f"{type(e).__name__}: {e}")
    return (s[:n] + "…") if len(s) > n else s


def _safe_jsonable(x: Any) -> Any:
    try:
        json.dumps(x, default=str)
        return x
    except Exception:
        return str(x)


# =============================================================================
# Strict-auth relaxation helpers
# =============================================================================
def _looks_like_placeholder_key(v: str) -> bool:
    s = (v or "").strip()
    if not s:
        return True
    low = s.lower()
    bad = {
        "$jup_api_key",
        "$jup_pro_key",
        "your-api-key",
        "your_key",
        "changeme",
        "replace_me",
        "none",
        "null",
    }
    if low in bad:
        return True
    if "$" in s and "key" in low:
        return True
    return bool(" " in s and "key" in low)


def _maybe_relax_router_auth_for_precheck(*, json_only: bool) -> None:
    """If no key exists, relax strict auth for quote-only precheck."""
    k = os.getenv("JUP_API_KEY") or os.getenv("JUP_PRO_KEY") or ""
    if _looks_like_placeholder_key(k) and os.getenv("ROUTER_STRICT_AUTH") is None:
        os.environ["ROUTER_STRICT_AUTH"] = "0"
        _hlog(
            "Auto-set ROUTER_STRICT_AUTH=0 (no valid JUP key detected) for precheck",
            json_only=json_only,
        )


def _maybe_relax_on_auth_error(err_text: str, *, json_only: bool) -> bool:
    """
    If we detect 401/403-ish, retry once with ROUTER_STRICT_AUTH=0.
    Returns True if we changed env and should retry.
    """
    t = (err_text or "").lower()
    if ("401" in t) or ("403" in t) or ("unauthorized" in t) or ("forbidden" in t):
        if os.getenv("ROUTER_STRICT_AUTH", "").strip() != "0":
            os.environ["ROUTER_STRICT_AUTH"] = "0"
            _hlog(
                "Detected auth error; set ROUTER_STRICT_AUTH=0 and retry once (precheck only)",
                json_only=json_only,
            )
            return True
    return False


# =============================================================================
# Deadline controller
# =============================================================================
@dataclass
class Deadline:
    deadline_ms: int | None

    @classmethod
    def from_total(cls, total_timeout_s: float) -> "Deadline":
        if total_timeout_s and total_timeout_s > 0:
            return cls(_now_ms() + int(float(total_timeout_s) * 1000))
        return cls(None)

    def left_s(self) -> float | None:
        if self.deadline_ms is None:
            return None
        return max(0.0, float(self.deadline_ms - _now_ms()) / 1000.0)

    def clamp(self, desired_s: float, *, min_s: float = 0.75) -> float:
        if desired_s <= 0:
            return 0.0
        left = self.left_s()
        if left is None:
            return float(desired_s)
        if left <= 0:
            return 0.0
        return max(min_s, min(float(desired_s), float(left)))

    def expired(self) -> bool:
        left = self.left_s()
        return (left is not None) and (left <= 0.0)


# =============================================================================
# small utils
# =============================================================================
def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or isinstance(x, bool):
            return int(default)
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            return int(x)
        s = str(x).strip()
        return int(float(s)) if s else int(default)
    except Exception:
        return int(default)


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or isinstance(x, bool):
            return float(default)
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        return float(s) if s else float(default)
    except Exception:
        return float(default)


def _raw_to_ui(raw: int, decimals: int | None) -> float | None:
    try:
        if raw <= 0:
            return None
        d = int(decimals or 0)
        if d <= 0 or d > 18:
            return None
        return float(raw) / float(10**d)
    except Exception:
        return None


def _sol_to_lamports(amount_sol: float, sol_decimals: int = 9) -> int:
    d = int(sol_decimals if sol_decimals > 0 else 9)
    return int(round(float(amount_sol) * float(10**d)))


def _pick(d: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return default


def _maybe_base58_like(s: str) -> bool:
    ss = (s or "").strip()
    if len(ss) < 28:
        return False
    return "://" not in ss


def _is_exhaustive(debug: bool) -> bool:
    return bool(debug or _envb("CHECK_ROUTER_EXHAUSTIVE", False))


# =============================================================================
# trace events (for JSON-only debugging)
# =============================================================================
@dataclass
class TraceEvent:
    scope: str
    fn: str
    used_name: str
    args_len: int
    kwargs_keys: list[str]
    ok: int
    ms: int
    err: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "fn": self.fn,
            "used": self.used_name,
            "args_len": self.args_len,
            "kwargs_keys": self.kwargs_keys,
            "ok": self.ok,
            "ms": self.ms,
            "err": self.err,
        }


# =============================================================================
# Signature-drift tolerant calling (SYNC safe)
# =============================================================================
def _signature_params(fn: Any) -> list[str]:
    try:
        sig = inspect.signature(fn)
        return [p.name for p in sig.parameters.values()]
    except Exception:
        return []


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


def _build_kwargs_from_signature(
    fn: Any,
    *,
    input_mint: str,
    output_mint: str,
    amount_lamports: int,
    amount_sol: float,
    slippage_bps: int,
    timeout_s: float,
) -> dict[str, Any]:
    params = _signature_params(fn)
    out: dict[str, Any] = {}

    for k in params:
        lk = k.lower()
        if lk in {"inputmint", "input_mint", "in_mint", "mint_in", "from_mint", "src_mint"}:
            out[k] = input_mint
        elif lk in {"outputmint", "output_mint", "out_mint", "mint_out", "to_mint", "dst_mint"}:
            out[k] = output_mint
        elif lk in {"mint", "token_mint"}:
            out[k] = output_mint

    for k in params:
        lk = k.lower()
        if lk in {
            "amount",
            "inamount",
            "in_amount",
            "amount_in",
            "amountin",
            "amount_raw",
            "amount_in_raw",
        }:
            out[k] = int(amount_lamports)
        elif lk in {"lamports", "lamports_in", "lamportsin"}:
            out[k] = int(amount_lamports)
        elif lk in {"amount_sol", "amountsol", "ui_amount", "amount_ui"}:
            out[k] = float(amount_sol)

    for k in params:
        lk = k.lower()
        if lk in {"slippagebps", "slippage_bps", "slippage", "max_slippage_bps"}:
            out[k] = int(slippage_bps)

    for k in params:
        lk = k.lower()
        if lk in {"timeout", "timeout_s", "timeoutsec", "timeout_seconds"}:
            out[k] = float(timeout_s)

    return out


async def _invoke(fn: Any, *args: Any, **kwargs: Any) -> Any:
    if inspect.iscoroutinefunction(fn):
        return await fn(*args, **kwargs)
    res = await asyncio.to_thread(fn, *args, **kwargs)
    if inspect.isawaitable(res):
        return await res
    return res


@dataclass
class CallHit:
    ok: bool
    used: str
    err: str
    val: Any


async def _try_call(
    scope: str,
    target: Any,
    fn_names: tuple[str, ...],
    variants: Sequence[tuple[tuple[Any, ...], dict[str, Any]]],
    timeout_s: float,
    *,
    deadline: Deadline,
    debug: bool,
    json_only: bool,
    trace: list[TraceEvent] | None,
    sig_fill: dict[str, Any] | None = None,
) -> CallHit:
    last_err = "no_callable_found"

    for nm in fn_names:
        fn = getattr(target, nm, None)
        if not callable(fn):
            continue

        for args, kwargs in variants:
            if deadline.expired():
                return CallHit(False, "", "deadline_exceeded", None)

            t0 = _now_ms()
            try:
                kw = _filter_kwargs(fn, dict(kwargs))
                if debug:
                    _hlog(
                        f"CALL {scope}.{nm} args={len(args)} kwargs={sorted(list(kw.keys()))}",
                        json_only=json_only,
                    )
                t_call = deadline.clamp(float(timeout_s))
                if t_call <= 0:
                    return CallHit(False, "", "deadline_exceeded", None)

                val = await asyncio.wait_for(
                    _invoke(fn, *args, **kw), timeout=max(0.5, float(t_call))
                )
                if trace is not None:
                    trace.append(
                        TraceEvent(
                            scope, nm, nm, len(args), sorted(list(kw.keys())), 1, _now_ms() - t0, ""
                        )
                    )
                return CallHit(True, nm, "", val)
            except TypeError as e:
                last_err = _sanitize_text(f"{nm}: {e}")
                if trace is not None:
                    trace.append(
                        TraceEvent(
                            scope,
                            nm,
                            nm,
                            len(args),
                            sorted(list(kwargs.keys())),
                            0,
                            _now_ms() - t0,
                            _sanitize_text(e),
                        )
                    )
                continue
            except TimeoutError:
                last_err = f"{nm}: timeout"
                if trace is not None:
                    trace.append(
                        TraceEvent(
                            scope,
                            nm,
                            nm,
                            len(args),
                            sorted(list(kwargs.keys())),
                            0,
                            _now_ms() - t0,
                            "timeout",
                        )
                    )
                continue
            except Exception as e:
                last_err = _sanitize_text(f"{nm}: {_short_exc(e)}")
                if trace is not None:
                    trace.append(
                        TraceEvent(
                            scope,
                            nm,
                            nm,
                            len(args),
                            sorted(list(kwargs.keys())),
                            0,
                            _now_ms() - t0,
                            _short_exc(e),
                        )
                    )
                continue

        if sig_fill:
            if deadline.expired():
                return CallHit(False, "", "deadline_exceeded", None)

            t1 = _now_ms()
            try:
                kw2 = _filter_kwargs(fn, dict(sig_fill))
                if kw2:
                    t_call2 = deadline.clamp(float(timeout_s))
                    if t_call2 <= 0:
                        return CallHit(False, "", "deadline_exceeded", None)
                    val2 = await asyncio.wait_for(
                        _invoke(fn, **kw2), timeout=max(0.5, float(t_call2))
                    )
                    if trace is not None:
                        trace.append(
                            TraceEvent(
                                scope, nm, nm, 0, sorted(list(kw2.keys())), 1, _now_ms() - t1, ""
                            )
                        )
                    return CallHit(True, nm, "", val2)
            except Exception as e:
                last_err = _sanitize_text(f"{nm}: {_short_exc(e)}")
                if trace is not None:
                    trace.append(
                        TraceEvent(
                            scope,
                            nm,
                            nm,
                            0,
                            sorted(list(sig_fill.keys())),
                            0,
                            _now_ms() - t1,
                            _short_exc(e),
                        )
                    )

    return CallHit(False, "", last_err, None)


# =============================================================================
# Quote normalization / parsing
# =============================================================================
def _obj_to_dict(obj: Any) -> dict[str, Any] | None:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return dict(obj)
    if dataclasses.is_dataclass(obj):
        with contextlib.suppress(Exception):
            return dataclasses.asdict(obj)  # type: ignore[arg-type]
    if hasattr(obj, "model_dump") and callable(obj.model_dump):
        with contextlib.suppress(Exception):
            out = obj.model_dump()
            if isinstance(out, dict):
                return dict(out)
    if hasattr(obj, "dict") and callable(obj.dict):
        with contextlib.suppress(Exception):
            out = obj.dict()
            if isinstance(out, dict):
                return dict(out)
    with contextlib.suppress(Exception):
        d = vars(obj)
        if isinstance(d, dict) and d:
            return dict(d)
    return None


def _find_dict_quote(obj: Any) -> dict[str, Any] | None:
    if obj is None:
        return None

    if isinstance(obj, dict):
        data = obj.get("data")
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return dict(data[0])
        return dict(obj)

    if isinstance(obj, list) and obj:
        if isinstance(obj[0], dict):
            return dict(obj[0])
        return _find_dict_quote(obj[0])

    if isinstance(obj, tuple) and obj:
        for item in obj:
            q = _find_dict_quote(item)
            if q:
                return q

    d = _obj_to_dict(obj)
    if isinstance(d, dict) and d:
        return _find_dict_quote(d)

    out_amt = (
        getattr(obj, "outAmount", None)
        or getattr(obj, "out_amount", None)
        or getattr(obj, "out_amount_raw", None)
    )
    if out_amt is not None:
        return {"outAmount": out_amt}

    return None


def _parse_out_amount_raw(q: dict[str, Any]) -> int:
    return _to_int(
        _pick(
            q,
            "out_amount_raw",
            "outAmountRaw",
            "outAmount",
            "out_amount",
            "outputAmount",
            default=0,
        ),
        default=0,
    )


def _parse_in_amount_raw(q: dict[str, Any]) -> int:
    return _to_int(
        _pick(q, "in_amount_raw", "inAmountRaw", "inAmount", "in_amount", "inputAmount", default=0),
        default=0,
    )


def _quote_summary(q: dict[str, Any]) -> dict[str, Any]:
    keys = sorted(list(q.keys()))
    routeish = any(k in q for k in ("routePlan", "route_plan", "routes", "data"))
    return {
        "keys": keys[:90],
        "has_route": int(bool(routeish)),
        "in_amount_raw": (_parse_in_amount_raw(q) or None),
        "out_amount_raw": (_parse_out_amount_raw(q) or None),
        "priceImpactPct": q.get("priceImpactPct"),
    }


# =============================================================================
# Mint resolution
# =============================================================================
def _normalize_mint_best_effort(m: str) -> str:
    s = (m or "").strip()
    if not s:
        return s
    with contextlib.suppress(Exception):
        from utils_pkg.mints import try_normalize_mint  # type: ignore

        out = try_normalize_mint(s)
        if isinstance(out, str) and out.strip():
            return out.strip()
    with contextlib.suppress(Exception):
        from utils_pkg.mints import normalize_mint  # type: ignore

        out2 = normalize_mint(s)
        if isinstance(out2, str) and out2.strip():
            return out2.strip()
    return s


def _canonical_mints() -> tuple[str, str]:
    sol = "So11111111111111111111111111111111111111112"
    usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    with contextlib.suppress(Exception):
        from utils_pkg.mints import SOL_MINT, USDC_MINT  # type: ignore

        sol = str(SOL_MINT)
        usdc = str(USDC_MINT)
    return _normalize_mint_best_effort(sol), _normalize_mint_best_effort(usdc)


# =============================================================================
# Router target resolution + lifecycle
# =============================================================================
async def _resolve_router_target(
    router_mod: Any,
    timeout_s: float,
    *,
    deadline: Deadline,
    debug: bool,
    json_only: bool,
    trace: list[TraceEvent] | None,
) -> tuple[Any, str]:
    hit = await _try_call(
        "module",
        router_mod,
        ("get_router", "router", "get", "get_instance"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
        deadline=deadline,
        debug=debug,
        json_only=json_only,
        trace=trace,
    )
    if hit.ok and hit.val is not None:
        return hit.val, f"{hit.used}()"

    RouterCls = getattr(router_mod, "Router", None)
    if callable(RouterCls):
        try:
            t_call = deadline.clamp(timeout_s)
            if t_call <= 0:
                return router_mod, "module"
            obj = await asyncio.wait_for(
                asyncio.to_thread(RouterCls), timeout=max(0.5, float(t_call))
            )
            return obj, "Router()"
        except Exception as e:
            if debug:
                _hlog(f"Router() init failed: {_short_exc(e)}", json_only=json_only)

    return router_mod, "module"


async def _bootstrap_router(
    target: Any,
    timeout_s: float,
    *,
    deadline: Deadline,
    debug: bool,
    json_only: bool,
    trace: list[TraceEvent] | None,
) -> str:
    hit = await _try_call(
        "target",
        target,
        ("start", "bootstrap", "init", "setup", "warmup"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
        deadline=deadline,
        debug=debug,
        json_only=json_only,
        trace=trace,
    )
    return hit.used if hit.ok else f"router_bootstrap_skipped({hit.err})"


async def _shutdown_router(
    target: Any,
    timeout_s: float,
    *,
    deadline: Deadline,
    debug: bool,
    json_only: bool,
    trace: list[TraceEvent] | None,
) -> str:
    hit = await _try_call(
        "target",
        target,
        ("shutdown", "close", "stop", "teardown"),
        variants=[((), {}), ((), {"timeout_s": timeout_s}), ((), {"timeout": timeout_s})],
        timeout_s=timeout_s,
        deadline=deadline,
        debug=debug,
        json_only=json_only,
        trace=trace,
    )
    return hit.used if hit.ok else f"router_shutdown_skipped({hit.err})"


# =============================================================================
# Decimals: target -> module -> solana_token fallback
# =============================================================================
async def _get_decimals_any(
    target: Any,
    router_mod: Any,
    mint: str,
    timeout_s: float,
    *,
    deadline: Deadline,
    debug: bool,
    json_only: bool,
    trace: list[TraceEvent] | None,
) -> tuple[int | None, str]:
    hit = await _try_call(
        "target",
        target,
        ("get_decimals", "mint_decimals", "decimals", "fetch_decimals"),
        variants=[((mint,), {}), ((), {"mint": mint})],
        timeout_s=timeout_s,
        deadline=deadline,
        debug=debug,
        json_only=json_only,
        trace=trace,
    )
    if hit.ok:
        d = _to_int(hit.val, default=0)
        if 0 < d <= 18:
            return d, f"target.{hit.used}"

    hit2 = await _try_call(
        "module",
        router_mod,
        ("get_decimals", "mint_decimals", "decimals", "fetch_decimals"),
        variants=[((mint,), {}), ((), {"mint": mint})],
        timeout_s=timeout_s,
        deadline=deadline,
        debug=debug,
        json_only=json_only,
        trace=trace,
    )
    if hit2.ok:
        d = _to_int(hit2.val, default=0)
        if 0 < d <= 18:
            return d, f"module.{hit2.used}"

    with contextlib.suppress(Exception):
        from utils_pkg import solana_token as st  # type: ignore

        for fn_name in ("get_token_decimals", "token_decimals", "get_decimals"):
            fn = getattr(st, fn_name, None)
            if callable(fn):
                try:
                    t_call = deadline.clamp(timeout_s)
                    if t_call <= 0:
                        break
                    try:
                        val = await asyncio.wait_for(
                            _invoke(fn, mint, default=None), timeout=max(0.5, float(t_call))
                        )
                    except TypeError:
                        val = await asyncio.wait_for(
                            _invoke(fn, mint), timeout=max(0.5, float(t_call))
                        )
                except Exception:
                    continue

                if val is not None:
                    d3 = int(_to_int(val, default=0))
                    if 0 < d3 <= 18:
                        return d3, f"solana_token.{fn_name}"

    err = hit.err if hit.err != "no_callable_found" else hit2.err
    return None, err


# =============================================================================
# Quote getters (drift tolerant)
# =============================================================================
async def _get_buy_quote_any(
    target: Any,
    router_mod: Any,
    *,
    out_mint: str,
    amount_sol: float,
    slippage_bps: int,
    sol_mint: str,
    sol_decimals: int,
    timeout_s: float,
    deadline: Deadline,
    debug: bool,
    json_only: bool,
    trace: list[TraceEvent] | None,
) -> tuple[dict[str, Any] | None, str, str]:
    lamports = _sol_to_lamports(amount_sol, sol_decimals=sol_decimals)
    sb = int(slippage_bps)

    exhaustive = _is_exhaustive(debug)

    variants_buy_min = [
        ((), {"mint": out_mint, "amount_sol": float(amount_sol), "slippage_bps": sb}),
        ((out_mint, float(amount_sol), sb), {}),
        ((out_mint,), {"amount_sol": float(amount_sol), "slippage_bps": sb}),
    ]
    variants_buy_extra = [
        ((), {"mint": out_mint, "amount_sol": float(amount_sol), "slippageBps": sb}),
        ((), {"mint": out_mint, "lamports_in": int(lamports), "slippage_bps": sb}),
        ((), {"mint": out_mint, "amount_in": int(lamports), "slippage_bps": sb}),
        ((out_mint, int(lamports), sb), {}),
        ((out_mint,), {"amount_in": int(lamports), "slippage_bps": sb}),
    ]
    variants_buy = variants_buy_min + (variants_buy_extra if exhaustive else [])

    variants_quote_min = [
        (
            (),
            {
                "inputMint": sol_mint,
                "outputMint": out_mint,
                "amount": int(lamports),
                "slippageBps": sb,
            },
        ),
        (
            (),
            {
                "input_mint": sol_mint,
                "output_mint": out_mint,
                "amount": int(lamports),
                "slippage_bps": sb,
            },
        ),
    ]
    variants_quote_extra = [
        (
            (),
            {
                "mint_in": sol_mint,
                "mint_out": out_mint,
                "amount_in": int(lamports),
                "slippage_bps": sb,
            },
        ),
        (
            (),
            {
                "from_mint": sol_mint,
                "to_mint": out_mint,
                "amount_in": int(lamports),
                "slippage_bps": sb,
            },
        ),
        (
            (),
            {
                "in_mint": sol_mint,
                "out_mint": out_mint,
                "amount_in": int(lamports),
                "slippage_bps": sb,
            },
        ),
        ((sol_mint, out_mint, int(lamports), sb), {}),
        ((sol_mint, out_mint, int(lamports)), {"slippageBps": sb}),
        ((sol_mint, out_mint, float(amount_sol), sb), {}),
    ]
    variants_quote = variants_quote_min + (variants_quote_extra if exhaustive else [])

    last_err = ""
    for scope, obj in (("target", target), ("module", router_mod)):
        for nm in ("get_buy_quote", "buy_quote", "quote_buy"):
            fn = getattr(obj, nm, None)
            if not callable(fn):
                continue
            sig_fill = _build_kwargs_from_signature(
                fn,
                input_mint=sol_mint,
                output_mint=out_mint,
                amount_lamports=int(lamports),
                amount_sol=float(amount_sol),
                slippage_bps=sb,
                timeout_s=timeout_s,
            )
            hit = await _try_call(
                scope,
                obj,
                (nm,),
                variants=variants_buy,
                timeout_s=timeout_s,
                deadline=deadline,
                debug=debug,
                json_only=json_only,
                trace=trace,
                sig_fill=sig_fill,
            )
            if hit.ok:
                q = _find_dict_quote(hit.val)
                if q:
                    return q, f"{scope}.{hit.used}", ""
                last_err = "quote_parse_failed"
            else:
                last_err = hit.err

    for scope, obj in (("target", target), ("module", router_mod)):
        fn_names = (
            "get_quote",
            "quote",
            "get_swap_quote",
            "swap_quote",
            "jup_quote",
            "jupiter_quote",
            "quote_swap",
        )
        sig_fill = None
        for nm in fn_names:
            fn = getattr(obj, nm, None)
            if callable(fn):
                sig_fill = _build_kwargs_from_signature(
                    fn,
                    input_mint=sol_mint,
                    output_mint=out_mint,
                    amount_lamports=int(lamports),
                    amount_sol=float(amount_sol),
                    slippage_bps=sb,
                    timeout_s=timeout_s,
                )
                break

        hit2 = await _try_call(
            scope,
            obj,
            fn_names,
            variants=variants_quote,
            timeout_s=timeout_s,
            deadline=deadline,
            debug=debug,
            json_only=json_only,
            trace=trace,
            sig_fill=sig_fill,
        )
        if hit2.ok:
            q2 = _find_dict_quote(hit2.val)
            if q2:
                return q2, f"{scope}.{hit2.used}", ""
            last_err = "quote_parse_failed"
        else:
            last_err = hit2.err

    return None, "buy_quote_failed_all_variants", last_err


async def _get_sell_quote_any(
    target: Any,
    router_mod: Any,
    *,
    in_mint: str,
    amount_raw: int,
    slippage_bps: int,
    sol_mint: str,
    timeout_s: float,
    deadline: Deadline,
    debug: bool,
    json_only: bool,
    trace: list[TraceEvent] | None,
) -> tuple[dict[str, Any] | None, str]:
    amt = int(max(1, int(amount_raw)))
    sb = int(slippage_bps)

    exhaustive = _is_exhaustive(debug)

    variants_sell_min = [
        ((), {"mint": in_mint, "amount_raw": amt, "slippage_bps": sb}),
        ((in_mint, amt, sb), {}),
        ((in_mint,), {"amount_raw": amt, "slippage_bps": sb}),
    ]
    variants_sell_extra = [
        ((), {"mint": in_mint, "amount": amt, "slippageBps": sb}),
        ((), {"mint": in_mint, "input_mint": in_mint, "amount_raw": amt, "slippage_bps": sb}),
    ]
    variants_sell = variants_sell_min + (variants_sell_extra if exhaustive else [])

    for scope, obj in (("target", target), ("module", router_mod)):
        hit = await _try_call(
            scope,
            obj,
            ("get_sell_quote", "sell_quote", "quote_sell"),
            variants=variants_sell,
            timeout_s=timeout_s,
            deadline=deadline,
            debug=debug,
            json_only=json_only,
            trace=trace,
        )
        q = _find_dict_quote(hit.val) if hit.ok else None
        if q:
            return q, f"{scope}.{hit.used}"

    variants_quote_min = [
        ((), {"inputMint": in_mint, "outputMint": sol_mint, "amount": amt, "slippageBps": sb}),
        ((), {"input_mint": in_mint, "output_mint": sol_mint, "amount": amt, "slippage_bps": sb}),
    ]
    variants_quote_extra = [
        ((), {"mint_in": in_mint, "mint_out": sol_mint, "amount_in": amt, "slippage_bps": sb}),
        ((in_mint, sol_mint, amt, sb), {}),
    ]
    variants2 = variants_quote_min + (variants_quote_extra if exhaustive else [])

    for scope, obj in (("target", target), ("module", router_mod)):
        hit2 = await _try_call(
            scope,
            obj,
            (
                "get_quote",
                "quote",
                "get_swap_quote",
                "swap_quote",
                "jup_quote",
                "jupiter_quote",
                "quote_swap",
            ),
            variants=variants2,
            timeout_s=timeout_s,
            deadline=deadline,
            debug=debug,
            json_only=json_only,
            trace=trace,
        )
        q2 = _find_dict_quote(hit2.val) if hit2.ok else None
        if q2:
            return q2, f"{scope}.{hit2.used}"

    return None, "sell_quote_failed_all_variants"


async def _get_price_ui_any(
    target: Any,
    router_mod: Any,
    mint: str,
    timeout_s: float,
    *,
    deadline: Deadline,
    debug: bool,
    json_only: bool,
    trace: list[TraceEvent] | None,
) -> tuple[float | None, str]:
    for scope, obj in (("target", target), ("module", router_mod)):
        hit = await _try_call(
            scope,
            obj,
            (
                "get_price_ui",
                "price_ui",
                "get_price",
                "price",
                "spot_price",
                "price_of_ui",
                "price_of",
            ),
            variants=[((mint,), {}), ((), {"mint": mint})],
            timeout_s=timeout_s,
            deadline=deadline,
            debug=debug,
            json_only=json_only,
            trace=trace,
        )
        if hit.ok:
            f = _to_float(hit.val, default=0.0)
            if f > 0:
                return f, f"{scope}.{hit.used}"
    return None, "price_unavailable"


# =============================================================================
# Hints
# =============================================================================
def _hintify(
    errors: list[str], warnings: list[str], *, strict: bool, require_price: bool
) -> list[str]:
    if not _envb("CHECK_ROUTER_HINTS", True):
        return []
    txt = " ".join([e.lower() for e in errors] + [w.lower() for w in warnings])

    hints: list[str] = []
    if "401" in txt or "403" in txt or "unauthorized" in txt or "forbidden" in txt:
        hints.append(
            "Auth failure detected. Set valid JUP_PRO_KEY/JUP_API_KEY OR allow public quoting (ROUTER_STRICT_AUTH=0)."
        )
    if "timeout" in txt:
        hints.append(
            "Timeout detected. Increase CHECK_ROUTER_TIMEOUT_S or switch endpoints / proxies."
        )
        hints.append(
            "If preflight, set CHECK_ROUTER_TOTAL_TIMEOUT_S < PREFLIGHT_STEP_TIMEOUT_S so it exits cleanly."
        )
    if "import_router_failed" in txt:
        hints.append(
            "Router import failed. Verify repo bootstrap and that utils_pkg is importable."
        )
    if strict and any("sell" in e.lower() for e in errors):
        hints.append(
            "Strict mode requires SELL quote. Disable --strict or ensure router exposes get_sell_quote()."
        )
    if require_price and any("price" in e.lower() for e in errors):
        hints.append(
            "Price required but unavailable. Wire router.get_price_ui() or disable --require-price."
        )
    if not hints and errors:
        hints.append(
            "Router precheck failed. Use --debug or set CHECK_ROUTER_TRACE=1 to embed call trace in JSON."
        )
    out: list[str] = []
    seen = set()
    for h in hints:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out[:10]


# =============================================================================
# CLI (repo-clean parsing: unknown args -> JSON error + exit 1)
# =============================================================================
def _build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="God-Tier Router quote-only precheck (Repo-clean FINAL v3.1)",
        add_help=True,
    )
    ap.add_argument(
        "--module",
        default=_envs("CHECK_ROUTER_MODULE", "utils_pkg.router"),
        help="Router module import path",
    )
    ap.add_argument(
        "--mint",
        default=_envs("CHECK_ROUTER_MINT", "USDC"),
        help="Token mint/symbol (USDC/SOL) or actual mint",
    )
    ap.add_argument(
        "--amount-sol",
        type=float,
        default=float(_envf("CHECK_ROUTER_AMOUNT_SOL", 0.01)),
        help="SOL amount for buy quote",
    )
    ap.add_argument(
        "--slippage-bps",
        type=int,
        default=int(_envi("CHECK_ROUTER_SLIPPAGE_BPS", 200)),
        help="Slippage bps",
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=float(_envf("CHECK_ROUTER_TIMEOUT_S", _envf("HTTP_TOTAL_TIMEOUT_SEC", 12.0))),
        help="Per-call timeout seconds",
    )
    ap.add_argument(
        "--total-timeout",
        type=float,
        default=float(_envf("CHECK_ROUTER_TOTAL_TIMEOUT_S", 0.0)),
        help="Total cap seconds (0 disables unless PREFLIGHT=1, then auto-caps)",
    )
    ap.add_argument("--strict", action="store_true", help="Require BOTH buy and sell quote")
    ap.add_argument("--require-price", action="store_true", help="Require spot price")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT")
    ap.add_argument("--json-only", action="store_true", help="Only emit JSON (no human logs)")
    ap.add_argument(
        "--debug",
        action="store_true",
        default=_envb("CHECK_ROUTER_DEBUG", False) or _envb("ROUTER_DEBUG", False),
        help="Verbose tracing (sanitized)",
    )
    ap.add_argument(
        "--force", action="store_true", help="Run even if DISABLE_SCANNERS=1 / KILL_SWITCH=1."
    )
    ap.add_argument("--list", action="store_true", help="List discovered router callables and exit")
    ap.add_argument(
        "--print-config", action="store_true", help="Print resolved config JSON and exit"
    )
    return ap


def _parse_args_repo_clean(
    argv: list[str],
) -> tuple[argparse.Namespace | None, list[str], str | None]:
    """
    Repo-clean parse:
      - never let argparse exit(2) for unknown args
      - returns (args, unknown, err_text)
    """
    ap = _build_parser()
    try:
        args, unknown = ap.parse_known_args(argv)
        return args, unknown, None
    except SystemExit as e:
        # argparse help/usage or errors. Map to repo-clean fatal, but preserve message if possible.
        return None, [], f"argparse_systemexit:{e.code}"
    except Exception as e:
        return None, [], _short_exc(e)


def _list_callables(obj: Any) -> list[str]:
    out: list[str] = []
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            if callable(getattr(obj, name)):
                out.append(name)
        except Exception:
            continue
    return sorted(out)


def _should_skip(args: argparse.Namespace) -> tuple[bool, str]:
    if _envb("CHECK_ROUTER_DISABLED", False):
        return True, "CHECK_ROUTER_DISABLED=1"
    if args.force:
        return False, ""

    kill = _envb("KILL_SWITCH", False)
    dis_scanners = _envb("DISABLE_SCANNERS", False)

    explicit_cli_intent = bool(
        args.strict
        or args.require_price
        or args.debug
        or args.json
        or args.json_only
        or args.list
        or args.print_config
        or (args.module and args.module != "utils_pkg.router")
    )

    if (kill or dis_scanners) and (not explicit_cli_intent):
        if kill:
            return True, "KILL_SWITCH=1 (no explicit CLI intent; use --force to override)"
        return True, "DISABLE_SCANNERS=1 (no explicit CLI intent; use --force to override)"

    return False, ""


def _auto_total_timeout_if_preflight(requested: float) -> float:
    """
    If running under preflight, auto-cap total runtime so this script never gets
    killed externally (rc=124). Standalone behavior unchanged.
    """
    if requested and requested > 0:
        return float(requested)

    if not _envb("PREFLIGHT", False):
        return 0.0

    step = float(_envf("PREFLIGHT_STEP_TIMEOUT_S", 60.0))
    cap = max(10.0, step - 8.0)  # buffer for wrapper overhead
    return float(cap)


async def _amain(argv: list[str]) -> int:
    _load_dotenv_best_effort(_REPO_ROOT)
    # -------------------------------------------------------------------------
    # JSON-only contract: if json_only, stdout must contain ONLY the final JSON.
    # Everything else (human logs, library logs, router logs) must go to stderr.
    # -------------------------------------------------------------------------
    env_json = _envb("CHECK_ROUTER_JSON", True)
    env_json_only = _envb("CHECK_ROUTER_JSON_ONLY", False)

    # We may need json_mode/json_only even when parsing fails
    json_only = bool(env_json_only or ("--json-only" in argv))
    json_mode = bool(env_json or env_json_only or ("--json-only" in argv) or ("--json" in argv))

    # Capture the real stdout BEFORE we redirect anything.
    real_stdout = sys.stdout
    if json_only:
        # Force all incidental prints/logs from imports/runtime to stderr.
        # The ONLY thing that may write to real stdout is our final JSON summary.
        sys.stdout = sys.stderr

    # Always write JSON summaries to this handle (real stdout).
    json_out = real_stdout

    # Parse args (repo-clean: never exit(2); unknown args => JSON error + rc=1)
    args, unknown, parse_err = _parse_args_repo_clean(argv)

    debug = (
        bool(args.debug)
        if args is not None
        else bool(_envb("CHECK_ROUTER_DEBUG", False) or _envb("ROUTER_DEBUG", False))
    )
    strict = bool(args.strict) if args is not None else bool(_envb("CHECK_ROUTER_STRICT", False))
    require_price = (
        bool(args.require_price)
        if args is not None
        else bool(_envb("CHECK_ROUTER_REQUIRE_PRICE", False))
    )
    trace_on = bool(_envb("CHECK_ROUTER_TRACE", False) or debug)
    trace: list[TraceEvent] | None = [] if trace_on else None

    # Base summary (always available for repo-clean error reporting)
    t0_ms = _now_ms()
    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "exit_code": RC_FATAL,
        "repo_root": _sanitize_text(_REPO_ROOT),
        "module": _sanitize_text(
            getattr(args, "module", _envs("CHECK_ROUTER_MODULE", "utils_pkg.router"))
        ),
        "strict": int(strict),
        "require_price": int(require_price),
        "errors": [],
        "warnings": [],
        "fix_hints": [],
        "elapsed_ms": 0,
        "contract": {
            "script": "scripts.check_router",
            "version": "v3.1",
            "exit_codes": {"ok": RC_OK, "fatal": RC_FATAL, "ctrl_c": RC_CTRL_C},
        },
    }
    if trace is not None:
        summary["trace"] = []

    # Parse failed -> repo-clean fatal (rc=1), but still emit JSON if json_mode
    if parse_err or args is None:
        summary["errors"].append(f"cli_parse_failed:{_sanitize_text(parse_err)}")
        summary["fix_hints"] = ["Check CLI flags. Use --help to see supported options."]
        summary["elapsed_ms"] = _now_ms() - t0_ms
        summary["exit_code"] = RC_FATAL
        if trace is not None:
            summary["trace"] = [t.as_dict() for t in trace][-250:]
        if json_mode:
            _safe_print(
                json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str),
                file=json_out,
            )
        else:
            _hlog(f"FAIL parse args: {parse_err}", json_only=json_only)
        return RC_FATAL

    # Unknown args -> repo-clean fatal (rc=1)
    if unknown:
        summary["errors"].append(f"unknown_args:{_sanitize_text(' '.join(unknown))}")
        summary["fix_hints"] = [
            "Remove unknown args or update scripts/check_router.py to accept them."
        ]
        summary["elapsed_ms"] = _now_ms() - t0_ms
        summary["exit_code"] = RC_FATAL
        if trace is not None:
            summary["trace"] = [t.as_dict() for t in trace][-250:]
        if json_mode:
            _safe_print(
                json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str),
                file=json_out,
            )
        else:
            _hlog(f"FAIL unknown args: {unknown}", json_only=json_only)
        return RC_FATAL

    # Re-resolve json flags now that args parsed
    json_only = bool(args.json_only or env_json_only)
    json_mode = bool(args.json or args.json_only or env_json or env_json_only)

    # If json_only becomes true here (rare), enforce redirection now.
    if json_only and sys.stdout is not sys.stderr:
        sys.stdout = sys.stderr

    timeout_s = max(3.0, float(args.timeout))
    total_timeout_s = _auto_total_timeout_if_preflight(float(args.total_timeout))
    deadline = Deadline.from_total(total_timeout_s)

    # Mark probe intent for router implementations to avoid heavy init.
    os.environ.setdefault("CHECK_ROUTER", "1")

    amount_sol = max(0.000001, float(args.amount_sol))
    slippage_bps = int(max(1, min(5000, int(args.slippage_bps))))

    # Skip logic
    skip, reason = _should_skip(args)
    if skip:
        out = {
            "ts": _ts(),
            "ok": True,
            "exit_code": RC_OK,
            "skipped": True,
            "reason": reason,
            "contract": summary["contract"],
        }
        if json_mode:
            _safe_print(
                json.dumps(_safe_jsonable(out), ensure_ascii=False, default=str),
                file=json_out,
            )
        _hlog(f"SKIP ({reason})", json_only=json_only)
        return RC_OK

    if args.print_config:
        cfg = {
            "ts": _ts(),
            "repo_root": _sanitize_text(_REPO_ROOT),
            "module": _sanitize_text(args.module),
            "strict": bool(strict),
            "require_price": bool(require_price),
            "timeout_s": float(timeout_s),
            "total_timeout_s": float(total_timeout_s),
            "mint_input": _sanitize_text(args.mint),
            "amount_sol": float(amount_sol),
            "slippage_bps": int(slippage_bps),
            "trace": int(trace_on),
            "DISABLE_SCANNERS": bool(_envb("DISABLE_SCANNERS", False)),
            "KILL_SWITCH": bool(_envb("KILL_SWITCH", False)),
            "PREFLIGHT": bool(_envb("PREFLIGHT", False)),
            "contract": summary["contract"],
        }
        _safe_print(json.dumps(_safe_jsonable(cfg), ensure_ascii=False, default=str), file=json_out)
        return RC_OK

    sol_mint, usdc_mint = _canonical_mints()
    mint_input = str(args.mint).strip()
    mi_up = mint_input.upper()

    test_mint = _normalize_mint_best_effort(mint_input)
    if mi_up == "USDC":
        test_mint = usdc_mint
    elif mi_up in {"SOL", "WSOL"}:
        test_mint = sol_mint
    if not test_mint:
        test_mint = usdc_mint

    # Extend summary with run config
    summary.update(
        {
            "module": _sanitize_text(args.module),
            "mint_input": _sanitize_text(mint_input),
            "mint": test_mint,
            "sol_mint": sol_mint,
            "amount_sol": float(amount_sol),
            "slippage_bps": int(slippage_bps),
            "timeout_s": float(timeout_s),
            "total_timeout_s": float(total_timeout_s),
            "router_target": "",
            "router_bootstrap": "",
            "router_shutdown": "",
            "timings_ms": {},
            "capabilities": {},
            "decimals": {},
            "buy_quote": {},
            "sell_quote": {},
            "price": {},
        }
    )

    if not _maybe_base58_like(test_mint):
        summary["warnings"].append(
            "mint_does_not_look_like_base58_mint (might be symbol; normalization may fail)"
        )

    _maybe_relax_router_auth_for_precheck(json_only=json_only)

    # Import router module
    try:
        import importlib

        router_mod = importlib.import_module(args.module)
    except Exception as e:
        summary["errors"].append(f"import_router_failed:{_short_exc(e)}")
        summary["fix_hints"] = _hintify(
            summary["errors"], summary["warnings"], strict=strict, require_price=require_price
        )
        summary["elapsed_ms"] = _now_ms() - t0_ms
        summary["exit_code"] = RC_FATAL
        if trace is not None:
            summary["trace"] = [t.as_dict() for t in trace][-250:]
        if json_mode:
            _safe_print(
                json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str),
                file=json_out,
            )
        else:
            _hlog(f"FAIL import {args.module} err={_short_exc(e)}", json_only=json_only)
        return RC_FATAL

    # Resolve target
    t_resolve = _now_ms()
    target, target_used = await _resolve_router_target(
        router_mod,
        timeout_s=timeout_s,
        deadline=deadline,
        debug=debug,
        json_only=json_only,
        trace=trace,
    )
    summary["router_target"] = target_used
    summary["timings_ms"]["resolve_target"] = _now_ms() - t_resolve

    if args.list:
        summary["capabilities"] = {
            "target_used": target_used,
            "callables_target": _list_callables(target),
            "callables_module": _list_callables(router_mod),
        }
        summary["ok"] = True
        summary["exit_code"] = RC_OK
        summary["elapsed_ms"] = _now_ms() - t0_ms
        if trace is not None:
            summary["trace"] = [t.as_dict() for t in trace][-250:]
        if json_mode:
            _safe_print(
                json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str),
                file=json_out,
            )
        else:
            _hlog("LIST done", json_only=json_only)
        return RC_OK

    # Router bootstrap (best-effort)
    t_boot = _now_ms()
    summary["router_bootstrap"] = await _bootstrap_router(
        target,
        timeout_s=timeout_s,
        deadline=deadline,
        debug=debug,
        json_only=json_only,
        trace=trace,
    )
    summary["timings_ms"]["router_bootstrap"] = _now_ms() - t_boot

    sol_decimals = 9

    async def _run_body() -> int:
        nonlocal sol_decimals

        # Decimals
        t_dec = _now_ms()
        d_sol, via_sol = await _get_decimals_any(
            target,
            router_mod,
            sol_mint,
            timeout_s,
            deadline=deadline,
            debug=debug,
            json_only=json_only,
            trace=trace,
        )
        d_tok, via_tok = await _get_decimals_any(
            target,
            router_mod,
            test_mint,
            timeout_s,
            deadline=deadline,
            debug=debug,
            json_only=json_only,
            trace=trace,
        )
        if d_sol:
            sol_decimals = int(d_sol)
        else:
            summary["warnings"].append("sol_decimals_unavailable_assuming_9")
            sol_decimals = 9
        summary["decimals"] = {
            "sol": {"val": d_sol, "via": via_sol},
            "token": {"val": d_tok, "via": via_tok},
        }
        summary["timings_ms"]["decimals"] = _now_ms() - t_dec

        # BUY quote (required)
        t_buy = _now_ms()
        buy_q, buy_via, buy_last_err = await _get_buy_quote_any(
            target,
            router_mod,
            out_mint=test_mint,
            amount_sol=amount_sol,
            slippage_bps=slippage_bps,
            sol_mint=sol_mint,
            sol_decimals=sol_decimals,
            timeout_s=timeout_s,
            deadline=deadline,
            debug=debug,
            json_only=json_only,
            trace=trace,
        )
        summary["timings_ms"]["buy_quote"] = _now_ms() - t_buy

        if not buy_q:
            summary["buy_quote"] = {
                "ok": 0,
                "via": buy_via,
                "last_err": _sanitize_text(buy_last_err),
            }
            summary["errors"].append(f"buy_quote_failed:{buy_via}")
            if buy_last_err:
                summary["errors"].append(f"buy_quote_last_err:{_sanitize_text(buy_last_err)}")

            if (
                _maybe_relax_on_auth_error(buy_last_err, json_only=json_only)
                and not deadline.expired()
            ):
                t_buy2 = _now_ms()
                buy_q2, buy_via2, buy_last_err2 = await _get_buy_quote_any(
                    target,
                    router_mod,
                    out_mint=test_mint,
                    amount_sol=amount_sol,
                    slippage_bps=slippage_bps,
                    sol_mint=sol_mint,
                    sol_decimals=sol_decimals,
                    timeout_s=timeout_s,
                    deadline=deadline,
                    debug=debug,
                    json_only=json_only,
                    trace=trace,
                )
                summary["timings_ms"]["buy_quote_retry"] = _now_ms() - t_buy2
                if buy_q2:
                    buy_q = buy_q2
                    buy_via = buy_via2
                else:
                    summary["buy_quote"]["retry_last_err"] = _sanitize_text(buy_last_err2)

            if not buy_q:
                return RC_FATAL

        out_raw = _parse_out_amount_raw(buy_q)
        out_ui = _raw_to_ui(out_raw, summary.get("decimals", {}).get("token", {}).get("val"))
        summary["buy_quote"] = {
            "ok": 1,
            "via": buy_via,
            "out_amount_raw": (out_raw if out_raw > 0 else None),
            "out_amount_ui": out_ui,
            **_quote_summary(buy_q),
        }
        if out_raw <= 0:
            summary["warnings"].append("buy_quote_out_amount_missing_or_zero")

        # SELL quote (optional unless strict)
        t_sell = _now_ms()
        sell_ok = False
        sell_q: dict[str, Any] | None = None
        sell_via = "skipped"

        sell_amt = 0
        if out_raw and out_raw > 0:
            clamp_max = int(_envi("CHECK_ROUTER_SELL_CLAMP_RAW", 50_000_000))
            clamp_min = int(_envi("CHECK_ROUTER_SELL_MIN_RAW", 10))
            sell_amt = int(max(clamp_min, min(int(out_raw), clamp_max)))

        if sell_amt > 0 and not deadline.expired():
            sell_q, sell_via = await _get_sell_quote_any(
                target,
                router_mod,
                in_mint=test_mint,
                amount_raw=int(sell_amt),
                slippage_bps=slippage_bps,
                sol_mint=sol_mint,
                timeout_s=timeout_s,
                deadline=deadline,
                debug=debug,
                json_only=json_only,
                trace=trace,
            )
            sell_ok = bool(sell_q)

        summary["timings_ms"]["sell_quote"] = _now_ms() - t_sell

        if sell_q:
            sell_out_raw = _parse_out_amount_raw(sell_q)
            sell_out_sol_ui = _raw_to_ui(
                sell_out_raw, summary.get("decimals", {}).get("sol", {}).get("val") or 9
            )
            summary["sell_quote"] = {
                "ok": 1,
                "via": sell_via,
                "sell_amount_raw_used": int(sell_amt),
                "out_amount_raw": (sell_out_raw if sell_out_raw > 0 else None),
                "out_amount_sol_ui": sell_out_sol_ui,
                **_quote_summary(sell_q),
            }
        else:
            summary["sell_quote"] = {
                "ok": 0,
                "via": sell_via,
                "sell_amount_raw_used": int(sell_amt),
                "note": "no_sell_quote",
            }

        # Price (best-effort)
        t_px = _now_ms()
        px, px_via = await _get_price_ui_any(
            target,
            router_mod,
            test_mint,
            timeout_s,
            deadline=deadline,
            debug=debug,
            json_only=json_only,
            trace=trace,
        )
        summary["timings_ms"]["price"] = _now_ms() - t_px
        summary["price"] = {"via": px_via, "val": px}

        ok = True
        if strict and not sell_ok:
            ok = False
            summary["errors"].append("strict_requires_sell_quote")
        if require_price and (px is None or px <= 0):
            ok = False
            summary["errors"].append("require_price_failed")

        summary["ok"] = bool(ok)
        return RC_OK if ok else RC_FATAL

    rc = RC_FATAL
    try:
        if total_timeout_s > 0:
            rc = await asyncio.wait_for(_run_body(), timeout=float(total_timeout_s))
        else:
            rc = await _run_body()
    except TimeoutError:
        summary["ok"] = False
        summary["errors"].append("total_timeout")
        rc = RC_FATAL
    finally:
        t_shut = _now_ms()
        summary["router_shutdown"] = await _shutdown_router(
            target,
            timeout_s=timeout_s,
            deadline=deadline,
            debug=debug,
            json_only=json_only,
            trace=trace,
        )
        summary["timings_ms"]["router_shutdown"] = _now_ms() - t_shut

        summary["elapsed_ms"] = _now_ms() - t0_ms
        summary["fix_hints"] = _hintify(
            summary["errors"], summary["warnings"], strict=strict, require_price=require_price
        )
        summary["exit_code"] = int(rc)

        if trace is not None:
            summary["trace"] = [t.as_dict() for t in trace][-250:]

        if json_mode:
            _safe_print(
                json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str),
                file=json_out,
            )
        else:
            _hlog(
                ("OK" if summary.get("ok") else "FAIL")
                + f" target={summary.get('router_target')}"
                + f" buy_via={summary.get('buy_quote', {}).get('via')}"
                + f" sell_via={summary.get('sell_quote', {}).get('via')}"
                + f" rc={rc} elapsed_ms={summary.get('elapsed_ms')}",
                json_only=json_only,
            )

    return int(rc)


def main() -> None:
    argv = sys.argv[1:]
    try:
        rc = asyncio.run(_amain(argv))
    except KeyboardInterrupt:
        rc = RC_CTRL_C
    except Exception:
        err = _sanitize_text(traceback.format_exc())
        _safe_print(f"[check_router] {_ts()} FAIL unhandled\n{err[:1600]}", file=sys.stderr)
        rc = RC_FATAL
    raise SystemExit(rc)


if __name__ == "__main__":
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]
    main()
