# scripts/smoke_init.py
from __future__ import annotations

"""
scripts/smoke_init.py — Smoke: Init Resources (P0 Determinism)
==============================================================

Goal
----
A safe “boot wiring” smoke test that guarantees startup wiring is not broken, while
being *offline-safe*, *deterministic*, and *secret-safe*.

Core contract
-------------
- Human logs -> STDERR
- JSON summary -> STDOUT (when enabled)
- No secrets printed (aggressive redaction; preserves base58 pubkeys/mints)
- Crash-proof: never throws; always returns a deterministic exit code
- Never hangs: timeouts around init/probes/close

Network policy (no surprises)
-----------------------------
By default, probes are OFF and network is treated as NOT allowed unless you explicitly
enable probes AND network is allowed.

Network is disallowed if any are true:
  - NO_NETWORK=1, DISABLE_NETWORK=1, SAFE_MODE=1, DRYRUN=1
  - SMOKE_INIT_NO_NETWORK=1
  - PREFLIGHT_NETWORK_ALLOWED=0 (if set)

Probes are enabled only if:
  - --probes OR SMOKE_INIT_PROBES=1
  - AND not --skip-probes / SMOKE_INIT_SKIP_PROBES=1
  - AND network is allowed

Env contract checks
-------------------
- Best-effort import of utils_pkg.contracts
- If validate_env_values(environ, strict=...) exists, run it (no network)
- STRICT_ENV aware required keys:
    * if STRICT_ENV=1 and REQUIRED_KEYS missing -> FAIL early (clean JSON)
"""

import argparse
import asyncio
import contextlib
import importlib
import inspect
import json
import os
import platform
import re
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

# =============================================================================
# Exit codes (match preflight expectations)
# =============================================================================
RC_OK = 0
RC_FAIL = 2
RC_CTRL_C = 130

SCRIPT_ID = "scripts.smoke_init"
VERSION = "v5"

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

# Hard timeouts (seconds) — prevents hangs
DEFAULT_INIT_MIN_TIMEOUT_S = 5.0
DEFAULT_CLOSE_TIMEOUT_S = 10.0
DEFAULT_PROBE_TIMEOUT_S = 4.0

# =============================================================================
# Regex / redaction primitives
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")  # scheme://userinfo@
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|passwd|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)
_QP_SECRET_RE = re.compile(r"(?i)(password|pass|token|api_key|apikey|secret)=([^&\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")

_BASE58_ALPH = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


# =============================================================================
# Small helpers
# =============================================================================
def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _strip_wrapping_quotes(s: str) -> str:
    ss = (s or "").strip()
    if (ss.startswith('"') and ss.endswith('"')) or (ss.startswith("'") and ss.endswith("'")):
        return ss[1:-1].strip()
    return ss


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return _strip_wrapping_quotes((v if v is not None else d) or "")


def _envb(k: str, d: bool = False) -> bool:
    raw = _envs(k, "")
    if raw == "":
        return bool(d)
    s = raw.strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)).replace("_", ""))
    except Exception:
        return float(d)


def _parse_csv(s: str) -> list[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _looks_like_base58_pubkey(s: str) -> bool:
    ss = (s or "").strip()
    if not (32 <= len(ss) <= 44):
        return False
    return all(ch in _BASE58_ALPH for ch in ss)


def _safe_write_line(stream: Any, s: str) -> None:
    """Robust line writer (Windows-safe). Never throws."""
    try:
        if not s.endswith("\n"):
            s += "\n"
        stream.write(s)
        stream.flush()
        return
    except UnicodeEncodeError:
        try:
            enc = getattr(stream, "encoding", None) or "utf-8"
            b = s.encode(enc, errors="replace")
            if hasattr(stream, "buffer"):
                stream.buffer.write(b)  # type: ignore[attr-defined]
                stream.buffer.flush()  # type: ignore[attr-defined]
            else:
                stream.write(b.decode(enc, errors="replace"))
                stream.flush()
        except Exception:
            return
    except Exception:
        return


def _safe_dump_json(payload: dict[str, Any], *, ensure_ascii: bool = True) -> None:
    """Dump JSON to stdout; never throws."""
    try:
        sys.stdout.write(json.dumps(payload, ensure_ascii=ensure_ascii, default=str) + "\n")
        sys.stdout.flush()
    except Exception:
        with contextlib.suppress(Exception):
            _safe_write_line(sys.stdout, json.dumps(payload, ensure_ascii=True, default=str))


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write_line(sys.stderr, f"[smoke_init] {_ts()} {msg}")


# =============================================================================
# repo-root bootstrap (direct-run friendly, no throws)
# =============================================================================
def _bootstrap_repo_root() -> str:
    """
    Detect repo root via common markers, export REPO_ROOT, and update sys.path.
    Never throws.
    """
    try:
        here = Path(__file__).resolve()
    except Exception:
        root = os.getenv("REPO_ROOT", "") or ""
        if root:
            os.environ.setdefault("REPO_ROOT", root)
        return root

    markers = (
        "pyproject.toml",
        "requirements.txt",
        "setup.cfg",
        ".git",
        ".env.example",
        "render.yaml",
    )
    candidates = [here.parent, *list(here.parents)]
    root: Path | None = None

    for p in candidates:
        try:
            if any((p / m).exists() for m in markers) or (p / "src").is_dir():
                root = p
                break
        except Exception:
            continue

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    root_s = str(root)
    os.environ.setdefault("REPO_ROOT", root_s)

    try:
        if root_s and root_s not in sys.path:
            sys.path.insert(0, root_s)
        src = root / "src"
        if src.is_dir():
            src_s = str(src)
            if src_s not in sys.path:
                sys.path.insert(0, src_s)
    except Exception:
        pass

    return root_s


# =============================================================================
# dotenv (best-effort, runtime only)
# =============================================================================
def _maybe_load_dotenv(*, disabled: bool) -> bool:
    if disabled:
        return False
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        repo = Path(os.getenv("REPO_ROOT", "") or os.getcwd())
        p = repo / ".env"
        if p.is_file():
            load_dotenv(p, override=False)
        else:
            load_dotenv(override=False)
        return True
    return False


# =============================================================================
# redaction / safety
# =============================================================================
def _sensitive_key_name(k: str) -> bool:
    kk = (k or "").upper()
    needles = (
        "SECRET",
        "TOKEN",
        "PASSWORD",
        "PASS",
        "API_KEY",
        "APIKEY",
        "KEYPAIR",
        "PRIVATE",
        "WALLET",
        "DSN",
        "DATABASE_URL",
        "POSTGRES",
        "PG_DSN",
        "REDIS_URL",
        "AUTH",
        "WEBHOOK",
    )
    return any(n in kk for n in needles)


def _redact_keys() -> list[str]:
    base = [
        "JUP_PRO_KEY",
        "JUP_API_KEY",
        "JUP_KEY",
        "OPENAI_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_TOKEN",
        "DISCORD_WEBHOOK_URL",
        "NOTION_TOKEN",
        "NOTION_DATABASE_ID",
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
        "PNL_DSN",
        "REDIS_URL",
        "REDIS_TLS_URL",
        "RPC_URL",
        "SOL_RPC_URL",
        "SOL_RPC_URLS",
    ]
    extra = _parse_csv(_envs("PREFLIGHT_REDACT_KEYS", "")) + _parse_csv(
        _envs("SMOKE_INIT_REDACT_KEYS", "")
    )
    out: list[str] = []
    seen: set[str] = set()
    for k in base + extra:
        kk = (k or "").strip()
        if kk and kk not in seen:
            out.append(kk)
            seen.add(kk)
    # Also redact anything that looks sensitive by name (keys only; values handled separately)
    for k in os.environ.keys():
        if _sensitive_key_name(k) and k not in seen:
            out.append(k)
            seen.add(k)
    return out


def _collect_secret_values(redact_keys: list[str]) -> list[str]:
    vals: list[str] = []
    # direct known keys
    for k in redact_keys:
        v = _strip_wrapping_quotes(os.getenv(k, "") or "")
        if v and len(v) >= 6:
            vals.append(v)
    # heuristics: any env where key name looks sensitive
    for k, v in os.environ.items():
        if not v:
            continue
        if _sensitive_key_name(k):
            vv = _strip_wrapping_quotes(v)
            if vv and len(vv) >= 6:
                vals.append(vv)
    # unique, longest first for best replacement
    uniq = sorted(set(vals), key=lambda s: (-len(s), s))
    return uniq


def _mask_long_tokens_enabled() -> bool:
    # Use SMOKE_INIT_MASK_LONG_TOKENS if set; else inherit PREFLIGHT_MASK_LONG_TOKENS; else default True.
    if "SMOKE_INIT_MASK_LONG_TOKENS" in os.environ:
        return _envb("SMOKE_INIT_MASK_LONG_TOKENS", True)
    if "PREFLIGHT_MASK_LONG_TOKENS" in os.environ:
        return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)
    return True


def _mask_tokenish_match(m: re.Match) -> str:
    tok = m.group(0)
    # Preserve base58 pubkeys/mints
    if _looks_like_base58_pubkey(tok):
        return tok
    return "***"


def _sanitize(x: Any, *, secret_values: list[str]) -> str:
    if x is None:
        return ""
    s = str(x)

    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_HDR_RE.sub(r"\1***", s)
    s = _QP_SECRET_RE.sub(r"\1=***", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)

    for v in secret_values:
        with contextlib.suppress(Exception):
            if v and v in s:
                s = s.replace(v, "***")

    if _mask_long_tokens_enabled():
        s = _LONG_TOKENISH_RE.sub(_mask_tokenish_match, s)

    return s


def _dedupe(xs: list[str], cap: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in xs:
        x = (x or "").strip()
        if x and x not in seen:
            out.append(x)
            seen.add(x)
        if len(out) >= cap:
            break
    return out


# =============================================================================
# policy: network allowed?
# =============================================================================
def _network_allowed() -> bool:
    # Explicit override from preflight
    if "PREFLIGHT_NETWORK_ALLOWED" in os.environ and not _envb("PREFLIGHT_NETWORK_ALLOWED", True):
        return False
    if _envb("SMOKE_INIT_NO_NETWORK", False):
        return False
    if _envb("NO_NETWORK", False):
        return False
    if _envb("DISABLE_NETWORK", False):
        return False
    if _envb("SAFE_MODE", False):
        return False
    if _envb("DRYRUN", False):
        return False
    return True


# =============================================================================
# sqlite path guard (prevents Git-for-Windows /opt landmine)
# =============================================================================
def _pnorm(p: str) -> str:
    return (p or "").replace("\\", "/").strip()


def _looks_like_git_for_windows_opt_path(p: str) -> bool:
    s = _pnorm(p).lower()
    if "program files/git" in s and ("/opt" in s or "/usr" in s):
        return True
    if s.startswith("c:/program files/git/"):
        return True
    return False


def _ensure_repo_sqlite_path(repo_root: str, checks: list[dict[str, Any]]) -> dict[str, Any]:
    repo = Path(repo_root or os.getcwd())
    try:
        repo = repo.resolve()
    except Exception:
        repo = Path(repo_root or os.getcwd())

    state_dir = repo / ".state"
    sqlite_path = state_dir / "state.sqlite3"

    patched = False
    reason = ""

    try:
        state_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        state_dir = repo
        sqlite_path = state_dir / "state.sqlite3"
        reason = "state_dir_create_failed_fallback_repo"

    cur = _envs("SQLITE_DB_PATH", "").strip()
    if not cur:
        os.environ["SQLITE_DB_PATH"] = str(sqlite_path)
        patched = True
        reason = "unset"
    elif _looks_like_git_for_windows_opt_path(cur):
        os.environ["SQLITE_DB_PATH"] = str(sqlite_path)
        patched = True
        reason = "git_opt_landmine"

    checks.append(
        {
            "name": "sqlite_path_patch",
            "ok": True,
            "detail": os.environ.get("SQLITE_DB_PATH", str(sqlite_path)),
        }
    )
    return {
        "state_dir": str(state_dir),
        "sqlite_path": str(os.environ.get("SQLITE_DB_PATH", str(sqlite_path))),
        "patched": int(patched),
        "reason": reason,
    }


# =============================================================================
# async helpers
# =============================================================================
async def _maybe_await(x: Any) -> Any:
    if inspect.isawaitable(x):
        return await x
    return x


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    out = fn(*args, **kwargs)
    return await _maybe_await(out)


async def _with_timeout(coro: Any, timeout_s: float) -> Any:
    return await asyncio.wait_for(coro, timeout=max(0.1, float(timeout_s)))


# =============================================================================
# probes (best-effort, non-breaking)
# =============================================================================
async def _probe_redis(cu: Any, *, secret_values: list[str]) -> tuple[bool, str]:
    getter = None
    for name in ("get_redis_async", "get_redis", "get_redis_client"):
        if hasattr(cu, name):
            getter = getattr(cu, name)
            break
    if getter is None:
        return True, "redis_probe_skipped(no_getter)"

    try:
        r = await _call_maybe_async(getter)
        if r is None:
            return False, "redis_client_none"
        if hasattr(r, "ping"):
            ok = await _maybe_await(r.ping())
            return (bool(ok) if ok is not None else True), "redis_ping_ok"
        return True, "redis_probe_skipped(no_ping)"
    except Exception as e:
        return (
            False,
            f"redis_probe_err:{type(e).__name__}:{_sanitize(e, secret_values=secret_values)}",
        )


async def _probe_pg(cu: Any, *, secret_values: list[str]) -> tuple[bool, str]:
    getter = None
    for name in ("get_pg_pool", "get_pg_pool_async", "get_pg_conn", "get_db_pool"):
        if hasattr(cu, name):
            getter = getattr(cu, name)
            break
    if getter is None:
        return True, "pg_probe_skipped(no_getter)"

    try:
        obj = await _call_maybe_async(getter)
        if obj is None:
            return False, "pg_obj_none"

        if hasattr(obj, "acquire"):
            async with obj.acquire() as conn:
                if hasattr(conn, "fetchval"):
                    v = await _maybe_await(conn.fetchval("SELECT 1"))
                    return (int(v) == 1), "pg_select1_ok"
                if hasattr(conn, "execute"):
                    await _maybe_await(conn.execute("SELECT 1"))
                    return True, "pg_select1_ok"
            return True, "pg_probe_ok"

        if hasattr(obj, "fetchval"):
            v = await _maybe_await(obj.fetchval("SELECT 1"))
            return (int(v) == 1), "pg_select1_ok"

        if hasattr(obj, "execute"):
            await _maybe_await(obj.execute("SELECT 1"))
            return True, "pg_select1_ok"

        return True, "pg_probe_skipped(unknown_obj)"
    except Exception as e:
        return False, f"pg_probe_err:{type(e).__name__}:{_sanitize(e, secret_values=secret_values)}"


def _fix_hints_from_error(err_text: str) -> list[str]:
    blob = (err_text or "").lower()
    hints: list[str] = []

    if "missing env" in blob or "missing environment" in blob or "keyerror" in blob:
        hints.append("Missing environment variables. Verify Render Secrets and .env wiring.")
    if ("postgres" in blob or "asyncpg" in blob) and (
        "dsn" in blob or "database_url" in blob or "connect" in blob
    ):
        hints.append(
            "Postgres DSN/pool issue. Verify DATABASE_URL/POSTGRES_DSN and Render networking/sslmode."
        )
    if "redis" in blob and ("refused" in blob or "timeout" in blob or "connect" in blob):
        hints.append("Redis connectivity issue. Verify REDIS_URL/REDIS_TLS_URL and TLS settings.")
    if any(x in blob for x in ("unauthorized", "forbidden", "401", "403")):
        hints.append("Auth error. Verify API keys/endpoints (do not print secrets).")
    if "timeout" in blob:
        hints.append(
            "Timeout. Increase SMOKE_INIT_TIMEOUT_S or fix slow dependency initialization."
        )
    return _dedupe(hints, 12)


# =============================================================================
# env contract checks (STRICT_ENV-aware)
# =============================================================================
def _strict_env_enabled(environ: Mapping[str, str] | None = None) -> bool:
    env = dict(os.environ) if environ is None else dict(environ)
    v = (env.get("STRICT_ENV") or env.get("ENV_STRICT") or "0").strip()
    return v.lower() in _TRUE


def _required_missing(required_keys: set[str], environ: Mapping[str, str]) -> list[str]:
    missing: list[str] = []
    for k in sorted(required_keys):
        if (environ.get(k) or "").strip() == "":
            missing.append(k)
    return missing


def _contracts_env_check(
    *,
    secret_values: list[str],
    checks: list[dict[str, Any]],
    warnings: list[str],
    fix_hints: list[str],
) -> dict[str, Any]:
    meta: dict[str, Any] = {"contracts_loaded": 0}

    try:
        c = importlib.import_module("utils_pkg.contracts")
        meta["contracts_loaded"] = 1

        rep = getattr(c, "validate_env_values", None)
        if callable(rep):
            out = rep(os.environ, strict=False)
            ok = bool(out.get("ok", True))
            errs = list(out.get("errors") or [])
            warns = list(out.get("warnings") or [])

            checks.append(
                {
                    "name": "env_values",
                    "ok": ok,
                    "detail": "ok" if ok else "invalid_values",
                    "errors": [_sanitize(e, secret_values=secret_values) for e in errs[:40]],
                    "warnings": [_sanitize(w, secret_values=secret_values) for w in warns[:40]],
                }
            )
            for w in warns[:12]:
                warnings.append(str(w))
            if errs:
                fix_hints.append("Fix invalid env values (see env_values.errors).")

        req = getattr(c, "REQUIRED_KEYS", None)
        if isinstance(req, set):
            strict = _strict_env_enabled(os.environ)
            missing = _required_missing(req, os.environ)
            meta["required_keys_count"] = int(len(req))
            meta["required_missing_count"] = int(len(missing))
            meta["strict_env"] = int(strict)

            if missing:
                if strict:
                    checks.append(
                        {
                            "name": "required_env",
                            "ok": False,
                            "detail": f"missing:{len(missing)}",
                            "missing": missing[:80],
                        }
                    )
                    fix_hints.append(
                        "STRICT_ENV=1 and required env keys are missing (see required_env.missing)."
                    )
                else:
                    checks.append(
                        {
                            "name": "required_env",
                            "ok": True,
                            "detail": f"missing_warn:{len(missing)}",
                            "missing": missing[:80],
                        }
                    )
                    warnings.append(
                        f"Missing required env keys (STRICT_ENV=0 => warn): {', '.join(missing[:12])}"
                    )
            else:
                checks.append({"name": "required_env", "ok": True, "detail": "present"})

        keys = getattr(c, "ENV_KEYS", None)
        if isinstance(keys, set):
            meta["contract_keys_count"] = int(len(keys))

    except Exception as e:
        msg = _sanitize(f"{type(e).__name__}: {e}", secret_values=secret_values)
        checks.append({"name": "env_contract", "ok": False, "detail": msg})
        warnings.append("Could not import utils_pkg.contracts for env checks.")
        fix_hints.append("Fix utils_pkg.contracts import so P0 env validation can run.")

    return meta


# =============================================================================
# report model
# =============================================================================
@dataclass
class SmokeResult:
    ok: bool
    detail: str
    sec: float
    checks: list[dict[str, Any]]
    warnings: list[str]
    fix_hints: list[str]
    meta: dict[str, Any]


def _as_json(res: SmokeResult) -> dict[str, Any]:
    return {
        "ts": _ts(),
        "ok": bool(res.ok),
        "exit_code": RC_OK if res.ok else RC_FAIL,
        "step": "smoke_init",
        "detail": res.detail,
        "sec": round(float(res.sec), 3),
        "warnings": list(res.warnings),
        "fix_hints": list(res.fix_hints),
        "checks": list(res.checks),
        "meta": dict(res.meta),
        "contract": {
            "script": SCRIPT_ID,
            "version": VERSION,
            "exit_codes": {"ok": RC_OK, "fail": RC_FAIL, "ctrl_c": RC_CTRL_C},
            "no_network_default": True,
        },
    }


# =============================================================================
# CLI
# =============================================================================
class _ArgParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # noqa: D401
        raise ValueError(message)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    ap = _ArgParser(description="Smoke test: init core resources safely.")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only emit JSON (no human logs).")
    ap.add_argument(
        "--timeout",
        type=float,
        default=_envf("SMOKE_INIT_TIMEOUT_S", _envf("PREFLIGHT_TIMEOUT_S", 60.0)),
        help="Timeout for init+probes (seconds).",
    )
    ap.add_argument("--probes", action="store_true", help="Enable optional redis/pg probes.")
    ap.add_argument("--skip-probes", action="store_true", help="Skip optional probes.")
    ap.add_argument(
        "--no-close", action="store_true", help="Do not call close_resources() (debug)."
    )
    ap.add_argument("--no-dotenv", action="store_true", help="Do not load .env via python-dotenv.")
    return ap.parse_args(argv)


def _probes_enabled(args: argparse.Namespace) -> bool:
    if bool(getattr(args, "skip_probes", False)) or _envb("SMOKE_INIT_SKIP_PROBES", False):
        return False
    if not _network_allowed():
        return False
    if bool(getattr(args, "probes", False)):
        return True
    if "SMOKE_INIT_PROBES" in os.environ:
        return _envb("SMOKE_INIT_PROBES", False)
    return False


# =============================================================================
# main logic
# =============================================================================
async def _run(args: argparse.Namespace) -> SmokeResult:
    # Safety defaults for smoke runs (do not override explicit user choices)
    os.environ.setdefault("SMOKE_INIT", "1")
    os.environ.setdefault("SMOKE", "1")
    os.environ.setdefault("DRYRUN", os.getenv("DRYRUN", "1"))
    os.environ.setdefault("DISABLE_BUYS", os.getenv("DISABLE_BUYS", "1"))
    os.environ.setdefault("DISABLE_SELLS", os.getenv("DISABLE_SELLS", "1"))
    os.environ.setdefault("DISABLE_SCANNERS", os.getenv("DISABLE_SCANNERS", "1"))

    repo_root = _bootstrap_repo_root()

    dotenv_loaded = _maybe_load_dotenv(
        disabled=bool(args.no_dotenv or _envb("SMOKE_INIT_NO_DOTENV", False))
    )
    json_only = bool(
        args.json_only
        or _envb("SMOKE_INIT_JSON_ONLY", False)
        or _envb("PREFLIGHT_JSON_ONLY", False)
    )

    checks: list[dict[str, Any]] = []
    warnings: list[str] = []
    fix_hints: list[str] = []

    redact_keys = _redact_keys()
    secret_values = _collect_secret_values(redact_keys)

    t0 = time.monotonic()

    # Ensure SQLITE_DB_PATH is repo-local/writable
    state_info = _ensure_repo_sqlite_path(repo_root, checks)

    # Env contract checks (STRICT_ENV-aware)
    env_meta = _contracts_env_check(
        secret_values=secret_values, checks=checks, warnings=warnings, fix_hints=fix_hints
    )

    # Fail early if STRICT_ENV=1 and required env missing
    for chk in checks:
        if chk.get("name") == "required_env" and chk.get("ok") is False:
            sec = time.monotonic() - t0
            warnings2 = _dedupe([_sanitize(w, secret_values=secret_values) for w in warnings], 12)
            hints2 = _dedupe([_sanitize(h, secret_values=secret_values) for h in fix_hints], 12)
            meta = {
                "script": SCRIPT_ID,
                "version": VERSION,
                "python": _sanitize(sys.version.split()[0], secret_values=secret_values),
                "platform": _sanitize(platform.platform(), secret_values=secret_values),
                "pid": os.getpid(),
                "cwd": _sanitize(os.getcwd(), secret_values=secret_values),
                "repo_root": _sanitize(
                    repo_root or os.getenv("REPO_ROOT", ""), secret_values=secret_values
                ),
                "dotenv_loaded": int(dotenv_loaded),
                "network_allowed": int(_network_allowed()),
                "probes_enabled": 0,
                "state": state_info,
                "env": env_meta,
            }
            return SmokeResult(
                ok=False,
                detail="strict_env_missing_required",
                sec=float(sec),
                checks=checks,
                warnings=warnings2,
                fix_hints=hints2,
                meta=meta,
            )

    probes_on = _probes_enabled(args)
    no_close = bool(args.no_close or _envb("SMOKE_INIT_NO_CLOSE", False))
    init_timeout = max(DEFAULT_INIT_MIN_TIMEOUT_S, float(args.timeout))
    probe_timeout = float(_envf("SMOKE_INIT_PROBE_TIMEOUT_S", DEFAULT_PROBE_TIMEOUT_S))
    close_timeout = float(_envf("SMOKE_INIT_CLOSE_TIMEOUT_S", DEFAULT_CLOSE_TIMEOUT_S))

    cu: Any | None = None
    ok = False
    detail = "init_not_started"

    try:
        cu = importlib.import_module("utils_pkg.core_utils")
        if not hasattr(cu, "init_resources"):
            raise RuntimeError("utils_pkg.core_utils.init_resources is missing")

        _hlog(
            f"init_resources(timeout={init_timeout:.1f}s) probes={int(probes_on)} close={int(not no_close)}",
            json_only=json_only,
        )

        await _with_timeout(_call_maybe_async(cu.init_resources), timeout_s=init_timeout)
        checks.append({"name": "init_resources", "ok": True})

        if probes_on:
            try:
                r_ok, r_msg = await _with_timeout(
                    _probe_redis(cu, secret_values=secret_values), timeout_s=probe_timeout
                )
            except Exception as e:
                r_ok, r_msg = False, f"redis_probe_timeout_or_err:{type(e).__name__}"
            checks.append(
                {
                    "name": "redis_probe",
                    "ok": bool(r_ok),
                    "detail": _sanitize(r_msg, secret_values=secret_values),
                }
            )
            if not r_ok:
                warnings.append("Redis probe failed (resources may be partially healthy).")

            try:
                p_ok, p_msg = await _with_timeout(
                    _probe_pg(cu, secret_values=secret_values), timeout_s=probe_timeout
                )
            except Exception as e:
                p_ok, p_msg = False, f"pg_probe_timeout_or_err:{type(e).__name__}"
            checks.append(
                {
                    "name": "pg_probe",
                    "ok": bool(p_ok),
                    "detail": _sanitize(p_msg, secret_values=secret_values),
                }
            )
            if not p_ok:
                warnings.append("Postgres probe failed (resources may be partially healthy).")
        else:
            checks.append({"name": "probes", "ok": True, "detail": "probes_skipped"})

        ok = True
        detail = "init_ok"

    except asyncio.TimeoutError:
        ok = False
        detail = f"timeout_after_{init_timeout:.0f}s"
        checks.append({"name": "smoke_init", "ok": False, "detail": detail})
        fix_hints.append("Increase SMOKE_INIT_TIMEOUT_S or fix slow dependency initialization.")

    except Exception as e:
        ok = False
        err_s = _sanitize(f"{type(e).__name__}: {e}", secret_values=secret_values)
        detail = err_s
        checks.append({"name": "smoke_init", "ok": False, "detail": err_s})
        fix_hints.extend(_fix_hints_from_error(err_s))

        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        checks.append(
            {
                "name": "trace",
                "ok": False,
                "detail": _sanitize(tb, secret_values=secret_values)[-4000:],
            }
        )

    finally:
        if not no_close:
            with contextlib.suppress(Exception):
                if cu is None:
                    cu = importlib.import_module("utils_pkg.core_utils")
                if cu is not None and hasattr(cu, "close_resources"):
                    try:
                        await _with_timeout(
                            _call_maybe_async(cu.close_resources), timeout_s=close_timeout
                        )
                        checks.append({"name": "close_resources", "ok": True})
                    except Exception as e:
                        checks.append(
                            {
                                "name": "close_resources",
                                "ok": False,
                                "detail": _sanitize(
                                    f"{type(e).__name__}: {e}", secret_values=secret_values
                                ),
                            }
                        )
                        warnings.append("close_resources failed or timed out.")
                else:
                    checks.append(
                        {"name": "close_resources", "ok": True, "detail": "missing_close_resources"}
                    )
        else:
            checks.append({"name": "close_resources", "ok": True, "detail": "skipped(--no-close)"})

    sec = time.monotonic() - t0
    warnings2 = _dedupe([_sanitize(w, secret_values=secret_values) for w in warnings], 12)
    hints2 = _dedupe([_sanitize(h, secret_values=secret_values) for h in fix_hints], 12)

    meta = {
        "script": SCRIPT_ID,
        "version": VERSION,
        "python": _sanitize(sys.version.split()[0], secret_values=secret_values),
        "platform": _sanitize(platform.platform(), secret_values=secret_values),
        "pid": os.getpid(),
        "cwd": _sanitize(os.getcwd(), secret_values=secret_values),
        "repo_root": _sanitize(
            repo_root or os.getenv("REPO_ROOT", ""), secret_values=secret_values
        ),
        "dotenv_loaded": int(dotenv_loaded),
        "network_allowed": int(_network_allowed()),
        "probes_enabled": int(probes_on),
        "state": state_info,
        "env": env_meta,
    }

    return SmokeResult(
        ok=bool(ok),
        detail=detail,
        sec=float(sec),
        checks=checks,
        warnings=warnings2,
        fix_hints=hints2,
        meta=meta,
    )


def cli(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

    argv_list = list(argv if argv is not None else sys.argv[1:])

    try:
        args = _parse_args(argv_list)
    except Exception as e:
        json_only = _envb("SMOKE_INIT_JSON_ONLY", False) or _envb("PREFLIGHT_JSON_ONLY", False)
        emit_json = (
            _envb("SMOKE_INIT_JSON", False) or _envb("PREFLIGHT_JSON", False) or bool(json_only)
        )
        redact_keys = _redact_keys()
        secret_values = _collect_secret_values(redact_keys)
        msg = _sanitize(str(e), secret_values=secret_values)

        payload = {
            "ts": _ts(),
            "ok": False,
            "exit_code": RC_FAIL,
            "step": "smoke_init",
            "detail": "cli_parse_error",
            "sec": 0.0,
            "warnings": [],
            "fix_hints": [
                "Fix CLI flags; preflight expects scripts/smoke_init.py --json-only to work."
            ],
            "checks": [
                {"name": "cli", "ok": False, "detail": msg, "argv": [a for a in argv_list[:120]]}
            ],
            "meta": {
                "script": SCRIPT_ID,
                "version": VERSION,
                "python": sys.version.split()[0],
                "cwd": os.getcwd(),
            },
            "contract": {
                "script": SCRIPT_ID,
                "version": VERSION,
                "exit_codes": {"ok": RC_OK, "fail": RC_FAIL, "ctrl_c": RC_CTRL_C},
                "no_network_default": True,
            },
        }

        if emit_json:
            _safe_dump_json(payload, ensure_ascii=True)
        if not json_only:
            _safe_write_line(sys.stderr, "smoke_init: FAILED (cli)")
            _safe_write_line(sys.stderr, f"  error: {msg}")
        return RC_FAIL

    json_only = bool(
        args.json_only
        or _envb("SMOKE_INIT_JSON_ONLY", False)
        or _envb("PREFLIGHT_JSON_ONLY", False)
    )
    emit_json = bool(
        args.json
        or args.json_only
        or _envb("SMOKE_INIT_JSON", False)
        or _envb("PREFLIGHT_JSON", False)
        or json_only
    )

    try:
        res = asyncio.run(_run(args))
    except KeyboardInterrupt:
        return RC_CTRL_C

    payload = _as_json(res)

    if emit_json:
        _safe_dump_json(payload, ensure_ascii=True)

    if not json_only:
        _hlog(
            "OK init_resources + cleanup succeeded" if res.ok else f"FAIL {res.detail}",
            json_only=False,
        )

    return RC_OK if res.ok else RC_FAIL


def main() -> None:
    raise SystemExit(int(cli()))


if __name__ == "__main__":
    main()
