# scripts/smoke_init.py
from __future__ import annotations

"""
scripts/smoke_init.py — Smoke: Init Resources (Phase 0–7)
=========================================================

Purpose
-------
A safe “boot wiring” smoke test that guarantees startup wiring is not broken:
  - loads dotenv (best effort)
  - patches SQLITE_DB_PATH to a repo-local, writable location if needed
  - imports core wiring
  - calls utils_pkg.core_utils.init_resources()
  - optionally performs non-destructive probes (redis ping / pg SELECT 1) if exposed
  - calls utils_pkg.core_utils.close_resources() (best effort)

Contracts
---------
- Human logs -> STDERR
- JSON summary -> STDOUT (when enabled)
- No secrets printed (aggressive redaction)
- Crash-proof: never throws; always returns an exit code
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
from typing import Any, Callable

RC_OK = 0
RC_FAIL = 2
RC_CTRL_C = 130

SCRIPT_ID = "scripts.smoke_init"
VERSION = "v4"

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

# =============================================================================
# repo-root bootstrap (direct-run friendly)
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

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
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
# env helpers
# =============================================================================


def _strip_wrapping_quotes(s: str) -> str:
    ss = (s or "").strip()
    if (ss.startswith('"') and ss.endswith('"')) or (ss.startswith("'") and ss.endswith("'")):
        return ss[1:-1].strip()
    return ss


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return _strip_wrapping_quotes((v if v is not None else d) or "")


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


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)).replace("_", ""))
    except Exception:
        return float(d)


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# =============================================================================
# safe I/O (Windows encoding-safe)
# =============================================================================


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


def _safe_dump_json(payload: dict[str, Any]) -> None:
    """Dump JSON to stdout; never throws."""
    try:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
        sys.stdout.flush()
    except Exception:
        _safe_write_line(sys.stdout, json.dumps(payload, ensure_ascii=False, default=str))


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write_line(sys.stderr, f"[smoke_init] {_ts()} {msg}")


# =============================================================================
# redaction / safety
# =============================================================================

_URL_CRED_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^@\s/]+@)")  # scheme://userinfo@
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|passwd|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)
_QP_SECRET_RE = re.compile(r"(?i)(password|pass|token|api_key|apikey|secret)=([^&\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


def _parse_csv(s: str) -> list[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _is_sensitive_key(k: str) -> bool:
    kk = (k or "").upper()
    return any(x in kk for x in ("KEY", "TOKEN", "SECRET", "PASSWORD", "PASS", "PRIVATE"))


def _redact_keys() -> list[str]:
    base = [
        "JUP_PRO_KEY",
        "JUP_API_KEY",
        "JUP_KEY",
        "JUPITER_API_KEY",
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
    return out


_REDACT_KEY_LIST = _redact_keys()


def _collect_secret_values() -> set[str]:
    vals: set[str] = set()
    try:
        for k in _REDACT_KEY_LIST:
            v = os.getenv(k, "") or ""
            v = _strip_wrapping_quotes(v)
            if v and 6 <= len(v) <= 8192:
                vals.add(v)

        for k, v in os.environ.items():
            if not v:
                continue
            if _is_sensitive_key(k):
                vv = _strip_wrapping_quotes(v)
                if vv and 6 <= len(vv) <= 8192:
                    vals.add(vv)
    except Exception:
        return vals
    return vals


def _mask_long_tokens_enabled() -> bool:
    if "SMOKE_INIT_MASK_LONG_TOKENS" in os.environ:
        return _envb("SMOKE_INIT_MASK_LONG_TOKENS", True)
    if "PREFLIGHT_MASK_LONG_TOKENS" in os.environ:
        return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)
    return True


def _sanitize(x: Any, *, secret_values: set[str]) -> str:
    if x is None:
        return ""
    s = str(x)

    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_HDR_RE.sub(r"\1***", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)
    s = _QP_SECRET_RE.sub(r"\1=***", s)

    for v in secret_values:
        try:
            if v and v in s:
                s = s.replace(v, "***")
        except Exception:
            continue

    if _mask_long_tokens_enabled():
        s = _LONG_TOKENISH_RE.sub("***", s)

    return s


# =============================================================================
# optional dotenv (best-effort)
# =============================================================================


def _maybe_load_dotenv(*, disabled: bool) -> None:
    if disabled:
        return
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        # Prefer repo-root .env if present
        repo = Path(os.getenv("REPO_ROOT", "") or os.getcwd())
        p = repo / ".env"
        if p.is_file():
            load_dotenv(p, override=False)
        else:
            load_dotenv(override=False)


# =============================================================================
# sqlite path guard (no hidden landmines)
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
    """
    Ensure SQLITE_DB_PATH is a writable repo-local path.
    Never throws. Returns state dict for meta.
    """
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
        # If we can't create .state, fall back to cwd
        state_dir = repo
        sqlite_path = state_dir / "state.sqlite3"
        reason = "state_dir_create_failed_fallback_repo"

    cur = _envs("SQLITE_DB_PATH", "").strip()
    if not cur:
        os.environ["SQLITE_DB_PATH"] = str(sqlite_path)
        patched = True
        reason = "unset"
    else:
        # Patch only if it smells like the Git-for-Windows /opt landmine
        if _looks_like_git_for_windows_opt_path(cur):
            os.environ["SQLITE_DB_PATH"] = str(sqlite_path)
            patched = True
            reason = "git_opt_landmine"

    if patched:
        checks.append({"name": "sqlite_path_patch", "ok": True, "detail": str(sqlite_path)})
    else:
        checks.append({"name": "sqlite_path_patch", "ok": True, "detail": cur or str(sqlite_path)})

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


# =============================================================================
# probes (best-effort, non-breaking)
# =============================================================================


async def _probe_redis(cu: Any, *, secret_values: set[str]) -> tuple[bool, str]:
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


async def _probe_pg(cu: Any, *, secret_values: set[str]) -> tuple[bool, str]:
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

    if "missing env" in blob or "missing env vars" in blob or "keyerror" in blob:
        hints.append("Missing environment variables. Verify Render Secrets and .env wiring.")
    if "postgres" in blob and ("dsn" in blob or "database_url" in blob or "asyncpg" in blob):
        hints.append("Postgres DSN/pool issue. Verify POSTGRES_DSN/PG_DSN and Render networking.")
    if "redis" in blob and ("url" in blob or "connection" in blob or "refused" in blob):
        hints.append("Redis connectivity issue. Verify REDIS_URL/REDIS_TLS_URL and TLS settings.")
    if "rpc" in blob and ("timeout" in blob or "blockhash" in blob or "http" in blob):
        hints.append("RPC connectivity issue. Verify SOL_RPC_URL/RPC_URL and provider uptime.")
    if any(x in blob for x in ("unauthorized", "forbidden", "401", "403")):
        hints.append("Auth error. Verify API keys/endpoints (do not print secrets).")

    out: list[str] = []
    seen: set[str] = set()
    for h in hints:
        if h and h not in seen:
            out.append(h)
            seen.add(h)
    return out[:12]


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
        "warnings": res.warnings,
        "fix_hints": res.fix_hints,
        "checks": res.checks,
        "meta": res.meta,
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


def _apply_cli_env_overrides(argv: list[str]) -> None:
    if "--json" in argv:
        os.environ["SMOKE_INIT_JSON"] = "1"
    if "--json-only" in argv:
        os.environ["SMOKE_INIT_JSON_ONLY"] = "1"
        os.environ["SMOKE_INIT_JSON"] = "1"
    if "--skip-probes" in argv:
        os.environ["SMOKE_INIT_SKIP_PROBES"] = "1"
    if "--probes" in argv:
        os.environ["SMOKE_INIT_PROBES"] = "1"
    if "--no-close" in argv:
        os.environ["SMOKE_INIT_NO_CLOSE"] = "1"
    if "--no-dotenv" in argv:
        os.environ["SMOKE_INIT_NO_DOTENV"] = "1"
    if "--timeout" in argv:
        try:
            idx = argv.index("--timeout")
            if idx + 1 < len(argv):
                os.environ["SMOKE_INIT_TIMEOUT_S"] = argv[idx + 1]
        except Exception:
            pass


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


# =============================================================================
# main logic
# =============================================================================


def _policy_no_network() -> bool:
    if "PREFLIGHT_NETWORK_ALLOWED" in os.environ and not _envb("PREFLIGHT_NETWORK_ALLOWED", True):
        return True
    if _envb("SMOKE_INIT_NO_NETWORK", False):
        return True
    return False


def _probes_enabled(args: argparse.Namespace) -> bool:
    if bool(getattr(args, "skip_probes", False)) or _envb("SMOKE_INIT_SKIP_PROBES", False):
        return False
    if bool(getattr(args, "probes", False)):
        return True
    if "SMOKE_INIT_PROBES" in os.environ:
        return _envb("SMOKE_INIT_PROBES", False)
    if _policy_no_network():
        return False
    return True


async def _run(args: argparse.Namespace) -> SmokeResult:
    os.environ.setdefault("SMOKE_INIT", "1")
    os.environ.setdefault("SMOKE", "1")
    os.environ.setdefault("DRYRUN", os.getenv("DRYRUN", "1"))
    os.environ.setdefault("DISABLE_BUYS", os.getenv("DISABLE_BUYS", "1"))
    os.environ.setdefault("DISABLE_SELLS", os.getenv("DISABLE_SELLS", "1"))
    os.environ.setdefault("DISABLE_SCANNERS", os.getenv("DISABLE_SCANNERS", "1"))

    repo_root = _bootstrap_repo_root()

    dotenv_disabled = bool(args.no_dotenv or _envb("SMOKE_INIT_NO_DOTENV", False))
    _maybe_load_dotenv(disabled=dotenv_disabled)

    probes_on = _probes_enabled(args)
    no_close = bool(args.no_close or _envb("SMOKE_INIT_NO_CLOSE", False))

    secret_values = _collect_secret_values()

    checks: list[dict[str, Any]] = []
    warnings: list[str] = []
    fix_hints: list[str] = []

    ok = False
    detail = ""

    t0 = time.monotonic()
    cu: Any | None = None

    # Ensure SQLITE_DB_PATH is repo-local/writable (prevents Git-for-Windows /opt landmine)
    state_info = _ensure_repo_sqlite_path(repo_root, checks)

    try:
        cu = importlib.import_module("utils_pkg.core_utils")

        if not hasattr(cu, "init_resources"):
            raise RuntimeError("utils_pkg.core_utils.init_resources is missing")

        if not hasattr(cu, "close_resources"):
            warnings.append(
                "utils_pkg.core_utils.close_resources is missing (cleanup may leak resources)"
            )

        async def _do() -> None:
            nonlocal ok, detail

            await _call_maybe_async(cu.init_resources)
            checks.append({"name": "init_resources", "ok": True})

            if probes_on:
                r_ok, r_msg = await _probe_redis(cu, secret_values=secret_values)
                checks.append({"name": "redis_probe", "ok": bool(r_ok), "detail": r_msg})
                if not r_ok:
                    warnings.append("Redis probe failed (resources may be partially healthy).")

                p_ok, p_msg = await _probe_pg(cu, secret_values=secret_values)
                checks.append({"name": "pg_probe", "ok": bool(p_ok), "detail": p_msg})
                if not p_ok:
                    warnings.append("Postgres probe failed (resources may be partially healthy).")
            else:
                checks.append({"name": "probes", "ok": True, "detail": "probes_skipped"})

            ok = True
            detail = "init_ok"

        await asyncio.wait_for(_do(), timeout=max(5.0, float(args.timeout)))

    except asyncio.TimeoutError:
        ok = False
        detail = f"timeout_after_{float(args.timeout):.0f}s"
        checks.append({"name": "smoke_init", "ok": False, "detail": detail})
        fix_hints.append("Increase SMOKE_INIT_TIMEOUT_S or fix slow dependencies (PG/Redis/RPC).")

    except Exception as e:
        ok = False
        err_s = _sanitize(f"{type(e).__name__}: {e}", secret_values=secret_values)
        detail = err_s
        checks.append({"name": "smoke_init", "ok": False, "detail": err_s})
        fix_hints.extend(_fix_hints_from_error(err_s))

        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        tb = _sanitize(tb, secret_values=secret_values)
        checks.append({"name": "trace", "ok": False, "detail": tb[-4000:]})

    finally:
        if not no_close:
            with contextlib.suppress(Exception):
                if cu is None:
                    with contextlib.suppress(Exception):
                        cu = importlib.import_module("utils_pkg.core_utils")
                if cu is not None and hasattr(cu, "close_resources"):
                    await _call_maybe_async(cu.close_resources)
                    checks.append({"name": "close_resources", "ok": True})
        else:
            checks.append({"name": "close_resources", "ok": True, "detail": "skipped(--no-close)"})

    sec = time.monotonic() - t0

    warnings = _dedupe([_sanitize(w, secret_values=secret_values) for w in warnings], 12)
    fix_hints = _dedupe([_sanitize(h, secret_values=secret_values) for h in fix_hints], 12)

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
        "dryrun": _sanitize(os.getenv("DRYRUN", ""), secret_values=secret_values),
        "no_network_policy": int(_policy_no_network()),
        "probes_enabled": int(probes_on),
        "dotenv_loaded": int(not dotenv_disabled),
        "state": state_info,
    }

    return SmokeResult(
        ok=bool(ok),
        detail=detail,
        sec=float(sec),
        checks=checks,
        warnings=warnings,
        fix_hints=fix_hints,
        meta=meta,
    )


def cli(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding=stream.encoding or "utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass

    argv_list = list(argv if argv is not None else sys.argv[1:])
    _apply_cli_env_overrides(argv_list)

    try:
        args = _parse_args(argv_list)
    except Exception as e:
        json_only = _envb("SMOKE_INIT_JSON_ONLY", False) or _envb("PREFLIGHT_JSON_ONLY", False)
        emit_json = _envb("SMOKE_INIT_JSON", False) or _envb("PREFLIGHT_JSON", False) or json_only

        secret_values = _collect_secret_values()
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
            _safe_dump_json(payload)

        if not json_only:
            _safe_write_line(sys.stderr, "")
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
    )

    res = asyncio.run(_run(args))

    if emit_json:
        _safe_dump_json(_as_json(res))

    if not json_only:
        _hlog(
            "OK init_resources + cleanup succeeded" if res.ok else f"FAIL {res.detail}",
            json_only=False,
        )

    return RC_OK if res.ok else RC_FAIL


def main() -> None:
    try:
        rc = cli()
    except KeyboardInterrupt:
        rc = RC_CTRL_C
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
