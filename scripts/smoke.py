# scripts/smoke.py
from __future__ import annotations

"""
scripts/smoke.py — Canonical Smoke “GO LIVE” Guard (P0[11] LOCK-IN)
===================================================================

What this is
------------
A deterministic, secret-safe, READ-ONLY smoke runner that validates the end-to-end
*profit pipe prerequisites* without placing trades.

It is designed to be the final guardrail before going live on Render:
- Validates env contract (via scripts.check_env) [optional but recommended]
- Validates Solana RPC viability (via scripts.check_rpc) [recommended]
- Validates core resource init (aiohttp/redis clients etc, via utils_pkg.core_utils)
- Validates price oracle path (WSOL + USDC token prices)
- Validates quote/route path (WSOL -> USDC Jupiter quote, route presence optional)
- Optional: validates Postgres schema init (idempotent ensure_schema; opt-in)
- Optional: validates Redis ping (opt-in)

Safety / non-trading
--------------------
- NO swaps, NO buys, NO sells. Only queries.
- Safety latch: requires ALLOW_SMOKE_SCRIPT=1 (or ALLOW_DRY_RUN_SCRIPT=1) OR --yes-i-am-sure.

Exit codes
----------
0  OK
2  One or more required checks failed
130 Ctrl-C

Typical usage
-------------
# Local (safe):
set ALLOW_SMOKE_SCRIPT=1
py -m scripts.smoke --require-quote --rpc-try-all --pretty

# Render one-off:
ALLOW_SMOKE_SCRIPT=1 python -m scripts.smoke --require-quote --require-pg --rpc-strict --rpc-try-all

Notes
-----
- This script tries hard to be robust even when some modules are missing.
- It prefers to *call the canonical scripts* (check_env/check_rpc) via -m so
  you don’t get divergent logic.
"""

import argparse
import asyncio
import contextlib
import json
import os
import platform
import re
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------------------
# Repo bootstrap (consistent imports regardless of CWD; matches check_rpc pattern)
# --------------------------------------------------------------------------------------


def _bootstrap_repo_root() -> Path:
    try:
        here = Path(__file__).resolve()
    except Exception:
        here = Path.cwd()

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    candidates = [here.parent, *list(here.parents)]
    root: Path | None = None

    for p in candidates:
        try:
            if any((p / m).exists() for m in markers):
                root = p
                break
            if (p / "src").is_dir():
                root = p
                break
        except Exception:
            continue

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    os.environ.setdefault("REPO_ROOT", str(root))

    rs = str(root)
    if rs and rs not in sys.path:
        sys.path.insert(0, rs)

    src = root / "src"
    if src.is_dir():
        ss = str(src)
        if ss not in sys.path:
            sys.path.insert(0, ss)

    return root


REPO_ROOT = _bootstrap_repo_root()

# --------------------------------------------------------------------------------------
# Optional dotenv load (Render: /etc/secrets/.env.extra)
# --------------------------------------------------------------------------------------
try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore


def _load_env_best_effort() -> None:
    if load_dotenv is None:
        return

    # Render pattern
    for p in (Path("/etc/secrets/.env.extra"), Path("/etc/secrets/.env")):
        if p.exists():
            with contextlib.suppress(Exception):
                load_dotenv(str(p), override=False)

    # Repo pattern
    for name in (".env.extra", ".env"):
        p = REPO_ROOT / name
        if p.exists():
            with contextlib.suppress(Exception):
                load_dotenv(str(p), override=False)

    # Fallback to system env discovery if supported by python-dotenv
    with contextlib.suppress(Exception):
        load_dotenv(override=False)


# --------------------------------------------------------------------------------------
# logging (canonical root logging_setup.py if present, otherwise stderr)
# --------------------------------------------------------------------------------------
def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_stderr(line: str) -> None:
    try:
        sys.stderr.write(line + ("" if line.endswith("\n") else "\n"))
        sys.stderr.flush()
    except Exception:
        pass


def _get_logger():
    with contextlib.suppress(Exception):
        from logging_setup import get_logger  # type: ignore

        return get_logger()
    return None


_LOG = _get_logger()


def _log(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    if _LOG is not None:
        with contextlib.suppress(Exception):
            _LOG.info(msg)  # type: ignore[attr-defined]
            return
    _safe_stderr(f"[smoke] {_ts()} {msg}")


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    s = _envs(k, "")
    if s == "":
        return bool(d)
    x = s.lower().strip()
    if x in _TRUE:
        return True
    if x in _FALSE:
        return False
    return bool(d)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _hosted_env_guess() -> bool:
    # Render sets one or more of these
    return bool(
        os.getenv("RENDER") or os.getenv("RENDER_SERVICE_ID") or os.getenv("RENDER_EXTERNAL_URL")
    )


def _must_allow(bypass: bool) -> None:
    """
    Guard against accidental runs.
    Smoke does not trade, but we keep a safety latch anyway.
    """
    allowed = (_envs("ALLOW_SMOKE_SCRIPT", "") == "1") or (_envs("ALLOW_DRY_RUN_SCRIPT", "") == "1")
    if not (allowed or bypass):
        raise SystemExit(
            "Safety latch: set ALLOW_SMOKE_SCRIPT=1 (or ALLOW_DRY_RUN_SCRIPT=1) "
            "or pass --yes-i-am-sure"
        )


def _mask(s: str) -> str:
    # conservative long-token masker (won’t catch everything; prevents obvious leaks)
    if not s:
        return s
    return re.sub(r"\b[A-Za-z0-9_\-]{28,}\b", "***", s)


def _extract_last_json(stdout_text: str) -> tuple[dict[str, Any] | None, str]:
    """
    Try to parse the *last* JSON object from stdout (since scripts may log before JSON).
    """
    t = (stdout_text or "").strip()
    if not t:
        return None, "empty_stdout"

    # Fast path: whole stdout is JSON
    with contextlib.suppress(Exception):
        obj = json.loads(t)
        if isinstance(obj, dict):
            return obj, ""

    # Otherwise scan from the end for a '{' that begins a dict JSON
    for i in range(len(t) - 1, -1, -1):
        if t[i] != "{":
            continue
        chunk = t[i:]
        with contextlib.suppress(Exception):
            obj2 = json.loads(chunk)
            if isinstance(obj2, dict):
                return obj2, ""
    return None, "no_json_object_found"


@dataclass
class CheckResult:
    name: str
    ok: bool
    ms: int
    severity: str  # "error" or "warn" or "info"
    detail: str
    data: dict[str, Any] | None = None

    def public(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ok": int(bool(self.ok)),
            "severity": self.severity,
            "ms": int(self.ms),
            "detail": self.detail,
            "data": self.data,
        }


# --------------------------------------------------------------------------------------
# Canonical mints (MAINNET)
# --------------------------------------------------------------------------------------
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


# --------------------------------------------------------------------------------------
# core_utils imports (authoritative)
# --------------------------------------------------------------------------------------
_CORE_IMPORT_ERR: str | None = None
try:
    from utils_pkg.core_utils import (  # type: ignore
        close_resources,
        get_jupiter_quote,
        get_token_price,
        init_resources,
    )
except Exception as e:  # pragma: no cover
    init_resources = None  # type: ignore
    close_resources = None  # type: ignore
    get_token_price = None  # type: ignore
    get_jupiter_quote = None  # type: ignore
    _CORE_IMPORT_ERR = repr(e)


async def _force_close_sessions_best_effort() -> None:
    # 1) ask core_utils to close
    if close_resources is not None:
        with contextlib.suppress(Exception):
            await close_resources()

    # 2) try closing shared session if core_utils exposes it
    with contextlib.suppress(Exception):
        from utils_pkg import core_utils as cu  # type: ignore

        get_sess = getattr(cu, "get_session", None)
        if callable(get_sess):
            sess = get_sess()
            if sess is not None and getattr(sess, "closed", True) is False:
                with contextlib.suppress(Exception):
                    await sess.close()

    # 3) give event loop a chance to flush closes
    await asyncio.sleep(0)


# --------------------------------------------------------------------------------------
# subprocess module runner (calls canonical scripts via "-m")
# --------------------------------------------------------------------------------------
async def _run_module_json(
    module: str,
    args: Sequence[str],
    *,
    timeout_s: float,
    env_extra: dict[str, str] | None = None,
) -> tuple[dict[str, Any] | None, str, str, int, int]:
    """
    Returns: (json_obj_or_none, stdout, stderr, rc, elapsed_ms)
    """
    t0 = time.time()
    py = sys.executable or "python"
    cmd = [py, "-m", module, *list(args)]
    env = os.environ.copy()
    if env_extra:
        env.update({str(k): str(v) for k, v in env_extra.items()})

    # ensure stable encoding
    env.setdefault("PYTHONIOENCODING", "utf-8")

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
    except Exception as e:
        return None, "", repr(e), 1, int((time.time() - t0) * 1000)

    try:
        out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=timeout_s)
    except TimeoutError:
        with contextlib.suppress(Exception):
            proc.kill()
        return None, "", f"timeout after {timeout_s:.1f}s", 124, int((time.time() - t0) * 1000)

    rc = int(proc.returncode or 0)
    stdout = (out_b or b"").decode("utf-8", "ignore")
    stderr = (err_b or b"").decode("utf-8", "ignore")

    obj, perr = _extract_last_json(stdout)
    if obj is None and perr and stderr:
        # sometimes JSON is on stderr in misconfigured envs
        obj2, perr2 = _extract_last_json(stderr)
        if obj2 is not None:
            obj, perr = obj2, ""

    elapsed_ms = int((time.time() - t0) * 1000)
    return obj, stdout, stderr, rc, elapsed_ms


# --------------------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------------------
async def _check_core_imports() -> CheckResult:
    t0 = time.time()
    if _CORE_IMPORT_ERR is not None:
        return CheckResult(
            name="core_imports",
            ok=False,
            ms=int((time.time() - t0) * 1000),
            severity="error",
            detail=f"utils_pkg.core_utils import failed: {_CORE_IMPORT_ERR}",
            data=None,
        )
    return CheckResult(
        name="core_imports",
        ok=True,
        ms=int((time.time() - t0) * 1000),
        severity="info",
        detail="ok",
        data=None,
    )


async def _check_init_resources(required: bool) -> CheckResult:
    t0 = time.time()
    if init_resources is None:
        return CheckResult(
            name="init_resources",
            ok=not required,
            ms=int((time.time() - t0) * 1000),
            severity="error" if required else "warn",
            detail="core_utils.init_resources missing",
            data=None,
        )
    try:
        await init_resources()
        return CheckResult(
            "init_resources", True, int((time.time() - t0) * 1000), "info", "ok", None
        )
    except Exception as e:
        return CheckResult(
            name="init_resources",
            ok=not required,
            ms=int((time.time() - t0) * 1000),
            severity="error" if required else "warn",
            detail=_mask(repr(e)),
            data=None,
        )


async def _check_token_prices(required: bool) -> CheckResult:
    t0 = time.time()
    if get_token_price is None:
        return CheckResult(
            name="token_prices",
            ok=not required,
            ms=int((time.time() - t0) * 1000),
            severity="error" if required else "warn",
            detail="core_utils.get_token_price missing",
            data=None,
        )

    wsol = 0.0
    usdc = 0.0
    err: str | None = None

    try:
        wsol = float(await get_token_price(WSOL_MINT) or 0.0)
    except Exception as e:
        err = f"wsol_price_err={_mask(repr(e))}"

    try:
        usdc = float(await get_token_price(USDC_MINT) or 0.0)
    except Exception as e:
        err = (err + "; " if err else "") + f"usdc_price_err={_mask(repr(e))}"

    ok = (wsol > 0.0) and (usdc > 0.0)
    detail = "ok" if ok else (err or "invalid price(s)")
    return CheckResult(
        name="token_prices",
        ok=ok if required else (err is None),
        ms=int((time.time() - t0) * 1000),
        severity="error" if (required and not ok) else ("warn" if not ok else "info"),
        detail=detail,
        data={
            "wsol_price": wsol,
            "usdc_price": usdc,
            "wsol_mint": WSOL_MINT,
            "usdc_mint": USDC_MINT,
        },
    )


async def _check_jupiter_quote(
    require_route: bool, slip_frac: float, sol_amount: float
) -> CheckResult:
    t0 = time.time()
    if get_jupiter_quote is None:
        return CheckResult(
            name="jupiter_quote",
            ok=not require_route,
            ms=int((time.time() - t0) * 1000),
            severity="error" if require_route else "warn",
            detail="core_utils.get_jupiter_quote missing",
            data=None,
        )

    lamports = int(max(0, round(float(sol_amount) * 1e9)))
    err: str | None = None
    has_route = False
    keys: list[str] = []

    try:
        # signature may vary; try common forms
        try:
            q = await get_jupiter_quote(WSOL_MINT, USDC_MINT, lamports, float(slip_frac))
        except TypeError:
            q = await get_jupiter_quote(WSOL_MINT, USDC_MINT, lamports)

        if isinstance(q, dict):
            keys = list(q.keys())

            rp = q.get("routePlan")
            if isinstance(rp, list) and len(rp) > 0:
                has_route = True

            data = q.get("data")
            if isinstance(data, list) and len(data) > 0:
                has_route = True

            if q.get("outAmount"):
                has_route = True
        else:
            err = f"unexpected_quote_type={type(q)}"
    except Exception as e:
        err = _mask(repr(e))

    ok = has_route if require_route else (err is None)
    detail = "ok" if ok else ("no_route" if err is None else err)
    sev = "error" if (require_route and not has_route) else ("warn" if not ok else "info")

    return CheckResult(
        name="jupiter_quote",
        ok=bool(ok),
        ms=int((time.time() - t0) * 1000),
        severity=sev,
        detail=detail,
        data={
            "require_route": int(bool(require_route)),
            "slip_frac": float(slip_frac),
            "sol_amount": float(sol_amount),
            "lamports_in": int(lamports),
            "has_route": int(bool(has_route)),
            "quote_keys": keys[:50],
        },
    )


async def _check_postgres(require_pg: bool) -> CheckResult:
    """
    Optional PG check. This can be idempotent schema ensure (your ai_db should be safe).
    """
    t0 = time.time()
    try:
        from utils_pkg.ai_db import ensure_schema  # type: ignore
    except Exception as e:
        return CheckResult(
            name="postgres_schema",
            ok=not require_pg,
            ms=int((time.time() - t0) * 1000),
            severity="error" if require_pg else "warn",
            detail=_mask(f"ensure_schema missing: {e!r}"),
            data=None,
        )

    try:
        await ensure_schema()
        return CheckResult(
            "postgres_schema", True, int((time.time() - t0) * 1000), "info", "ok", None
        )
    except Exception as e:
        return CheckResult(
            name="postgres_schema",
            ok=not require_pg,
            ms=int((time.time() - t0) * 1000),
            severity="error" if require_pg else "warn",
            detail=_mask(repr(e)),
            data=None,
        )


async def _check_redis(require_redis: bool) -> CheckResult:
    t0 = time.time()
    try:
        from utils_pkg import core_utils as cu  # type: ignore
    except Exception as e:
        return CheckResult(
            name="redis_ping",
            ok=not require_redis,
            ms=int((time.time() - t0) * 1000),
            severity="error" if require_redis else "warn",
            detail=_mask(f"core_utils import failed: {e!r}"),
            data=None,
        )

    get_redis = getattr(cu, "get_redis_async", None)
    if not callable(get_redis):
        return CheckResult(
            name="redis_ping",
            ok=not require_redis,
            ms=int((time.time() - t0) * 1000),
            severity="error" if require_redis else "warn",
            detail="core_utils.get_redis_async missing",
            data=None,
        )

    try:
        r = await get_redis()
        pong = None
        if r is not None and hasattr(r, "ping"):
            pong = await r.ping()
        ok = bool(pong is True or pong == "PONG" or pong == b"PONG")
        return CheckResult(
            name="redis_ping",
            ok=ok if require_redis else True,
            ms=int((time.time() - t0) * 1000),
            severity="error" if (require_redis and not ok) else ("warn" if not ok else "info"),
            detail="ok" if ok else "ping_failed",
            data={"pong": str(pong)[:50] if pong is not None else None},
        )
    except Exception as e:
        return CheckResult(
            name="redis_ping",
            ok=not require_redis,
            ms=int((time.time() - t0) * 1000),
            severity="error" if require_redis else "warn",
            detail=_mask(repr(e)),
            data=None,
        )


async def _check_script_check_env(enabled: bool, timeout_s: float) -> CheckResult:
    t0 = time.time()
    if not enabled:
        return CheckResult(
            "check_env", True, int((time.time() - t0) * 1000), "info", "skipped", None
        )

    # try “json-only/force” first; fallback if flags mismatch
    for argv in (["--json-only", "--force"], ["--json-only"], ["--force"], []):
        obj, out, err, rc, ms = await _run_module_json(
            "scripts.check_env",
            argv,
            timeout_s=timeout_s,
            env_extra={"ALLOW_DRY_RUN_SCRIPT": "1", "ALLOW_SMOKE_SCRIPT": "1"},
        )
        if obj is not None:
            ok = bool(obj.get("ok", rc == 0))
            detail = "ok" if ok else _mask(str(obj.get("error") or obj.get("detail") or f"rc={rc}"))
            sev = "error" if not ok else "info"
            return CheckResult(
                name="check_env",
                ok=ok,
                ms=ms,
                severity=sev,
                detail=detail,
                data={"rc": rc, "summary": obj},
            )
        # if script exists but didn’t emit JSON, keep trying flags; else break on hard import errors
        if "No module named" in (err or ""):
            break

    # no JSON extracted
    msg = "no_json_from_check_env"
    return CheckResult(
        name="check_env",
        ok=False,
        ms=int((time.time() - t0) * 1000),
        severity="error",
        detail=_mask(msg),
        data=None,
    )


async def _check_script_check_rpc(
    enabled: bool,
    *,
    strict: bool,
    progress: bool,
    try_all: bool,
    timeout_s: float,
) -> CheckResult:
    t0 = time.time()
    if not enabled:
        return CheckResult(
            "check_rpc", True, int((time.time() - t0) * 1000), "info", "skipped", None
        )

    argv: list[str] = ["--json-only"]
    if strict:
        argv.append("--strict")
    if progress:
        argv.append("--progress")
    if try_all:
        argv.append("--try-all")

    obj, out, err, rc, ms = await _run_module_json(
        "scripts.check_rpc",
        argv,
        timeout_s=timeout_s,
        env_extra={"ALLOW_DRY_RUN_SCRIPT": "1", "ALLOW_SMOKE_SCRIPT": "1"},
    )

    if obj is None:
        return CheckResult(
            name="check_rpc",
            ok=False,
            ms=ms,
            severity="error",
            detail=_mask(f"no_json_from_check_rpc rc={rc} err={err.strip()[:200] if err else ''}"),
            data={"rc": rc, "stderr": _mask((err or "")[:500])},
        )

    ok = bool(obj.get("ok", rc == 0))
    picked = obj.get("picked_url") or obj.get("picked") or None
    detail = "ok" if ok else _mask(str(obj.get("error") or obj.get("detail") or f"rc={rc}"))
    if picked and ok:
        detail = f"ok (picked={picked})"

    sev = "error" if not ok else "info"
    data = {
        "rc": rc,
        "picked_url": obj.get("picked_url"),
        "recommended_SOL_RPC_URLS_order": obj.get("recommended_SOL_RPC_URLS_order"),
        "fix_hints": obj.get("fix_hints"),
    }
    return CheckResult("check_rpc", ok, ms, sev, detail, data=data)


# --------------------------------------------------------------------------------------
# Orchestrator
# --------------------------------------------------------------------------------------
async def run_smoke(
    *,
    json_only: bool,
    env_check: bool,
    rpc_check: bool,
    rpc_strict: bool,
    rpc_progress: bool,
    rpc_try_all: bool,
    require_quote_route: bool,
    require_pg: bool,
    require_redis: bool,
    slip_frac: float,
    sol_amount: float,
    script_timeout_s: float,
) -> dict[str, Any]:
    started = time.time()
    checks: list[CheckResult] = []

    # Meta / guard notes
    notes: list[str] = []
    dry_run = _envs("DRY_RUN", "")
    if str(dry_run).strip() not in ("1", "true", "True"):
        notes.append(
            "Recommended: set DRY_RUN=1 for go-live validation runs (smoke itself does not trade)."
        )

    kill = _envb("KILL_SWITCH", False)
    dis_scanners = _envb("DISABLE_SCANNERS", False)
    if kill:
        notes.append("KILL_SWITCH=1 is set (smoke continues; trading should be blocked).")
    if dis_scanners:
        notes.append("DISABLE_SCANNERS=1 is set (smoke continues; scanners likely disabled).")

    # 0) core import sanity
    checks.append(await _check_core_imports())

    # 1) env contract (canonical script)
    checks.append(await _check_script_check_env(enabled=env_check, timeout_s=script_timeout_s))

    # 2) rpc viability (canonical script)
    checks.append(
        await _check_script_check_rpc(
            enabled=rpc_check,
            strict=rpc_strict,
            progress=rpc_progress,
            try_all=rpc_try_all,
            timeout_s=script_timeout_s,
        )
    )

    # 3) init resources (aiohttp clients etc)
    checks.append(await _check_init_resources(required=True))

    # 4) token prices (WSOL + USDC)
    checks.append(await _check_token_prices(required=True))

    # 5) quote path (WSOL -> USDC)
    checks.append(
        await _check_jupiter_quote(
            require_route=require_quote_route, slip_frac=slip_frac, sol_amount=sol_amount
        )
    )

    # 6) postgres schema (optional)
    checks.append(await _check_postgres(require_pg=require_pg))

    # 7) redis ping (optional)
    checks.append(await _check_redis(require_redis=require_redis))

    # Compute overall OK: any "error" severity with ok=False fails
    hard_fail = any((c.severity == "error" and not c.ok) for c in checks)
    ok = not hard_fail

    out: dict[str, Any] = {
        "ts": _ts(),
        "ok": bool(ok),
        "started_at": _now_iso(),
        "finished_at": None,
        "elapsed_ms": None,
        "meta": {
            "repo_root": str(REPO_ROOT),
            "hosted_env": int(bool(_hosted_env_guess())),
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "dry_run_env": dry_run,
            "kill_switch": int(bool(kill)),
            "disable_scanners": int(bool(dis_scanners)),
        },
        "checks": [c.public() for c in checks],
        "notes": notes,
    }

    out["finished_at"] = _now_iso()
    out["elapsed_ms"] = int((time.time() - started) * 1000)
    return out


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Canonical smoke test (P0 go-live guard)")
    ap.add_argument("--yes-i-am-sure", action="store_true", help="Bypass safety latch.")
    ap.add_argument("--force", action="store_true", help="Alias for --yes-i-am-sure (compat).")

    ap.add_argument("--json-only", action="store_true", help="No human logs; JSON output only.")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print JSON (indent).")

    ap.add_argument("--no-env-check", action="store_true", help="Skip scripts.check_env.")
    ap.add_argument("--no-rpc-check", action="store_true", help="Skip scripts.check_rpc.")

    ap.add_argument("--rpc-strict", action="store_true", help="Pass --strict to scripts.check_rpc.")
    ap.add_argument(
        "--rpc-progress", action="store_true", help="Pass --progress to scripts.check_rpc."
    )
    ap.add_argument(
        "--rpc-try-all",
        action="store_true",
        help="Pass --try-all to scripts.check_rpc (recommended).",
    )

    ap.add_argument(
        "--require-quote", action="store_true", help="Fail if Jupiter route cannot be found."
    )
    ap.add_argument("--require-pg", action="store_true", help="Fail if Postgres schema init fails.")
    ap.add_argument("--require-redis", action="store_true", help="Fail if Redis ping fails.")

    ap.add_argument(
        "--slip",
        type=float,
        default=0.01,
        help="Slippage fraction used for quote check (default 1%).",
    )
    ap.add_argument(
        "--sol", type=float, default=0.01, help="SOL amount used for quote check (default 0.01)."
    )
    ap.add_argument(
        "--script-timeout",
        type=float,
        default=40.0,
        help="Timeout seconds for check_env/check_rpc subprocess.",
    )
    return ap


async def _amain(ns: argparse.Namespace) -> int:
    bypass = bool(ns.yes_i_am_sure or ns.force)
    _must_allow(bypass)

    json_only = bool(ns.json_only)

    res = await run_smoke(
        json_only=json_only,
        env_check=not bool(ns.no_env_check),
        rpc_check=not bool(ns.no_rpc_check),
        rpc_strict=bool(ns.rpc_strict),
        rpc_progress=bool(ns.rpc_progress),
        rpc_try_all=bool(ns.rpc_try_all),
        require_quote_route=bool(ns.require_quote),
        require_pg=bool(ns.require_pg),
        require_redis=bool(ns.require_redis),
        slip_frac=float(ns.slip),
        sol_amount=float(ns.sol),
        script_timeout_s=max(8.0, float(ns.script_timeout)),
    )

    # Output (always JSON; canonical)
    if ns.pretty:
        print(json.dumps(res, ensure_ascii=False, indent=2, sort_keys=False, default=str))
    else:
        print(json.dumps(res, ensure_ascii=False, separators=(",", ":"), default=str))

    # Clean up sessions
    await _force_close_sessions_best_effort()
    return 0 if res.get("ok") else 2


def main() -> None:
    _load_env_best_effort()

    # Windows-friendly event loop policy
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]

    ap = build_arg_parser()
    ns = ap.parse_args()

    try:
        rc = asyncio.run(_amain(ns))
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _safe_stderr(f"[smoke] {_ts()} FAIL unhandled {type(e).__name__}: {_mask(repr(e))}")
        rc = 2

    raise SystemExit(rc)


if __name__ == "__main__":
    main()
