# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/healthcheck.py — Canonical Healthcheck (Render-fast, deterministic)
==========================================================================

Goals
-----
- Deterministic & fast by default (Render-ready).
- No event-loop misuse.
- No hangs: sync fallback runs in a real subprocess with hard timeout.
- JSON output is clean & stable, especially with --json-only.
- Dotenv loading never uses find_dotenv() (avoids stdin/frame assertion edge cases).

Modes
-----
- local: calls utils_pkg.health.health_snapshot() (async preferred)
- http: calls an HTTP readiness endpoint (e.g., /readyz)
- auto: tries http quickly, then falls back to local (if allowed)

Exit codes
----------
0 = ready
1 = not ready
2 = misconfigured / internal error
130 = ctrl-c

Environment knobs (optional)
---------------------------
HEALTHCHECK_MODE=auto|http|local
HEALTHCHECK_URL=...
HEALTHCHECK_HTTP_PATH=/readyz
PORT=8080

HEALTHCHECK_RETRIES=2
HEALTHCHECK_RETRY_DELAY_S=0.15
HEALTHCHECK_HTTP_TIMEOUT_S=1.2
HEALTHCHECK_TIMEOUT_S=8.0

HEALTHCHECK_DEEP=0/1
HEALTHCHECK_JSON=0/1
HEALTHCHECK_JSON_ONLY=0/1

HEALTHCHECK_ALLOW_LOCAL_FALLBACK=0/1   (auto mode)
HEALTHCHECK_ALLOW_SYNC_FALLBACK=0/1    (local mode if only sync exists)
HEALTHCHECK_NO_DOTENV=0/1

SKIP_HEALTHCHECK=0/1 (and a few aliases)
"""

import argparse
import asyncio
import contextlib
import inspect
import json
import os
import re
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Final, Mapping

# ----------------------------
# Exit codes (contract)
# ----------------------------
RC_READY: Final[int] = 0
RC_NOT_READY: Final[int] = 1
RC_MISCONFIG: Final[int] = 2
RC_CTRL_C: Final[int] = 130


# ----------------------------
# Parsing helpers
# ----------------------------
_TRUE: Final[set[str]] = {"1", "true", "yes", "y", "on", "t"}
_FALSE: Final[set[str]] = {"0", "false", "no", "n", "off", "f"}

_URL_CRED_RE: Final[re.Pattern[str]] = re.compile(r"(://)([^/@\s]+)@")
_KV_SECRET_RE: Final[re.Pattern[str]] = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)


def _ts() -> str:
    # Stable ISO8601 UTC with Z, matches prior outputs
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now_ms() -> int:
    return int(time.monotonic() * 1000)


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


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
        return int(float(_envs(k, str(d))))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)))
    except Exception:
        return float(d)


def _mask_url(u: str) -> str:
    if not u:
        return ""
    out = _URL_CRED_RE.sub(r"\1***@", str(u))
    out = _KV_SECRET_RE.sub(r"\1\2***", out)
    if "?" in out:
        out = out.split("?", 1)[0] + "?..."
    return out


# ----------------------------
# Repo bootstrap + dotenv
# ----------------------------
def _bootstrap_repo_root() -> str:
    """
    Ensure repo root is on sys.path so `python -m scripts.healthcheck` works
    in Render and local shells even if CWD differs.
    """
    try:
        here = Path(__file__).resolve()
    except Exception:
        return ""

    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    for p in (here.parent, *here.parents):
        with contextlib.suppress(Exception):
            if any((p / m).exists() for m in markers) or (p / "src").is_dir():
                root = str(p)
                if root and root not in sys.path:
                    sys.path.insert(0, root)
                os.environ.setdefault("REPO_ROOT", root)
                return root

    root = str(here.parents[1] if len(here.parents) > 1 else here.parent)
    if root and root not in sys.path:
        sys.path.insert(0, root)
    os.environ.setdefault("REPO_ROOT", root)
    return root


def _maybe_load_dotenv() -> None:
    """
    Load dotenv ONLY from explicit paths to avoid python-dotenv find_dotenv()
    frame assertion edge-cases (notably when code is executed via stdin).
    """
    if _envb("HEALTHCHECK_NO_DOTENV", False):
        return

    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        repo = Path(_envs("REPO_ROOT", "")).resolve() if _envs("REPO_ROOT", "") else None
        if repo and repo.exists():
            for nm in (".env.extra", ".env"):
                p = repo / nm
                if p.exists():
                    load_dotenv(dotenv_path=str(p), override=False)


def _should_skip_entirely() -> tuple[bool, str]:
    for k in (
        "SKIP_PREFLIGHT_HEALTHCHECK",
        "PREFLIGHT_SKIP_HEALTHCHECK",
        "SKIP_HEALTHCHECK",
        "HEALTHCHECK_DISABLED",
    ):
        if _envb(k, False):
            return True, f"{k}=1"
    return False, ""


# ----------------------------
# URL + kwargs helpers
# ----------------------------
def _default_health_url() -> str:
    u = _envs("HEALTHCHECK_URL", "").strip()
    if u:
        return u
    port = _envi("PORT", 8080)
    path = _envs("HEALTHCHECK_HTTP_PATH", "/readyz").strip() or "/readyz"
    if not path.startswith("/"):
        path = "/" + path
    return f"http://127.0.0.1:{port}{path}"


def _filter_kwargs(fn: Any, kwargs: Mapping[str, Any]) -> dict[str, Any]:
    """
    Only pass kwargs the callee accepts, unless it has **kwargs.
    """
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return dict(kwargs)
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return dict(kwargs)


def _interpret_ready(payload: Any) -> tuple[bool, str]:
    """
    Accept common readiness payload styles.
    Tri-state aware: status=ok|degraded|fail.
    """
    if isinstance(payload, dict):
        ok = payload.get("ok")
        ready = payload.get("ready")
        status = payload.get("status")
        why = payload.get("why") or payload.get("reason") or payload.get("message") or ""

        if isinstance(status, str):
            s = status.strip().lower()
            if s in {"ok", "ready", "healthy"}:
                return True, str(why or s)
            if s in {"degraded", "fail", "error", "unhealthy"}:
                return False, str(why or s)

        if ok is True or ready is True:
            return True, str(why or "ok")
        if ok is False or ready is False:
            return False, str(why or "not_ready")

        return False, f"unrecognized_payload_keys={sorted(payload.keys())[:12]}"

    if isinstance(payload, str):
        s = payload.strip().lower()
        if s in {"ok", "ready"}:
            return True, "text_ok"
        return False, "text_not_ready"

    return False, "unknown_payload_type"


# ----------------------------
# HTTP probe
# ----------------------------
async def _http_get(url: str, timeout_s: float) -> tuple[int, Any, str]:
    """
    Fast readiness probe. Prefers httpx if installed; falls back to urllib in a thread.
    """
    url = str(url).strip()
    if not url:
        raise RuntimeError("empty url")
    timeout_s = max(0.2, float(timeout_s))

    # httpx fast path
    with contextlib.suppress(Exception):
        import httpx  # type: ignore

        async with httpx.AsyncClient(timeout=timeout_s) as c:
            r = await c.get(url, headers={"accept": "application/json"})
            ctype = (r.headers.get("content-type") or "").lower()
            if "application/json" in ctype:
                with contextlib.suppress(Exception):
                    return int(r.status_code), r.json(), ctype
            return int(r.status_code), r.text, ctype

    # urllib fallback (threaded)
    import urllib.error
    import urllib.request

    def _do() -> tuple[int, Any, str]:
        req = urllib.request.Request(url, headers={"accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                code = int(getattr(resp, "status", 200))
                ctype = (resp.headers.get("content-type") or "").lower()
                body = resp.read().decode("utf-8", "ignore")
        except urllib.error.HTTPError as he:
            code = int(getattr(he, "code", 0) or 0)
            hdrs = getattr(he, "headers", None)
            ctype = (hdrs.get("content-type") if hdrs else "") or ""
            ctype = str(ctype).lower()
            raw = he.read()
            body = (
                raw.decode("utf-8", "ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
            )

        if "application/json" in ctype:
            with contextlib.suppress(Exception):
                return code, json.loads(body), ctype
        return code, body, ctype

    return await asyncio.to_thread(_do)


# ----------------------------
# JSON-only safety
# ----------------------------
@contextlib.contextmanager
def _capture_stdio(enabled: bool):
    """
    When --json-only is used, prevent stray prints from breaking JSON output.
    """
    if not enabled:
        yield None
        return

    import io

    buf_out = io.StringIO()
    buf_err = io.StringIO()
    with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
        yield (buf_out, buf_err)


# ----------------------------
# Local snapshot (async preferred; sync subproc fallback)
# ----------------------------
def _sync_snapshot_subproc(
    *,
    deep: bool,
    hard_timeout_s: float,
    repo_root: str,
) -> dict[str, Any]:
    """
    Run health_snapshot_sync() in a REAL subprocess so we can hard-kill on timeout.
    This is Windows-safe (no multiprocessing pickling issues).
    """
    exe = sys.executable or "python"
    rr = str(repo_root or _envs("REPO_ROOT", ""))

    code = (
        "import json; "
        "from utils_pkg.health import health_snapshot_sync; "
        f"out = health_snapshot_sync(deep={int(bool(deep))}); "
        "print(json.dumps(out, ensure_ascii=False, default=str))"
    )

    env = os.environ.copy()
    if rr:
        env.setdefault("REPO_ROOT", rr)
        env["PYTHONPATH"] = rr + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

    try:
        cp = subprocess.run(
            [exe, "-c", code],
            cwd=rr or None,
            env=env,
            capture_output=True,
            text=True,
            timeout=max(0.2, float(hard_timeout_s)),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {
            "ts": _ts(),
            "status": "fail",
            "ok": False,
            "ms": 0,
            "checks": [],
            "err": f"sync_snapshot_timeout:>{hard_timeout_s:.2f}s",
        }
    except Exception as e:
        return {
            "ts": _ts(),
            "status": "fail",
            "ok": False,
            "ms": 0,
            "checks": [],
            "err": f"sync_snapshot_subproc_failed: {type(e).__name__}: {e}",
        }

    raw = (cp.stdout or "").strip() or (cp.stderr or "").strip()
    if not raw:
        return {
            "ts": _ts(),
            "status": "fail",
            "ok": False,
            "ms": 0,
            "checks": [],
            "err": f"sync_snapshot_empty_output: rc={cp.returncode}",
        }

    with contextlib.suppress(Exception):
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj

    return {
        "ts": _ts(),
        "status": "fail",
        "ok": False,
        "ms": 0,
        "checks": [],
        "err": "sync_snapshot_bad_payload",
        "raw": raw[:600],
        "rc": int(cp.returncode),
    }


async def _call_health_snapshot(*, deep: bool, timeout_s: float, json_only: bool) -> dict[str, Any]:
    """
    Calls utils_pkg.health.health_snapshot() safely from inside asyncio.

    Priority:
    1) async health_snapshot() (preferred)
    2) sync fallback in subprocess (optional, safe)
    """
    repo_root = _envs("REPO_ROOT", "")

    try:
        from utils_pkg import health as h  # type: ignore
    except Exception as e:
        return {
            "ts": _ts(),
            "status": "fail",
            "ok": False,
            "ms": 0,
            "checks": [],
            "err": f"import_health_failed: {type(e).__name__}: {e}",
        }

    timeout_s = max(0.2, float(timeout_s))
    hs = getattr(h, "health_snapshot", None)

    if callable(hs):
        kw = _filter_kwargs(hs, {"deep": deep})
        with _capture_stdio(enabled=json_only):
            try:
                return await asyncio.wait_for(hs(**kw), timeout=timeout_s)
            except asyncio.TimeoutError:
                return {
                    "ts": _ts(),
                    "status": "fail",
                    "ok": False,
                    "ms": 0,
                    "checks": [],
                    "err": f"async_snapshot_timeout:>{timeout_s:.2f}s",
                }
            except Exception as e:
                return {
                    "ts": _ts(),
                    "status": "fail",
                    "ok": False,
                    "ms": 0,
                    "checks": [],
                    "err": f"async_snapshot_failed: {type(e).__name__}: {e}",
                    "trace": traceback.format_exc()[:1200],
                }

    allow_sync = _envb("HEALTHCHECK_ALLOW_SYNC_FALLBACK", True)
    hsync = getattr(h, "health_snapshot_sync", None)
    if allow_sync and callable(hsync):
        hard = min(max(0.5, timeout_s + 0.75), 15.0)
        with _capture_stdio(enabled=json_only):
            return await asyncio.to_thread(
                _sync_snapshot_subproc,
                deep=deep,
                hard_timeout_s=float(hard),
                repo_root=str(repo_root),
            )

    return {
        "ts": _ts(),
        "status": "fail",
        "ok": False,
        "ms": 0,
        "checks": [],
        "err": "health_snapshot_missing",
    }


# ----------------------------
# Main logic
# ----------------------------
async def _amain(args: argparse.Namespace) -> int:
    t0 = _now_ms()

    json_only = bool(args.json_only or _envb("HEALTHCHECK_JSON_ONLY", False))
    # json-only implies emit json
    emit_json = bool(args.json or _envb("HEALTHCHECK_JSON", False) or json_only)

    skip, reason = _should_skip_entirely()
    if skip:
        payload = {
            "ts": _ts(),
            "status": "ok",
            "ok": True,
            "skipped": True,
            "reason": reason,
            "ms": _now_ms() - t0,
        }
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        else:
            print(f"READY (skipped: {reason})")
        return RC_READY

    mode = (args.mode or _envs("HEALTHCHECK_MODE", "auto")).strip().lower()
    if mode not in {"auto", "http", "local"}:
        payload = {
            "ts": _ts(),
            "status": "fail",
            "ok": False,
            "error": f"bad mode={mode!r}",
            "ms": _now_ms() - t0,
        }
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        else:
            print(f"MISCONFIG: {payload['error']}")
        return RC_MISCONFIG

    url = (args.url or _default_health_url()).strip()

    http_timeout_s = float(args.http_timeout or _envf("HEALTHCHECK_HTTP_TIMEOUT_S", 1.2))
    http_retries = int(args.retries or _envi("HEALTHCHECK_RETRIES", 2))
    http_retry_delay = float(args.retry_delay or _envf("HEALTHCHECK_RETRY_DELAY_S", 0.15))

    deep = bool(args.deep or _envb("HEALTHCHECK_DEEP", False))
    snap_timeout_s = float(args.timeout or _envf("HEALTHCHECK_TIMEOUT_S", 8.0))

    payload: dict[str, Any] = {
        "ts": _ts(),
        "status": "fail",
        "ok": False,
        "mode": mode,
        "http": None,
        "snapshot": None,
        "ms": 0,
    }

    async def _do_local() -> bool:
        snap = await _call_health_snapshot(deep=deep, timeout_s=snap_timeout_s, json_only=json_only)
        payload["snapshot"] = snap
        ok_local = bool(isinstance(snap, dict) and snap.get("ok") is True)
        st = str(snap.get("status", "")).lower() if isinstance(snap, dict) else ""
        if st in {"ok", "degraded", "fail"}:
            payload["status"] = st
        return ok_local

    # HTTP-first for auto/http
    if mode in {"auto", "http"}:
        last_http: dict[str, Any] = {}
        ok_http = False

        for i in range(max(1, http_retries)):
            try:
                code, data, ctype = await _http_get(url, timeout_s=max(0.2, http_timeout_s))
                ready, why = _interpret_ready(data)
                last_http = {
                    "url": _mask_url(url),
                    "attempt": i + 1,
                    "status": int(code),
                    "content_type": str(ctype),
                    "ready": bool(ready),
                    "why": str(why),
                    "ok": bool(int(code) == 200 and ready),
                }
                if last_http["ok"]:
                    ok_http = True
                    break
            except Exception as e:
                last_http = {
                    "url": _mask_url(url),
                    "attempt": i + 1,
                    "ok": False,
                    "error": f"{type(e).__name__}: {e}",
                }

            if i + 1 < max(1, http_retries):
                await asyncio.sleep(max(0.01, http_retry_delay))

        payload["http"] = last_http or {
            "url": _mask_url(url),
            "ok": False,
            "error": "no_http_attempts",
        }

        if ok_http:
            payload["ok"] = True
            payload["status"] = "ok"
            payload["ms"] = _now_ms() - t0
            if emit_json:
                print(json.dumps(payload, ensure_ascii=False, default=str))
            else:
                print("READY (http)")
            return RC_READY

        if mode == "http":
            payload["ok"] = False
            payload["status"] = "fail"
            payload["ms"] = _now_ms() - t0
            if emit_json:
                print(json.dumps(payload, ensure_ascii=False, default=str))
            else:
                print("NOT READY (http)")
            return RC_NOT_READY

        allow_fallback = _envb("HEALTHCHECK_ALLOW_LOCAL_FALLBACK", True)
        if not allow_fallback:
            payload["ok"] = False
            payload["status"] = "fail"
            payload["ms"] = _now_ms() - t0
            if emit_json:
                print(json.dumps(payload, ensure_ascii=False, default=str))
            else:
                print("NOT READY (auto; fallback disabled)")
            return RC_NOT_READY

    ok_local = await _do_local()
    payload["ok"] = bool(ok_local)
    if payload.get("status") not in {"ok", "degraded", "fail"}:
        payload["status"] = "ok" if ok_local else "fail"
    payload["ms"] = _now_ms() - t0

    if emit_json:
        print(json.dumps(payload, ensure_ascii=False, default=str))
    else:
        print("READY (local)" if ok_local else "NOT READY (local)")

    return RC_READY if ok_local else RC_NOT_READY


def main() -> None:
    ap = argparse.ArgumentParser(description="Canonical healthcheck runner (auto/http/local).")
    ap.add_argument("--mode", type=str, default="", help="auto|http|local (default: env or auto)")
    ap.add_argument(
        "--url",
        type=str,
        default="",
        help="HTTP URL (default: HEALTHCHECK_URL or http://127.0.0.1:${PORT}/readyz)",
    )
    ap.add_argument("--retries", type=int, default=0, help="HTTP retries (default: env)")
    ap.add_argument("--retry-delay", type=float, default=0.0, help="HTTP retry delay seconds")
    ap.add_argument("--http-timeout", type=float, default=0.0, help="HTTP timeout seconds")
    ap.add_argument("--timeout", type=float, default=0.0, help="Local snapshot timeout seconds")
    ap.add_argument("--deep", action="store_true", help="Deep snapshot (if supported)")
    ap.add_argument("--json", action="store_true", help="Emit JSON to STDOUT")
    ap.add_argument("--json-only", action="store_true", help="Suppress non-JSON output")

    args = ap.parse_args()

    with contextlib.suppress(Exception):
        _bootstrap_repo_root()
    with contextlib.suppress(Exception):
        _maybe_load_dotenv()

    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]

    try:
        rc = asyncio.run(_amain(args))
    except KeyboardInterrupt:
        rc = RC_CTRL_C
    except Exception:
        err = traceback.format_exc()
        wants_json = bool(
            args.json
            or args.json_only
            or _envb("HEALTHCHECK_JSON", False)
            or _envb("HEALTHCHECK_JSON_ONLY", False)
        )
        if wants_json:
            print(
                json.dumps(
                    {"ts": _ts(), "status": "fail", "ok": False, "error": err[:1200]},
                    ensure_ascii=False,
                )
            )
        else:
            print(err)
        rc = RC_MISCONFIG

    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
