# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/healthcheck.py — Canonical Healthcheck (Render-fast, deterministic)
==========================================================================

Goals
-----
- Deterministic & fast by default (Render-ready).
- No event-loop misuse (never call a sync snapshot from inside a running loop).
- No hangs: sync fallback runs in a subprocess with hard kill.
- JSON output is clean & stable, especially with --json-only.

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
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Final

RC_READY: Final[int] = 0
RC_NOT_READY: Final[int] = 1
RC_MISCONFIG: Final[int] = 2
RC_CTRL_C: Final[int] = 130

_TRUE: Final[set[str]] = {"1", "true", "yes", "y", "on", "t"}
_FALSE: Final[set[str]] = {"0", "false", "no", "n", "off", "f"}

_URL_CRED_RE: Final[re.Pattern[str]] = re.compile(r"(://)([^/@\s]+)@")
_KV_SECRET_RE: Final[re.Pattern[str]] = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)


def _ts() -> str:
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


def _mask_url(u: str) -> str:
    if not u:
        return ""
    out = _URL_CRED_RE.sub(r"\1***@", str(u))
    out = _KV_SECRET_RE.sub(r"\1\2***", out)
    if "?" in out:
        out = out.split("?", 1)[0] + "?..."
    return out


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
    if _envb("HEALTHCHECK_NO_DOTENV", False):
        return
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        repo = _envs("REPO_ROOT", "")
        if repo:
            for nm in (".env.extra", ".env"):
                p = Path(repo) / nm
                if p.exists():
                    load_dotenv(str(p), override=False)
        load_dotenv(override=False)


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


def _default_health_url() -> str:
    u = _envs("HEALTHCHECK_URL", "").strip()
    if u:
        return u
    port = _envi("PORT", 8080)
    path = _envs("HEALTHCHECK_HTTP_PATH", "/readyz").strip() or "/readyz"
    if not path.startswith("/"):
        path = "/" + path
    return f"http://127.0.0.1:{port}{path}"


def _filter_kwargs(fn: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Only pass kwargs the callee accepts, unless it has **kwargs.
    """
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params):
            return kwargs
        allowed = {p.name for p in params}
        return {k: v for k, v in kwargs.items() if k in allowed}
    except Exception:
        return kwargs


def _interpret_ready(payload: Any) -> tuple[bool, str]:
    """
    Accept a few common readiness payload styles.
    """
    if isinstance(payload, dict):
        ok = payload.get("ok")
        ready = payload.get("ready")
        status = payload.get("status")
        why = payload.get("why") or payload.get("reason") or payload.get("message") or ""
        if (
            ok is True
            or ready is True
            or (isinstance(status, str) and status.lower() in {"ok", "ready", "healthy"})
        ):
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


async def _http_get(url: str, timeout_s: float) -> tuple[int, Any, str]:
    """
    Fast readiness probe. Prefers httpx if installed; falls back to urllib in a thread.
    Never raises to caller (exceptions handled by caller).
    """
    url = str(url).strip()
    if not url:
        raise RuntimeError("empty url")

    # Prefer httpx if present
    with contextlib.suppress(Exception):
        import httpx  # type: ignore

        async with httpx.AsyncClient(timeout=max(0.2, float(timeout_s))) as c:
            r = await c.get(url, headers={"accept": "application/json"})
            ctype = (r.headers.get("content-type") or "").lower()
            if "application/json" in ctype:
                with contextlib.suppress(Exception):
                    return int(r.status_code), r.json(), ctype
                # If JSON parse fails, return text.
            return int(r.status_code), r.text, ctype

    # Fallback: urllib in a thread
    import urllib.request

    def _do() -> tuple[int, Any, str]:
        req = urllib.request.Request(url, headers={"accept": "application/json"})
        with urllib.request.urlopen(req, timeout=max(0.2, float(timeout_s))) as resp:
            code = int(getattr(resp, "status", 200))
            ctype = (resp.headers.get("content-type") or "").lower()
            body = resp.read().decode("utf-8", "ignore")
            if "application/json" in ctype:
                with contextlib.suppress(Exception):
                    return code, json.loads(body), ctype
            return code, body, ctype

    return await asyncio.to_thread(_do)


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


def _sync_snapshot_subproc(
    *,
    deep: bool,
    timeout_s: float,
    hard_timeout_s: float,
    repo_root: str,
) -> dict[str, Any]:
    """
    Run health_snapshot_sync() in a subprocess so we can hard-kill on timeout.
    This avoids the classic hang where a thread blocks and prevents process exit.
    """
    import multiprocessing as mp

    ctx = mp.get_context("spawn")
    q: Any = ctx.Queue(maxsize=1)

    def _child(q_: Any, deep_: bool, timeout_s_: float, repo_root_: str) -> None:
        try:
            if repo_root_:
                os.environ.setdefault("REPO_ROOT", repo_root_)
                if repo_root_ not in sys.path:
                    sys.path.insert(0, repo_root_)

            from utils_pkg import health as h  # type: ignore

            fn = getattr(h, "health_snapshot_sync", None)
            if not callable(fn):
                q_.put(
                    json.dumps(
                        {
                            "ok": False,
                            "err": "health_snapshot_sync_missing",
                            "ts": _ts(),
                            "ms": 0,
                            "checks": [],
                        },
                        ensure_ascii=False,
                        default=str,
                    )
                )
                return

            kw = _filter_kwargs(fn, {"deep": deep_, "timeout_s": timeout_s_, "timeout": timeout_s_})
            out = fn(**kw)
            q_.put(json.dumps(out, ensure_ascii=False, default=str))
        except Exception as e:
            q_.put(
                json.dumps(
                    {
                        "ok": False,
                        "err": f"sync_snapshot_failed: {type(e).__name__}: {e}",
                        "trace": traceback.format_exc()[:1200],
                        "ts": _ts(),
                        "ms": 0,
                        "checks": [],
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )

    p = ctx.Process(
        target=_child,
        args=(q, bool(deep), float(timeout_s), str(repo_root)),
        daemon=True,
    )
    p.start()
    p.join(timeout=max(0.2, float(hard_timeout_s)))

    if p.is_alive():
        with contextlib.suppress(Exception):
            p.terminate()
        p.join(timeout=0.5)
        return {
            "ts": _ts(),
            "ok": False,
            "ms": 0,
            "checks": [],
            "err": f"sync_snapshot_timeout:>{hard_timeout_s:.2f}s",
        }

    raw: str | None = None
    with contextlib.suppress(Exception):
        raw = q.get_nowait()

    if not raw:
        return {"ts": _ts(), "ok": False, "ms": 0, "checks": [], "err": "sync_snapshot_no_result"}

    with contextlib.suppress(Exception):
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj

    return {"ts": _ts(), "ok": False, "ms": 0, "checks": [], "err": "sync_snapshot_bad_payload"}


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
            "ok": False,
            "ms": 0,
            "checks": [],
            "err": f"import_health_failed: {type(e).__name__}: {e}",
        }

    hs = getattr(h, "health_snapshot", None)
    if callable(hs):
        kw = _filter_kwargs(hs, {"deep": deep, "timeout_s": timeout_s, "timeout": timeout_s})
        with _capture_stdio(enabled=json_only):
            try:
                return await asyncio.wait_for(hs(**kw), timeout=max(0.2, float(timeout_s)))
            except asyncio.TimeoutError:
                return {
                    "ts": _ts(),
                    "ok": False,
                    "ms": 0,
                    "checks": [],
                    "err": f"async_snapshot_timeout:>{timeout_s:.2f}s",
                }
            except Exception as e:
                return {
                    "ts": _ts(),
                    "ok": False,
                    "ms": 0,
                    "checks": [],
                    "err": f"async_snapshot_failed: {type(e).__name__}: {e}",
                    "trace": traceback.format_exc()[:1200],
                }

    allow_sync = _envb("HEALTHCHECK_ALLOW_SYNC_FALLBACK", True)
    hsync = getattr(h, "health_snapshot_sync", None)
    if allow_sync and callable(hsync):
        hard = min(max(0.5, float(timeout_s) + 0.75), 15.0)
        with _capture_stdio(enabled=json_only):
            return await asyncio.to_thread(
                _sync_snapshot_subproc,
                deep=deep,
                timeout_s=float(timeout_s),
                hard_timeout_s=float(hard),
                repo_root=str(repo_root),
            )

    return {"ts": _ts(), "ok": False, "ms": 0, "checks": [], "err": "health_snapshot_missing"}


async def _amain(args: argparse.Namespace) -> int:
    t0 = _now_ms()

    json_only = bool(args.json_only or _envb("HEALTHCHECK_JSON_ONLY", False))
    emit_json = bool(args.json or _envb("HEALTHCHECK_JSON", False) or json_only)

    skip, reason = _should_skip_entirely()
    if skip:
        payload = {"ts": _ts(), "ok": True, "skipped": True, "reason": reason, "ms": _now_ms() - t0}
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        else:
            print(f"READY (skipped: {reason})")
        return RC_READY

    mode = (args.mode or _envs("HEALTHCHECK_MODE", "auto")).strip().lower()
    if mode not in {"auto", "http", "local"}:
        payload = {"ts": _ts(), "ok": False, "error": f"bad mode={mode!r}", "ms": _now_ms() - t0}
        if emit_json:
            print(json.dumps(payload, ensure_ascii=False, default=str))
        else:
            print(f"MISCONFIG: {payload['error']}")
        return RC_MISCONFIG

    url = (args.url or _default_health_url()).strip()
    http_timeout_s = float(args.http_timeout or float(_envs("HEALTHCHECK_HTTP_TIMEOUT_S", "1.2")))
    http_retries = int(args.retries or int(_envs("HEALTHCHECK_RETRIES", "2")))
    http_retry_delay = float(args.retry_delay or float(_envs("HEALTHCHECK_RETRY_DELAY_S", "0.15")))

    deep = bool(args.deep or _envb("HEALTHCHECK_DEEP", False))
    snap_timeout_s = float(args.timeout or float(_envs("HEALTHCHECK_TIMEOUT_S", "8.0")))

    payload: dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "mode": mode,
        "http": None,
        "snapshot": None,
        "ms": 0,
    }

    async def _do_local() -> bool:
        snap = await _call_health_snapshot(deep=deep, timeout_s=snap_timeout_s, json_only=json_only)
        payload["snapshot"] = snap
        return bool(isinstance(snap, dict) and snap.get("ok") is True)

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
            payload["ms"] = _now_ms() - t0
            if emit_json:
                print(json.dumps(payload, ensure_ascii=False, default=str))
            else:
                print("READY (http)")
            return RC_READY

        if mode == "http":
            payload["ok"] = False
            payload["ms"] = _now_ms() - t0
            if emit_json:
                print(json.dumps(payload, ensure_ascii=False, default=str))
            else:
                print("NOT READY (http)")
            return RC_NOT_READY

        # auto fallback -> local if allowed
        allow_fallback = _envb("HEALTHCHECK_ALLOW_LOCAL_FALLBACK", True)
        if not allow_fallback:
            payload["ok"] = False
            payload["ms"] = _now_ms() - t0
            if emit_json:
                print(json.dumps(payload, ensure_ascii=False, default=str))
            else:
                print("NOT READY (auto; fallback disabled)")
            return RC_NOT_READY

    # local path
    ok_local = await _do_local()
    payload["ok"] = bool(ok_local)
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

    # Windows asyncio stability
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
            print(json.dumps({"ts": _ts(), "ok": False, "error": err[:1200]}, ensure_ascii=False))
        else:
            print(err)
        rc = RC_MISCONFIG

    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
