# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/preflight.py — God-Tier Preflight (Phase 0–7 LOCK-IN)
=============================================================

Canonical boot gate used by start.sh / Render.

Hard requirements
-----------------
- ZERO check side-effects in-process: every check runs in a child subprocess.
- Secret-safe logs: aggressive redaction (URL creds, header tokens, env secret values).
- Deterministic exit codes:
    0 = all required steps passed (optional may fail/skip)
    1 = only optional steps failed
    2 = any required step failed (OR --canary requested but cannot run / failed)
  (Ctrl-C returns 130)
- Human logs -> STDERR; JSON summary -> STDOUT (if enabled).
- Works on Render Linux and Windows dev machines.

Default plan (policy-aware)
---------------------------
0) scripts.check_env        REQUIRED  (cannot be skipped unless explicitly allowed)
1) scripts.check_rpc        REQUIRED
2) scripts.test_pg          REQUIRED  (auto-pruned if SKIP_DSN_REQUIRED=1)
3) scripts.check_schema     REQUIRED  (auto-pruned if SKIP_DSN_REQUIRED=1)
4) scripts.check_router     REQUIRED if present; optional if missing
5) scripts.smoke_lightning  REQUIRED if present; optional if missing; skippable
6) scripts.healthcheck      OPTIONAL if present
7) scripts.canary_trade     ONLY when --canary AND CANARY_TRADE_ENABLED=1
                             (treated as REQUIRED once requested)

Step 2 LOCK-IN
--------------
- check_env is ALWAYS executed with argv=["--json-only"] so preflight never depends
  on parsing human output.

Step 3 LOCK-IN
--------------
- If SKIP_DSN_REQUIRED=1, DB steps are pruned from the effective plan:
    - test_pg
    - check_schema

Windows support
---------------
- Preflight runs child subprocesses. On Windows, asyncio must use Proactor.

Env knobs (preflight)
---------------------
START_PRECHECKS=0|1
START_PRECHECKS_STRICT=0|1

PREFLIGHT_JSON=0|1                 (default: 1)
PREFLIGHT_JSON_ONLY=0|1
PREFLIGHT_CHILD_JSON=0|1           (attempt to parse child JSON and include it)
PREFLIGHT_CHILD_JSON_ONLY=0|1      (force child checks to JSON-only when supported)
PREFLIGHT_INCLUDE_CHILD_STDIO=0|1  (include sanitized child stdout/stderr in summary)

PREFLIGHT_VERBOSE=0|1              (stream sanitized child output live)
PREFLIGHT_STEP_TIMEOUT_S=90
PREFLIGHT_TOTAL_TIMEOUT_S=0        (0 disables)

PREFLIGHT_MAX_BYTES=196608         (per-stream cap)
PREFLIGHT_MAX_OUTPUT=3500          (chars cap after sanitize)

PREFLIGHT_REDACT_KEYS=K1,K2,...    (extra env keys to redact by exact value)
PREFLIGHT_MASK_LONG_TOKENS=0|1
PREFLIGHT_ALLOW_SKIP_CHECK_ENV=0|1
PREFLIGHT_ENFORCE_LIGHTNING_API=0|1
PREFLIGHT_KILL_TREE=0|1

Retry knobs
-----------
PREFLIGHT_RETRY_MAX=1              (retries AFTER first attempt; 0 disables)
PREFLIGHT_RETRY_BASE_S=0.6
PREFLIGHT_RETRY_JITTER_S=0.4
PREFLIGHT_RETRY_STEPS=check_rpc,check_router,smoke_lightning,healthcheck
"""

import argparse
import asyncio
import contextlib
import importlib.util
import json
import os
import platform
import random
import re
import socket
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

RC_OK = 0
RC_OPTIONAL_FAILED = 1
RC_REQUIRED_FAILED = 2


# =============================================================================
# repo-root bootstrap (import reliability only)
# =============================================================================
def _bootstrap_repo_root() -> str:
    """
    Make `python scripts/preflight.py` work by injecting repo root into sys.path.

    Heuristic: walk upwards from this file and look for:
      - pyproject.toml / requirements.txt / setup.cfg / .git / src/
    Fallback: parent of scripts/.
    """
    try:
        here = Path(__file__).resolve()
    except Exception:
        return ""

    candidates = [here.parent] + list(here.parents)
    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")

    root: Optional[Path] = None
    for p in candidates:
        try:
            if any((p / m).exists() for m in markers) or (p / "src").is_dir():
                root = p
                break
        except Exception:
            continue

    if root is None:
        root = here.parents[1] if len(here.parents) > 1 else here.parent

    root_str = str(root)
    if root_str and root_str not in sys.path:
        sys.path.insert(0, root_str)

    os.environ.setdefault("REPO_ROOT", root_str)
    return root_str


# =============================================================================
# env helpers
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = (v if v is not None else d)
    s = str(s).strip()
    # strip one layer of quotes
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


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _parse_csv(s: str) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]


# =============================================================================
# safe I/O (Windows cp1252-safe)
# =============================================================================
def _safe_write(file, s: str) -> None:
    try:
        file.write(s)
        file.flush()
        return
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = s.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            if hasattr(file, "buffer"):
                file.buffer.write(b)
                file.buffer.flush()
            else:
                file.write(b.decode(enc, errors="replace"))
                file.flush()
    except Exception:
        pass


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write(sys.stderr, f"[preflight] {_ts()} {msg}\n")


# =============================================================================
# redaction / safety
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_APIKEY_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")
_KV_SECRET_RE = re.compile(r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)")


def _redact_keys() -> List[str]:
    base = [
        "JUP_PRO_KEY",
        "JUP_API_KEY",
        "JUP_KEY",
        "OPENAI_API_KEY",
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_TOKEN",
        "DISCORD_WEBHOOK_URL",
        "NOTION_TOKEN",
        "WALLET_JSON",
        "WALLET_JSON_B64",
        "WALLET_PRIVATE_KEY",
        "KEYPAIR_B64",
        "KEYPAIR_PATH",
        "DATABASE_URL",
        "PG_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
        "POSTGRES_DSN",
        "PNL_DSN",
        "REDIS_URL",
        "REDIS_TLS_URL",
        "SOL_RPC_URL",
        "RPC_URL",
        "SOL_RPC_URLS",
    ]
    extra = _parse_csv(_envs("PREFLIGHT_REDACT_KEYS", ""))
    out = base + extra
    seen: Set[str] = set()
    uniq: List[str] = []
    for k in out:
        if k and k not in seen:
            uniq.append(k)
            seen.add(k)
    return uniq


def _sanitize(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_RE.sub(r"\1***", s)
    s = _BEARER_RE.sub(r"\1***", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)

    # Replace exact env secret values (best-effort).
    for k in _redact_keys():
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _envb("PREFLIGHT_MASK_LONG_TOKENS", True):
        s = _LONG_TOKENISH_RE.sub("***", s)
    return s


class _LiveSanitizer:
    """Streaming sanitizer for split-chunk secrets."""
    def __init__(self, tail_chars: int = 4096):
        self._tail_chars = max(256, int(tail_chars))
        self._tail = ""

    def feed(self, chunk_text: str) -> str:
        raw = self._tail + (chunk_text or "")
        if not raw:
            return ""
        tail_s = _sanitize(self._tail)
        full_s = _sanitize(raw)
        if full_s.startswith(tail_s):
            out = full_s[len(tail_s):]
        else:
            out = full_s
        self._tail = raw[-self._tail_chars:]
        return out


def _module_exists(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except Exception:
        return False


def _extract_json(text: str) -> Optional[Dict[str, Any]]:
    """Best-effort extraction of a dict JSON object from output."""
    if not text:
        return None
    raw = text.strip()
    if raw.startswith("{") and raw.endswith("}"):
        with contextlib.suppress(Exception):
            obj = json.loads(raw)
            if isinstance(obj, dict):
                return obj

    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    for ln in reversed(lines[-900:]):
        i = ln.find("{")
        j = ln.rfind("}")
        if i >= 0 and j > i:
            blob = ln[i:j + 1]
            with contextlib.suppress(Exception):
                obj = json.loads(blob)
                if isinstance(obj, dict):
                    return obj
    return None


# =============================================================================
# models
# =============================================================================
@dataclass(frozen=True)
class StepSpec:
    name: str
    module: str
    description: str = ""
    critical: bool = True
    timeout_s: Optional[float] = None
    argv: Tuple[str, ...] = ()
    env_overrides: Dict[str, str] = field(default_factory=dict)
    optional_if_missing: bool = False
    skip_if_env_true: Tuple[str, ...] = ()


@dataclass
class StepResult:
    name: str
    module: str
    ok: bool
    critical: bool
    rc: int
    seconds: float
    skipped: bool = False
    reason: str = ""
    warnings: List[str] = field(default_factory=list)
    child_json: Optional[Dict[str, Any]] = None
    fix_hints: List[str] = field(default_factory=list)
    attempts: int = 1
    stdout: str = ""
    stderr: str = ""

    def public(self, *, include_child_json: bool, include_stdio: bool) -> Dict[str, Any]:
        return {
            "name": self.name,
            "module": self.module,
            "ok": bool(self.ok),
            "critical": bool(self.critical),
            "rc": int(self.rc),
            "sec": round(float(self.seconds), 3),
            "skipped": bool(self.skipped),
            "reason": self.reason,
            "attempts": int(self.attempts),
            "warnings": list(self.warnings or []),
            "fix_hints": list(self.fix_hints or []),
            "child_json": (self.child_json if include_child_json else None),
            "stdout": (self.stdout if include_stdio else ""),
            "stderr": (self.stderr if include_stdio else ""),
        }


# =============================================================================
# subprocess runner (timeout-safe, best-effort no orphans)
# =============================================================================
def _win_creationflags() -> int:
    # CREATE_NEW_PROCESS_GROUP
    return 0x00000200 if os.name == "nt" else 0


def _posix_start_new_session() -> bool:
    return os.name != "nt"


async def _kill_tree_windows(pid: int) -> None:
    if pid <= 0:
        return
    try:
        proc = await asyncio.create_subprocess_exec(
            "taskkill", "/PID", str(pid), "/T", "/F",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=4.0)
    except Exception:
        pass


async def _terminate_process(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return

    with contextlib.suppress(Exception):
        proc.terminate()
    with contextlib.suppress(Exception):
        await asyncio.wait_for(proc.wait(), timeout=3.0)
        return

    try:
        if os.name == "nt":
            if _envb("PREFLIGHT_KILL_TREE", True):
                await _kill_tree_windows(int(proc.pid or 0))
            with contextlib.suppress(Exception):
                proc.kill()
        else:
            with contextlib.suppress(Exception):
                import signal
                if proc.pid:
                    os.killpg(proc.pid, signal.SIGKILL)
            with contextlib.suppress(Exception):
                proc.kill()
    except Exception:
        with contextlib.suppress(Exception):
            proc.kill()

    with contextlib.suppress(Exception):
        await asyncio.wait_for(proc.wait(), timeout=3.0)


async def _read_stream_capped(
    stream: asyncio.StreamReader,
    *,
    cap_bytes: int,
    stream_live: bool,
    json_only: bool,
    live_tag: str,
) -> bytes:
    buf = bytearray()
    sanitizer = _LiveSanitizer()

    while True:
        chunk = await stream.read(4096)
        if not chunk:
            break

        if stream_live and (not json_only):
            with contextlib.suppress(Exception):
                txt = chunk.decode("utf-8", "ignore")
                out = sanitizer.feed(txt)
                if out:
                    _safe_write(sys.stderr, f"[{live_tag}] {out}")

        remaining = cap_bytes - len(buf)
        if remaining > 0:
            buf.extend(chunk[:remaining])

    return bytes(buf)


def _inherit_pythonpath(env: Dict[str, str]) -> None:
    root = (env.get("REPO_ROOT") or os.getenv("REPO_ROOT") or "").strip()
    if not root:
        return
    existing = (env.get("PYTHONPATH") or "").strip()
    parts = [p for p in existing.split(os.pathsep) if p] if existing else []
    if root not in parts:
        parts.insert(0, root)
    env["PYTHONPATH"] = os.pathsep.join(parts)


async def _run_module(
    module_name: str,
    *,
    timeout_s: float,
    argv: Sequence[str],
    env_overrides: Dict[str, str],
    verbose: bool,
    json_only: bool,
    cap_bytes: int,
    cap_chars: int,
    live_tag: str,
) -> Tuple[int, str, str, bool]:
    exe = sys.executable or "python"
    env = os.environ.copy()
    env.update({k: str(v) for k, v in (env_overrides or {}).items()})
    env.setdefault("PYTHONUNBUFFERED", "1")
    _inherit_pythonpath(env)

    cmd = [exe, "-m", module_name, *list(argv)]

    kw: Dict[str, Any] = {
        "stdout": asyncio.subprocess.PIPE,
        "stderr": asyncio.subprocess.PIPE,
        "env": env,
        "creationflags": _win_creationflags(),
    }
    if _posix_start_new_session():
        kw["start_new_session"] = True

    timed_out = False
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, **kw)
    except NotImplementedError as e:
        err = _sanitize(f"NotImplementedError: {e} (Windows requires Proactor event loop for subprocesses)")
        return 2, "", err, False
    except Exception as e:
        err = _sanitize(f"{type(e).__name__}: {e}")
        return 2, "", err, False

    assert proc.stdout is not None
    assert proc.stderr is not None

    t_out = asyncio.create_task(
        _read_stream_capped(
            proc.stdout,
            cap_bytes=cap_bytes,
            stream_live=verbose,
            json_only=json_only,
            live_tag=live_tag,
        )
    )
    t_err = asyncio.create_task(
        _read_stream_capped(
            proc.stderr,
            cap_bytes=cap_bytes,
            stream_live=verbose,
            json_only=json_only,
            live_tag=live_tag,
        )
    )

    try:
        await asyncio.wait_for(proc.wait(), timeout=timeout_s)
    except asyncio.TimeoutError:
        timed_out = True
        await _terminate_process(proc)
    except asyncio.CancelledError:
        await _terminate_process(proc)
        raise
    except Exception:
        await _terminate_process(proc)

    out_b = b""
    err_b = b""
    with contextlib.suppress(Exception):
        out_b = await asyncio.wait_for(t_out, timeout=2.0)
    with contextlib.suppress(Exception):
        err_b = await asyncio.wait_for(t_err, timeout=2.0)

    out = _sanitize((out_b or b"").decode("utf-8", "ignore")).strip()
    err = _sanitize((err_b or b"").decode("utf-8", "ignore")).strip()

    if cap_chars > 0:
        if len(out) > cap_chars:
            out = out[:cap_chars] + "…"
        if len(err) > cap_chars:
            err = err[:cap_chars] + "…"

    if timed_out:
        if not err:
            err = _sanitize(f"timeout after {timeout_s:.0f}s")
        return 124, out, err, True

    rc = int(proc.returncode or 0)
    return rc, out, err, False


# =============================================================================
# step argv builders / retry policy / postcheck
# =============================================================================
def _smoke_argv_from_env() -> Tuple[str, ...]:
    amount = _envs("PREFLIGHT_SMOKE_AMOUNT_SOL", "0.005") or "0.005"
    roundtrip = "1" if _envb("PREFLIGHT_SMOKE_ROUNDTRIP", True) else "0"
    out = _envs("PREFLIGHT_SMOKE_OUT", "USDC") or "USDC"
    slp = _envs("PREFLIGHT_SMOKE_SLIPPAGE_BPS", "50") or "50"
    only_direct = "1" if _envb("PREFLIGHT_SMOKE_ONLY_DIRECT", False) else "0"
    return ("--amount-sol", amount, "--roundtrip", roundtrip, "--out", out, "--slippage-bps", slp, "--only-direct", only_direct)


def _retry_policy() -> Tuple[int, float, float, Set[str]]:
    max_retries = max(0, _envi("PREFLIGHT_RETRY_MAX", 1))
    base_s = max(0.05, float(_envf("PREFLIGHT_RETRY_BASE_S", 0.6)))
    jitter_s = max(0.0, float(_envf("PREFLIGHT_RETRY_JITTER_S", 0.4)))
    steps = set(_parse_csv(_envs("PREFLIGHT_RETRY_STEPS", "check_rpc,check_router,smoke_lightning,healthcheck")))
    return max_retries, base_s, jitter_s, steps


async def _sleep_backoff(attempt_idx: int, base_s: float, jitter_s: float) -> None:
    delay = base_s * (2 ** max(0, attempt_idx - 1))
    delay += random.random() * jitter_s
    await asyncio.sleep(min(delay, 8.0))


def _coerce_list(x: Any) -> List[str]:
    if isinstance(x, list):
        return [str(i) for i in x if str(i).strip()]
    if isinstance(x, tuple):
        return [str(i) for i in x if str(i).strip()]
    return []


def _child_json_ok(child: Optional[Dict[str, Any]], rc: int) -> bool:
    if isinstance(child, dict) and "ok" in child:
        return bool(child.get("ok"))
    return rc == 0


def _postcheck(
    spec: StepSpec,
    rc: int,
    out: str,
    err: str,
    child_json: Optional[Dict[str, Any]],
) -> Tuple[bool, List[str], List[str]]:
    ok = _child_json_ok(child_json, rc)
    warnings: List[str] = []
    fix_hints: List[str] = []

    if isinstance(child_json, dict):
        warnings.extend(_coerce_list(child_json.get("warnings")))
        fix_hints.extend(_coerce_list(child_json.get("fix_hints")))

        # Some of your check scripts emit structured fix hints under different keys
        if not fix_hints:
            fix_hints.extend(_coerce_list(child_json.get("hints")))
        if not fix_hints and isinstance(child_json.get("detail"), str):
            fix_hints.append(str(child_json["detail"]))

    # lightning enforcement
    if spec.name == "smoke_lightning":
        blob = (out or "") + "\n" + (err or "")
        if "falling back to direct http" in blob.lower():
            warnings.append("LIGHTNING_FALLBACK_TO_HTTP")
            if _envb("PREFLIGHT_ENFORCE_LIGHTNING_API", False):
                ok = False
                fix_hints.append("Lightning fallback detected but PREFLIGHT_ENFORCE_LIGHTNING_API=1. Fix lightning_impl or disable enforcement.")

    blob_l = ((out or "") + "\n" + (err or "")).lower()
    if not ok:
        # generic hints
        if "missing env" in blob_l or "missing required env" in blob_l:
            fix_hints.append("Missing environment variables. Re-check Render Secrets and .env/.env.extra wiring.")
        if "timeout" in blob_l or rc == 124:
            fix_hints.append("Timeout. Increase PREFLIGHT_STEP_TIMEOUT_S or fix endpoint latency.")
        if "unauthorized" in blob_l or "forbidden" in blob_l or " 401" in blob_l or " 403" in blob_l:
            fix_hints.append("Auth error. Verify API keys (JUP_PRO_KEY/JUP_API_KEY) and endpoints.")
        if "dns" in blob_l or "getaddrinfo" in blob_l or "could not resolve" in blob_l or "name or service not known" in blob_l:
            fix_hints.append("DNS/network issue. Verify outbound network/DNS or fix DSN host.")
        if "connection refused" in blob_l:
            fix_hints.append("Connection refused. Check service URL/port and runtime networking.")
        if "proactor" in blob_l and os.name == "nt":
            fix_hints.append("Windows requires Proactor event loop for subprocesses (do not force Selector policy).")

    warnings = list(dict.fromkeys([w for w in warnings if w]))[:12]
    fix_hints = list(dict.fromkeys([h for h in fix_hints if h]))[:14]
    return ok, warnings, fix_hints


# =============================================================================
# orchestration
# =============================================================================
async def _run_step(
    spec: StepSpec,
    *,
    default_timeout_s: float,
    remaining_total_s: Optional[float],
    verbose: bool,
    json_only: bool,
    strict: bool,
    include_child_json: bool,
    force_child_json_only: bool,
    include_stdio: bool,
) -> StepResult:
    t0 = time.monotonic()

    # env-based skip (policy)
    for k in spec.skip_if_env_true:
        if _envb(k, False):
            dt = time.monotonic() - t0
            _hlog(f"STEP skip : {spec.name} (policy {k}=1)", json_only=json_only)
            return StepResult(
                name=spec.name,
                module=spec.module,
                ok=True,
                critical=spec.critical,
                rc=0,
                seconds=float(dt),
                skipped=True,
                reason=f"policy_skip:{k}=1",
            )

    # module presence
    if not _module_exists(spec.module):
        dt = time.monotonic() - t0
        if spec.optional_if_missing:
            _hlog(f"STEP skip : {spec.name} (missing {spec.module})", json_only=json_only)
            return StepResult(
                name=spec.name,
                module=spec.module,
                ok=True,
                critical=spec.critical,
                rc=127,
                seconds=float(dt),
                skipped=True,
                reason="missing_module",
            )
        _hlog(f"STEP FAIL : {spec.name} missing {spec.module} (critical={int(spec.critical)})", json_only=json_only)
        return StepResult(
            name=spec.name,
            module=spec.module,
            ok=False,
            critical=spec.critical,
            rc=127,
            seconds=float(dt),
            skipped=True,
            reason="missing_module",
        )

    step_timeout = float(spec.timeout_s if spec.timeout_s is not None else default_timeout_s)
    step_timeout = max(8.0, step_timeout)
    if remaining_total_s is not None:
        step_timeout = min(step_timeout, max(1.0, float(remaining_total_s)))

    cap_bytes = max(8192, _envi("PREFLIGHT_MAX_BYTES", 196608))
    cap_chars = max(500, _envi("PREFLIGHT_MAX_OUTPUT", 3500))

    env_overrides = dict(spec.env_overrides or {})
    argv = list(spec.argv or ())

    # LOCK-IN: check_env always JSON-only; strict forwarded
    if spec.name == "check_env":
        if "--json-only" not in argv:
            argv.append("--json-only")
        env_overrides.setdefault("CHECK_ENV_JSON", "1")
        env_overrides.setdefault("CHECK_ENV_JSON_ONLY", "1")
        if strict:
            if "--strict" not in argv:
                argv.append("--strict")
            env_overrides.setdefault("CHECK_ENV_STRICT", "1")

    # Optional: force child JSON-only modes for supported scripts (to make parsing deterministic)
    if force_child_json_only:
        mapping = {
            "check_env": ("CHECK_ENV_JSON", "CHECK_ENV_JSON_ONLY"),
            "check_rpc": ("CHECK_RPC_JSON", "CHECK_RPC_JSON_ONLY"),
            "test_pg": ("PG_TEST_JSON", "PG_TEST_JSON_ONLY"),
            "check_schema": ("SCHEMA_JSON", "SCHEMA_JSON_ONLY"),
            "check_router": ("CHECK_ROUTER_JSON", "CHECK_ROUTER_JSON_ONLY"),
            "smoke_lightning": ("SMOKE_JSON", "SMOKE_JSON_ONLY"),
            "healthcheck": ("HEALTHCHECK_JSON", "HEALTHCHECK_JSON_ONLY"),
            "canary_trade": ("CANARY_JSON", "CANARY_JSON_ONLY"),
        }
        a, b = mapping.get(spec.name, (None, None))
        if a:
            env_overrides.setdefault(a, "1")
        if b:
            env_overrides.setdefault(b, "1")

    _hlog(f"STEP start: {spec.name} ({spec.module}) timeout={int(step_timeout)}s argv={argv}", json_only=json_only)

    max_retries, base_s, jitter_s, retry_steps = _retry_policy()
    attempts = 0

    last_rc = 0
    last_out = ""
    last_err = ""
    last_child: Optional[Dict[str, Any]] = None
    last_warn: List[str] = []
    last_hints: List[str] = []

    while True:
        attempts += 1

        rc, out, err, _timed_out = await _run_module(
            spec.module,
            timeout_s=step_timeout,
            argv=argv,
            env_overrides=env_overrides,
            verbose=verbose,
            json_only=json_only,
            cap_bytes=cap_bytes,
            cap_chars=cap_chars,
            live_tag=spec.name,
        )

        child_obj = None
        if include_child_json or spec.name == "check_env":
            child_obj = _extract_json(out) or _extract_json(err)

        ok, warnings, fix_hints = _postcheck(spec, rc, out, err, child_obj)

        last_rc, last_out, last_err, last_child = rc, out, err, child_obj
        last_warn, last_hints = warnings, fix_hints

        if ok:
            dt = time.monotonic() - t0
            _hlog(f"STEP done : {spec.name} ok=1 rc={rc} sec={dt:.2f} warn={warnings}", json_only=json_only)
            return StepResult(
                name=spec.name,
                module=spec.module,
                ok=True,
                critical=spec.critical,
                rc=int(rc),
                seconds=float(dt),
                skipped=False,
                reason="",
                warnings=warnings,
                child_json=(child_obj if isinstance(child_obj, dict) else None),
                fix_hints=fix_hints,
                attempts=attempts,
                stdout=(out if include_stdio else ""),
                stderr=(err if include_stdio else ""),
            )

        # Retry only if not strict and step is retry-eligible and rc looks transient
        transient = (rc == 124) or ("timeout" in (err or "").lower()) or ("server closed" in (err or "").lower())
        can_retry = (
            (not strict)
            and (spec.name in retry_steps)
            and (max_retries > 0)
            and ((attempts - 1) <= max_retries)
            and transient
        )

        if can_retry and (attempts <= (1 + max_retries)):
            _hlog(f"STEP retry: {spec.name} attempt={attempts+1}/{1+max_retries}", json_only=json_only)
            await _sleep_backoff(attempts, base_s, jitter_s)
            continue

        # final fail
        dt = time.monotonic() - t0
        reason = ""
        if rc == 124:
            reason = "timeout"
        elif err:
            reason = err[:220]

        _hlog(f"STEP done : {spec.name} ok=0 rc={rc} sec={dt:.2f} warn={warnings}", json_only=json_only)
        return StepResult(
            name=spec.name,
            module=spec.module,
            ok=False,
            critical=spec.critical,
            rc=int(last_rc),
            seconds=float(dt),
            skipped=False,
            reason=reason,
            warnings=last_warn,
            child_json=(last_child if isinstance(last_child, dict) else None),
            fix_hints=last_hints,
            attempts=attempts,
            stdout=(last_out if include_stdio else ""),
            stderr=(last_err if include_stdio else ""),
        )


def _build_registry(timeout_s: float) -> List[StepSpec]:
    # Only pass allow-create to check_schema if explicitly enabled.
    schema_argv: List[str] = []
    if _envb("ALLOW_SCHEMA_CREATE", False):
        schema_argv.append("--allow-create")
        if _envb("SCHEMA_DRYRUN", False):
            schema_argv.append("--dryrun")

    schema_profile = _envs("SCHEMA_PROFILE", "")
    if schema_profile:
        schema_argv.extend(["--profile", schema_profile])

    return [
        StepSpec(
            name="check_env",
            module="scripts.check_env",
            description="Hard gate: env/format sanity.",
            critical=True,
            timeout_s=min(timeout_s, 60.0),
            argv=("--json-only",),
            optional_if_missing=False,
        ),
        StepSpec(
            name="check_rpc",
            module="scripts.check_rpc",
            description="RPC sanity (health/blockhash/slot).",
            critical=True,
            timeout_s=min(timeout_s, 60.0),
            optional_if_missing=False,
        ),
        StepSpec(
            name="test_pg",
            module="scripts.test_pg",
            description="DB connectivity (auto-pruned if SKIP_DSN_REQUIRED=1).",
            critical=True,
            optional_if_missing=False,
            skip_if_env_true=("SKIP_DSN_REQUIRED",),
            argv=("--json-only",),
        ),
        StepSpec(
            name="check_schema",
            module="scripts.check_schema",
            description="Schema guard (auto-pruned if SKIP_DSN_REQUIRED=1).",
            critical=True,
            argv=tuple(["--json-only", *schema_argv]),
            optional_if_missing=False,
            skip_if_env_true=("SKIP_DSN_REQUIRED",),
        ),
        StepSpec(
            name="check_router",
            module="scripts.check_router",
            description="Router quote sanity (required if present; optional if missing).",
            critical=True,
            optional_if_missing=True,
            argv=("--json-only",),
        ),
        StepSpec(
            name="smoke_lightning",
            module="scripts.smoke_lightning",
            description="Lightning/Jupiter quote sanity (required if present; optional if missing; can be skipped).",
            critical=True,
            timeout_s=min(timeout_s, 75.0),
            argv=tuple(["--json-only", *_smoke_argv_from_env()]),
            optional_if_missing=True,
        ),
        StepSpec(
            name="healthcheck",
            module="scripts.healthcheck",
            description="Infra probes (optional).",
            critical=False,
            timeout_s=min(timeout_s, 45.0),
            optional_if_missing=True,
            argv=("--json-only",),
        ),
        StepSpec(
            name="canary_trade",
            module="scripts.canary_trade",
            description="Trade pipeline canary (default DRYRUN).",
            critical=False,
            timeout_s=max(timeout_s, 180.0),
            env_overrides={"CANARY_MODE": "dryrun", "DRYRUN": "1"},
            optional_if_missing=True,
            argv=("--json-only",),
        ),
    ]


def _policy_prune(plan: List[StepSpec]) -> Tuple[List[StepSpec], List[Dict[str, Any]]]:
    """
    LOCK-IN: prune steps from effective plan when policy says so.
    Also prune optional-missing steps from effective plan if module isn't present.
    """
    pruned: List[StepSpec] = []
    skips: List[Dict[str, Any]] = []

    for s in plan:
        env_skip = None
        for k in s.skip_if_env_true:
            if _envb(k, False):
                env_skip = k
                break
        if env_skip:
            skips.append({"name": s.name, "reason": f"policy_skip:{env_skip}=1"})
            continue

        if (not _module_exists(s.module)) and s.optional_if_missing:
            skips.append({"name": s.name, "reason": "missing_module"})
            continue

        pruned.append(s)

    return pruned, skips


async def _amain() -> int:
    ap = argparse.ArgumentParser(description="Run sniper preflight checks (Phase 0–7 LOCK-IN).")
    ap.add_argument("--strict", action="store_true", help="Stop on first failure.")
    ap.add_argument("--canary", action="store_true", help="Include canary_trade (requires CANARY_TRADE_ENABLED=1).")
    ap.add_argument("--no-smoke", action="store_true", help="Skip smoke_lightning step.")
    ap.add_argument("--timeout", type=float, default=_envf("PREFLIGHT_STEP_TIMEOUT_S", 90.0), help="Per-step timeout seconds.")
    ap.add_argument("--total-timeout", type=float, default=_envf("PREFLIGHT_TOTAL_TIMEOUT_S", 0.0), help="Overall timeout seconds (0 disables).")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only emit JSON summary (no human logs).")
    ap.add_argument("--verbose", action="store_true", help="Stream sanitized child output live to STDERR.")
    ap.add_argument("--list", action="store_true", help="List available steps and exit.")
    ap.add_argument("--print-plan", action="store_true", help="Print resolved plan as JSON and exit.")
    ap.add_argument("--steps", type=str, default="", help="Run only these steps (csv). Example: check_env,check_rpc")
    ap.add_argument("--skip", type=str, default="", help="Skip these steps (csv).")
    args = ap.parse_args()

    json_only = bool(args.json_only or _envb("PREFLIGHT_JSON_ONLY", False))
    start_prechecks = _envb("START_PRECHECKS", False)

    explicit_intent = bool(
        args.strict
        or args.canary
        or args.json
        or args.json_only
        or args.verbose
        or args.steps
        or args.skip
        or args.list
        or args.no_smoke
        or args.print_plan
    )

    if (not start_prechecks) and (not explicit_intent):
        _hlog("START_PRECHECKS!=1 and no CLI intent → skipping preflight", json_only=json_only)
        return RC_OK

    strict = bool(args.strict or _envb("START_PRECHECKS_STRICT", False))
    timeout_s = max(10.0, float(args.timeout))
    total_timeout_s = max(0.0, float(args.total_timeout))
    verbose = bool(args.verbose or _envb("PREFLIGHT_VERBOSE", False))

    emit_json = bool(args.json or json_only or _envb("PREFLIGHT_JSON", True))
    include_child_json = bool(_envb("PREFLIGHT_CHILD_JSON", False))
    force_child_json_only = bool(_envb("PREFLIGHT_CHILD_JSON_ONLY", False))
    include_stdio = bool(_envb("PREFLIGHT_INCLUDE_CHILD_STDIO", False))

    canary_env_enabled = _envb("CANARY_TRADE_ENABLED", False)
    want_canary = bool(args.canary)
    skip_smoke = bool(args.no_smoke) or _envb("SKIP_SMOKE_LIGHTNING", False)

    if want_canary and (not canary_env_enabled):
        summary = {
            "ts": _ts(),
            "ok": False,
            "critical_failed": True,
            "optional_failed": False,
            "reason": "--canary requested but CANARY_TRADE_ENABLED!=1",
        }
        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        _hlog(summary["reason"], json_only=json_only)
        return RC_REQUIRED_FAILED

    registry = _build_registry(timeout_s=timeout_s)

    if args.list:
        _hlog("AVAILABLE_STEPS:", json_only=json_only)
        for s in registry:
            _hlog(
                f" - {s.name:14} module={s.module} critical={int(s.critical)} "
                f"optional_if_missing={int(s.optional_if_missing)} skip_if={list(s.skip_if_env_true)} argv={list(s.argv)} :: {s.description}",
                json_only=json_only,
            )
        return RC_OK

    only_steps = set(_parse_csv(args.steps))
    skip_steps = set(_parse_csv(args.skip))

    allow_skip_env = _envb("PREFLIGHT_ALLOW_SKIP_CHECK_ENV", False)
    if "check_env" in skip_steps and (not allow_skip_env):
        _hlog("Refusing to skip check_env (set PREFLIGHT_ALLOW_SKIP_CHECK_ENV=1 to override).", json_only=json_only)
        skip_steps.discard("check_env")

    # If user chose --steps but forgot check_env, inject it (unless explicitly allowed to skip).
    if only_steps and ("check_env" not in only_steps) and (not allow_skip_env):
        only_steps.add("check_env")

    if skip_smoke:
        skip_steps.add("smoke_lightning")

    # build plan (selection layer)
    plan: List[StepSpec] = []
    for s in registry:
        if only_steps and (s.name not in only_steps):
            continue
        if s.name in skip_steps:
            continue

        if s.name == "canary_trade":
            if not want_canary:
                continue
            # once requested, canary becomes REQUIRED and must exist
            plan.append(
                StepSpec(
                    name=s.name,
                    module=s.module,
                    description=s.description,
                    critical=True,
                    timeout_s=s.timeout_s,
                    argv=s.argv,
                    env_overrides=s.env_overrides,
                    optional_if_missing=False,
                    skip_if_env_true=s.skip_if_env_true,
                )
            )
            continue

        plan.append(s)

    if not plan:
        _hlog("No steps selected (after --steps/--skip/canary gating).", json_only=json_only)
        return RC_OK

    # policy prune -> effective plan
    effective_plan, effective_skips = _policy_prune(plan)

    if args.print_plan:
        blob = {
            "ts": _ts(),
            "plan": [s.name for s in plan],
            "effective_plan": [s.name for s in effective_plan],
            "effective_skips": effective_skips,
            "skip_smoke": bool(skip_smoke),
            "policy": {
                "SKIP_DSN_REQUIRED": int(_envb("SKIP_DSN_REQUIRED", False)),
                "ALLOW_SCHEMA_CREATE": int(_envb("ALLOW_SCHEMA_CREATE", False)),
                "CANARY_TRADE_ENABLED": int(canary_env_enabled),
            },
        }
        print(json.dumps(blob, ensure_ascii=False, default=str))
        return RC_OK

    _hlog(
        "BOOT "
        f"strict={int(strict)} timeout_s={int(timeout_s)} total_timeout_s={int(total_timeout_s)} "
        f"SKIP_DSN_REQUIRED={int(_envb('SKIP_DSN_REQUIRED', False))} "
        f"ALLOW_SCHEMA_CREATE={int(_envb('ALLOW_SCHEMA_CREATE', False))} "
        f"CANARY_TRADE_ENABLED={int(canary_env_enabled)} "
        f"child_json={int(include_child_json)} force_child_json_only={int(force_child_json_only)} "
        f"include_child_stdio={int(include_stdio)} plan={[s.name for s in effective_plan]}",
        json_only=json_only,
    )

    results: List[StepResult] = []
    t_start = time.monotonic()

    critical_failed = False
    optional_failed = False

    for spec in effective_plan:
        elapsed = time.monotonic() - t_start
        remaining_total: Optional[float] = None
        if total_timeout_s > 0:
            remaining_total = float(total_timeout_s - elapsed)
            if remaining_total <= 0:
                critical_failed = True
                results.append(
                    StepResult(
                        name=spec.name,
                        module=spec.module,
                        ok=False,
                        critical=spec.critical,
                        rc=124,
                        seconds=0.0,
                        skipped=True,
                        reason=f"total_timeout>{total_timeout_s:.0f}s",
                    )
                )
                break

        r = await _run_step(
            spec,
            default_timeout_s=timeout_s,
            remaining_total_s=remaining_total,
            verbose=verbose,
            json_only=json_only,
            strict=strict,
            include_child_json=include_child_json,
            force_child_json_only=force_child_json_only,
            include_stdio=include_stdio,
        )
        results.append(r)

        if not r.ok:
            if spec.critical:
                critical_failed = True
            else:
                optional_failed = True

            # check_env failure always escalates and fail-fast
            if spec.name == "check_env":
                critical_failed = True
                strict = True

            if strict:
                break

    overall_ok = (not critical_failed) and (not optional_failed)

    summary: Dict[str, Any] = {
        "ts": _ts(),
        "ok": bool(overall_ok),
        "critical_failed": bool(critical_failed),
        "optional_failed": bool(optional_failed),
        "strict": bool(strict),
        "timeout_s": float(timeout_s),
        "total_timeout_s": float(total_timeout_s),
        "effective_plan": [s.name for s in effective_plan],
        "effective_skips": effective_skips,
        "policy": {
            "SKIP_DSN_REQUIRED": int(_envb("SKIP_DSN_REQUIRED", False)),
            "ALLOW_SCHEMA_CREATE": int(_envb("ALLOW_SCHEMA_CREATE", False)),
            "CANARY_TRADE_ENABLED": int(canary_env_enabled),
            "SKIP_SMOKE_LIGHTNING": int(skip_smoke),
        },
        "meta": {
            "python": _sanitize(sys.version.split()[0]),
            "platform": _sanitize(platform.platform()),
            "hostname": _sanitize(socket.gethostname()),
            "pid": os.getpid(),
            "cwd": _sanitize(os.getcwd()),
            "argv": [_sanitize(x) for x in sys.argv[:120]],
            "include_child_json": bool(include_child_json),
            "force_child_json_only": bool(force_child_json_only),
            "include_child_stdio": bool(include_stdio),
        },
        "results": [x.public(include_child_json=include_child_json, include_stdio=include_stdio) for x in results],
    }

    if emit_json:
        print(json.dumps(summary, ensure_ascii=False, default=str))

    if overall_ok:
        _hlog("SUMMARY ok=1 (preflight passed)", json_only=json_only)
        return RC_OK

    if critical_failed:
        _hlog("SUMMARY ok=0 (required step failed)", json_only=json_only)
        return RC_REQUIRED_FAILED

    _hlog("SUMMARY ok=0 (only optional steps failed)", json_only=json_only)
    return RC_OPTIONAL_FAILED


def main() -> None:
    # Windows MUST use Proactor policy for asyncio subprocess support.
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # type: ignore[attr-defined]

    with contextlib.suppress(Exception):
        _bootstrap_repo_root()

    try:
        rc = asyncio.run(_amain())
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        _hlog(f"FAIL unhandled err={_sanitize(type(e).__name__)}: {_sanitize(e)}", json_only=False)
        rc = RC_REQUIRED_FAILED
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
