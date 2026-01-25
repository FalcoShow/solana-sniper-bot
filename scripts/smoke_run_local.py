# scripts/smoke_run_local.py
from __future__ import annotations

"""
scripts/smoke_run_local.py — V3+++ Local "GO LIVE" Smoke Runner (LOCK-IN)
========================================================================

Goal
----
One command to validate the repo is safe to go live (locally) before pushing to Render:

  1) Compile checks (py_compile on key scripts)
  2) Dependency checks (imports)
  3) Preflight (scripts/preflight.py)
  4) Smoke suite:
        - scripts/smoke_init.py
        - scripts/smoke_core.py
        - scripts/smoke_lightning.py
        - scripts/smoke_pg_only.py
  5) Optional: scripts/backtest.py --dry-run

Hard rules
----------
- NEVER prints secrets (DSN passwords, API keys, wallet keys).
- Captures child stdout/stderr and sanitizes before printing.
- JSON summary (when enabled) goes to STDOUT; human logs to STDERR.

Exit codes
----------
0 = success
2 = failure
130 = ctrl-c

CLI examples
------------
py -m scripts.smoke_run_local
py -m scripts.smoke_run_local --json
py -m scripts.smoke_run_local --only preflight,smoke_core,smoke_lightning --debug
py -m scripts.smoke_run_local --skip-network 1
py -m scripts.smoke_run_local --skip-backtest 1

Env knobs
---------
SMOKE_JSON=0|1
SMOKE_JSON_ONLY=0|1
SMOKE_DEBUG=0|1
SMOKE_TIMEOUT_S=20
SMOKE_TOTAL_TIMEOUT_S=0
SMOKE_SKIP_NETWORK=0|1
SMOKE_SKIP_PG=0|1                 (skip pg smoke only)
SMOKE_SKIP_LIGHTNING=0|1          (skip lightning smoke only)
SMOKE_SKIP_BACKTEST=0|1
SMOKE_SKIP_COMPILE=0|1
SMOKE_SKIP_DEPS=0|1

PREFLIGHT_MASK_LONG_TOKENS=1      (default)
"""

import argparse
import contextlib
import json
import os
import platform
import re
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

RC_OK = 0
RC_FAIL = 2
RC_CTRL_C = 130

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")


# ──────────────────────────────────────────────────────────────────────────────
# Env helpers
# ──────────────────────────────────────────────────────────────────────────────
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


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now_ms() -> int:
    return int(time.time() * 1000)


# ──────────────────────────────────────────────────────────────────────────────
# Safe printing (cp1252-safe)
# ──────────────────────────────────────────────────────────────────────────────
def _safe_write(file, s: str) -> None:
    try:
        file.write(s)
        file.flush()
    except UnicodeEncodeError:
        enc = getattr(file, "encoding", None) or "utf-8"
        b = s.encode(enc, errors="replace")
        with contextlib.suppress(Exception):
            if hasattr(file, "buffer"):
                file.buffer.write(b)
                file.buffer.flush()
                return
        with contextlib.suppress(Exception):
            file.write(b.decode(enc, errors="replace"))
            file.flush()
    except Exception:
        return


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write(sys.stderr, f"[smoke_run_local] {_ts()} {msg}\n")


# ──────────────────────────────────────────────────────────────────────────────
# Redaction / sanitization
# ──────────────────────────────────────────────────────────────────────────────
_SENSITIVE_ENV_KEYS = [
    # Jupiter / API reopening keys
    "JUP_PRO_KEY",
    "JUP_API_KEY",
    "JUP_KEY",
    # Wallet / secrets
    "WALLET_PRIVATE_KEY",
    "WALLET_JSON",
    "WALLET_JSON_B64",
    "KEYPAIR_B64",
    "KEYPAIR_PATH",
    # DSNs
    "DATABASE_URL",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "POSTGRES_DSN",
    "PNL_DSN",
    # Redis
    "REDIS_URL",
    "REDIS_TLS_URL",
    # Misc
    "OPENAI_API_KEY",
    "TELEGRAM_BOT_TOKEN",
    "DISCORD_TOKEN",
    "DISCORD_WEBHOOK_URL",
    "NOTION_TOKEN",
    "RPC_URL",
    "SOL_RPC_URL",
]


def _sanitize(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _URL_CRED_RE.sub(r"\1***@", s)

    # Replace exact env values with ***
    for k in _SENSITIVE_ENV_KEYS:
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    # Optionally mask long token-ish substrings
    if _envb("PREFLIGHT_MASK_LONG_TOKENS", True):
        s = _LONG_TOKENISH_RE.sub("***", s)

    return s


def _mask_dsn_password(dsn: str) -> str:
    """Hide password but keep host/db visible if URL-shaped; otherwise sanitize."""
    try:
        from urllib.parse import urlparse, urlunparse

        u = urlparse(dsn)
        if not u.username:
            return _sanitize(dsn)
        netloc = u.netloc
        # Replace user:pass@ with user:***@
        netloc = re.sub(rf"{re.escape(u.username)}:([^@]+)@", f"{u.username}:***@", netloc)
        return _sanitize(urlunparse((u.scheme, netloc, u.path, u.params, u.query, u.fragment)))
    except Exception:
        return _sanitize("<dsn>")


# ──────────────────────────────────────────────────────────────────────────────
# Repo root discovery
# ──────────────────────────────────────────────────────────────────────────────
def _repo_root() -> Path:
    here = Path(__file__).resolve()
    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    for p in [here.parent, *list(here.parents)]:
        if any((p / m).exists() for m in markers) or (p / "src").is_dir():
            return p
    return here.parents[1] if len(here.parents) > 1 else here.parent


# ──────────────────────────────────────────────────────────────────────────────
# Subprocess runner (captures + sanitizes output)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class Step:
    name: str
    cmd: list[str]
    timeout_s: float
    optional: bool = False
    env_extra: dict[str, str] | None = None


@dataclass
class StepResult:
    name: str
    ok: bool
    rc: int
    ms: int
    out: str
    err: str


def _run_step(step: Step, *, json_only: bool, debug: bool) -> StepResult:
    t0 = _now_ms()
    env = dict(os.environ)

    # Safe defaults
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PREFLIGHT_MASK_LONG_TOKENS", "1")

    # Extra per-step env (sanitized by child scripts, but we still mask output here)
    if step.env_extra:
        env.update({k: str(v) for k, v in step.env_extra.items()})

    if not json_only:
        _hlog(f"$ {' '.join(step.cmd)}", json_only=False)

    try:
        cp = subprocess.run(
            step.cmd,
            cwd=str(_repo_root()),
            env=env,
            capture_output=True,
            text=True,
            timeout=float(step.timeout_s),
        )
        rc = int(cp.returncode)
        out = _sanitize(cp.stdout or "")
        err = _sanitize(cp.stderr or "")
        ms = int(_now_ms() - t0)

        ok = rc == 0

        if debug and (not json_only):
            if out.strip():
                _hlog(f"[{step.name}] stdout:\n{out.rstrip()}", json_only=False)
            if err.strip():
                _hlog(f"[{step.name}] stderr:\n{err.rstrip()}", json_only=False)

        return StepResult(name=step.name, ok=ok, rc=rc, ms=ms, out=out, err=err)

    except subprocess.TimeoutExpired as e:
        ms = int(_now_ms() - t0)
        out = _sanitize((getattr(e, "stdout", "") or "")[:6000])
        err = _sanitize((getattr(e, "stderr", "") or "")[:6000])
        if not json_only:
            _hlog(f"[{step.name}] TIMEOUT after {step.timeout_s}s", json_only=False)
        return StepResult(
            name=step.name, ok=False, rc=RC_FAIL, ms=ms, out=out, err=err or "timeout"
        )

    except KeyboardInterrupt:
        raise

    except Exception as e:
        ms = int(_now_ms() - t0)
        if not json_only:
            _hlog(f"[{step.name}] FAIL: {type(e).__name__}: {_sanitize(e)}", json_only=False)
        return StepResult(
            name=step.name,
            ok=False,
            rc=RC_FAIL,
            ms=ms,
            out="",
            err=_sanitize(f"{type(e).__name__}: {e}"),
        )


# ──────────────────────────────────────────────────────────────────────────────
# Step builder
# ──────────────────────────────────────────────────────────────────────────────
def _py_compile_steps(root: Path, timeout_s: float) -> list[Step]:
    # Only compile key scripts used in go-live guardrail
    targets = [
        root / "scripts" / "preflight.py",
        root / "scripts" / "smoke_init.py",
        root / "scripts" / "smoke_core.py",
        root / "scripts" / "smoke_lightning.py",
        root / "scripts" / "smoke_pg_only.py",
        root / "scripts" / "smoke_run_local.py",
        root / "scripts" / "check_router.py",
        root / "scripts" / "backtest.py",
    ]
    out: list[Step] = []
    for p in targets:
        if p.exists():
            out.append(
                Step(
                    name=f"compile:{p.name}",
                    cmd=[sys.executable, "-m", "py_compile", str(p)],
                    timeout_s=max(10.0, timeout_s),
                )
            )
    return out


def _deps_steps(timeout_s: float) -> list[Step]:
    # import checks via a subprocess so we don't poison this process state
    mods = [
        "httpx",
        "sqlalchemy",
        # asyncpg might be optional if not using PG in some modes, but smoke_pg_only will need it
        "asyncpg",
    ]
    code = (
        f"import importlib; mods={mods!r}; " + "missing=[]; "
        "[(missing.append(m),None)[1] if importlib.util.find_spec(m) is None else None for m in mods]; "
        "print('missing=' + ','.join(missing)); raise SystemExit(0 if not missing else 2)"
    )
    return [
        Step(
            name="deps:imports",
            cmd=[sys.executable, "-c", code],
            timeout_s=max(10.0, timeout_s),
            optional=False,
        )
    ]


def _smoke_suite_steps(
    root: Path, timeout_s: float, skip_network: bool, skip_pg: bool, skip_lightning: bool
) -> list[Step]:
    steps: list[Step] = []

    # preflight (always)
    preflight = root / "scripts" / "preflight.py"
    if preflight.exists():
        steps.append(
            Step(name="preflight", cmd=[sys.executable, str(preflight)], timeout_s=timeout_s)
        )

    # smoke_init (no wallet required; resource init)
    smoke_init = root / "scripts" / "smoke_init.py"
    if smoke_init.exists():
        steps.append(
            Step(name="smoke_init", cmd=[sys.executable, str(smoke_init)], timeout_s=timeout_s)
        )

    # smoke_core (core_utils sanity)
    smoke_core = root / "scripts" / "smoke_core.py"
    if smoke_core.exists():
        steps.append(
            Step(name="smoke_core", cmd=[sys.executable, str(smoke_core)], timeout_s=timeout_s)
        )

    # lightning (network)
    smoke_light = root / "scripts" / "smoke_lightning.py"
    if smoke_light.exists() and (not skip_network) and (not skip_lightning):
        steps.append(
            Step(
                name="smoke_lightning", cmd=[sys.executable, str(smoke_light)], timeout_s=timeout_s
            )
        )
    elif smoke_light.exists():
        steps.append(
            Step(
                name="smoke_lightning(skipped)",
                cmd=[sys.executable, "-c", "print('skipped smoke_lightning')"],
                timeout_s=10.0,
                optional=True,
            )
        )

    # pg_only (network)
    smoke_pg = root / "scripts" / "smoke_pg_only.py"
    if smoke_pg.exists() and (not skip_network) and (not skip_pg):
        steps.append(
            Step(name="smoke_pg_only", cmd=[sys.executable, str(smoke_pg)], timeout_s=timeout_s)
        )
    elif smoke_pg.exists():
        steps.append(
            Step(
                name="smoke_pg_only(skipped)",
                cmd=[sys.executable, "-c", "print('skipped smoke_pg_only')"],
                timeout_s=10.0,
                optional=True,
            )
        )

    return steps


def _backtest_step(root: Path, timeout_s: float) -> Step | None:
    bt = root / "scripts" / "backtest.py"
    if not bt.exists():
        return None
    return Step(
        name="backtest_dry_run",
        cmd=[sys.executable, str(bt), "--dry-run"],
        timeout_s=max(20.0, timeout_s),
        optional=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Local go-live smoke runner (LOCK-IN).")
    ap.add_argument(
        "--timeout",
        type=float,
        default=float(_envf("SMOKE_TIMEOUT_S", 20.0)),
        help="Per-step timeout seconds.",
    )
    ap.add_argument(
        "--total-timeout",
        type=float,
        default=float(_envf("SMOKE_TOTAL_TIMEOUT_S", 0.0)),
        help="Total cap seconds (0 disables).",
    )
    ap.add_argument(
        "--json",
        action="store_true",
        default=_envb("SMOKE_JSON", False),
        help="Emit JSON to STDOUT.",
    )
    ap.add_argument(
        "--json-only",
        action="store_true",
        default=_envb("SMOKE_JSON_ONLY", False),
        help="Only JSON (no human logs).",
    )
    ap.add_argument(
        "--debug",
        action="store_true",
        default=_envb("SMOKE_DEBUG", False),
        help="Print sanitized child output.",
    )
    ap.add_argument(
        "--only",
        type=str,
        default="",
        help="Comma list of step name prefixes to run (e.g. preflight,smoke_core).",
    )
    ap.add_argument(
        "--skip-network",
        type=int,
        default=1 if _envb("SMOKE_SKIP_NETWORK", False) else 0,
        help="1 to skip network steps.",
    )
    ap.add_argument(
        "--skip-pg",
        type=int,
        default=1 if _envb("SMOKE_SKIP_PG", False) else 0,
        help="1 to skip pg smoke.",
    )
    ap.add_argument(
        "--skip-lightning",
        type=int,
        default=1 if _envb("SMOKE_SKIP_LIGHTNING", False) else 0,
        help="1 to skip lightning smoke.",
    )
    ap.add_argument(
        "--skip-backtest",
        type=int,
        default=1 if _envb("SMOKE_SKIP_BACKTEST", False) else 0,
        help="1 to skip backtest dry run.",
    )
    ap.add_argument(
        "--skip-compile",
        type=int,
        default=1 if _envb("SMOKE_SKIP_COMPILE", False) else 0,
        help="1 to skip compile checks.",
    )
    ap.add_argument(
        "--skip-deps",
        type=int,
        default=1 if _envb("SMOKE_SKIP_DEPS", False) else 0,
        help="1 to skip dependency import checks.",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    json_only = bool(args.json_only)
    emit_json = bool(args.json or args.json_only)
    debug = bool(args.debug)

    root = _repo_root()
    timeout_s = max(8.0, float(args.timeout))
    total_timeout_s = max(0.0, float(args.total_timeout))

    only_prefixes = [p.strip() for p in (args.only or "").split(",") if p.strip()]

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "step": "smoke_run_local",
        "repo_root": _sanitize(str(root)),
        "python": _sanitize(sys.version.split()[0]),
        "platform": _sanitize(platform.platform()),
        "args": {
            "timeout_s": float(timeout_s),
            "total_timeout_s": float(total_timeout_s),
            "skip_network": int(args.skip_network),
            "skip_pg": int(args.skip_pg),
            "skip_lightning": int(args.skip_lightning),
            "skip_backtest": int(args.skip_backtest),
            "skip_compile": int(args.skip_compile),
            "skip_deps": int(args.skip_deps),
            "only": only_prefixes,
        },
        "results": [],
        "errors": [],
        "warnings": [],
        "elapsed_ms": 0,
    }

    if not json_only:
        _hlog(f"repo_root={root}", json_only=False)
        _hlog(f"python={sys.version.split()[0]} platform={platform.platform()}", json_only=False)

    steps: list[Step] = []

    if int(args.skip_compile) == 0:
        steps.extend(_py_compile_steps(root, timeout_s))

    if int(args.skip_deps) == 0:
        steps.extend(_deps_steps(timeout_s))

    steps.extend(
        _smoke_suite_steps(
            root,
            timeout_s=timeout_s,
            skip_network=bool(int(args.skip_network)),
            skip_pg=bool(int(args.skip_pg)),
            skip_lightning=bool(int(args.skip_lightning)),
        )
    )

    if int(args.skip_backtest) == 0:
        bt = _backtest_step(root, timeout_s)
        if bt:
            steps.append(bt)

    # Apply --only filter by prefix match
    if only_prefixes:
        filtered: list[Step] = []
        for s in steps:
            if any(s.name.startswith(p) for p in only_prefixes):
                filtered.append(s)
        steps = filtered

    start_ms = _now_ms()

    def _should_stop_on_failure(step: Step) -> bool:
        # Optional steps don't stop the world.
        return not step.optional

    try:
        deadline_ms = (start_ms + int(total_timeout_s * 1000)) if total_timeout_s > 0 else 0

        for step in steps:
            # total timeout enforcement
            if deadline_ms and _now_ms() > deadline_ms:
                summary["errors"].append("total_timeout")
                break

            res = _run_step(step, json_only=json_only, debug=debug)
            summary["results"].append(
                {
                    "name": res.name,
                    "ok": int(bool(res.ok)),
                    "rc": int(res.rc),
                    "ms": int(res.ms),
                    # store truncated output for JSON (still sanitized)
                    "out": res.out[:3000] if res.out else "",
                    "err": res.err[:3000] if res.err else "",
                }
            )

            if not res.ok:
                msg = f"{step.name} failed rc={res.rc}"
                if step.optional:
                    summary["warnings"].append(msg)
                    if not json_only:
                        _hlog(f"WARN: {msg}", json_only=False)
                else:
                    summary["errors"].append(msg)
                    if not json_only:
                        _hlog(f"FAIL: {msg}", json_only=False)
                    if _should_stop_on_failure(step):
                        break

        summary["ok"] = len(summary["errors"]) == 0

    except KeyboardInterrupt:
        if emit_json:
            summary["errors"].append("keyboard_interrupt")
            summary["ok"] = False
            summary["elapsed_ms"] = int(_now_ms() - start_ms)
            print(json.dumps(summary, ensure_ascii=False, default=str))
        return RC_CTRL_C

    finally:
        summary["elapsed_ms"] = int(_now_ms() - start_ms)

        # Small hints (non-secret)
        all_txt = " ".join(summary["errors"] + summary["warnings"]).lower()
        if "deps:imports" in all_txt:
            summary["warnings"].append(
                "Missing deps. pip install -r requirements.txt (or ensure venv is active)."
            )
        if "smoke_pg_only" in all_txt and ("ssl" in all_txt or "certificate" in all_txt):
            summary["warnings"].append(
                "PG TLS verify issue: check sslmode/CA or PG_SSL_INSECURE=1 for debug only."
            )
        if "smoke_lightning" in all_txt and ("429" in all_txt or "rate" in all_txt):
            summary["warnings"].append(
                "Jupiter rate limiting: add/verify API key or reduce quote frequency."
            )

        if emit_json:
            print(json.dumps(summary, ensure_ascii=False, default=str))
        else:
            _hlog(
                ("OK" if summary["ok"] else "FAIL") + f" elapsed_ms={summary['elapsed_ms']}",
                json_only=json_only,
            )

    return RC_OK if summary["ok"] else RC_FAIL


if __name__ == "__main__":
    raise SystemExit(main())
