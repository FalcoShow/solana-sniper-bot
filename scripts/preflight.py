# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/preflight.py — God-Tier Preflight Gatekeeper (P0 LOCK-IN)

Preflight is the SINGLE gatekeeper for boot. It runs a deterministic plan of checks
and exits non-zero if required steps fail.

NEW (LOCK-IN): Config Gatekeeper
--------------------------------
This preflight now treats `validate_config(C)` from `config.py` as the single
authoritative configuration gatekeeper:
  - Preflight imports `from config import C, validate_config`
  - Runs `validate_config(C)` first
  - If config validation fails -> FAIL FAST (RC_REQUIRED_FAILED=2)
  - The result is emitted as the first `results[]` entry in JSON output.

Core guarantees
---------------
- Child checks are side-effect-free in the parent process:
    * network probes / infra checks run in subprocesses (python -m ...) or inline (python -c ...)
- Secret-safe output:
    * URL credential masking
    * header token masking
    * key=value secret masking
    * query-param masking
    * exact env secret value replacement
    * optional long-token masking (preserves Solana base58 pubkeys/mints)
- Deterministic exit codes:
    0   = pass
    1   = only optional steps failed AND PREFLIGHT_FAIL_ON_OPTIONAL=1
    2   = required step failed OR required canary cannot run OR CLI error
    130 = Ctrl-C
- Human logs -> STDERR, JSON summary -> STDOUT (when enabled).

Policy
------
- Reachability checks (DNS/RPC/Router/Jupiter/DB) run only when network is allowed:
    NO_NETWORK=1 or DISABLE_NETWORK=1 or SAFE_MODE=1 or DRYRUN=1  => network disallowed
- Additionally, preflight becomes policy-aware from check_env output:
    * If DISABLE_SCANNERS=1, scanner reachability steps are pruned.
    * If DISABLE_BUYS=1 and DISABLE_SELLS=1, router/jupiter reachability steps are pruned.
    * If check_env says db_required=0, DB probe is pruned.

DNS hardening (IMPORTANT)
-------------------------
- Render stays STRICT (DNS failures are CRITICAL).
- Local/dev defaults to SOFT (DNS failures won't hard-fail):
    * internal-only hosts are skipped (e.g., Render internal Postgres hostnames with no dots)
    * pass if at least PREFLIGHT_DNS_MIN_OK hosts resolve (default 1)
    * warnings/fix_hints included in JSON summary

Override knobs:
  PREFLIGHT_DNS_MODE=strict|soft|off
  PREFLIGHT_DNS_MIN_OK=1
  PREFLIGHT_DNS_ATTEMPTS=3
  PREFLIGHT_DNS_HOSTS=comma,separated,hosts,to,check
  PREFLIGHT_DNS_SKIP_INTERNAL=1 (default in soft mode)
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
import traceback
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def _load_repo_dotenv_best_effort() -> None:
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        repo = Path(__file__).resolve().parents[1]
        p = repo / ".env"
        if p.is_file():
            load_dotenv(p, override=False)


_load_repo_dotenv_best_effort()

# =============================================================================
# contract
# =============================================================================
SCRIPT_NAME = "scripts.preflight"
VERSION = "v12.0"

# =============================================================================
# exit codes (DO NOT CHANGE)
# =============================================================================
RC_OK = 0
RC_OPTIONAL_FAILED = 1
RC_REQUIRED_FAILED = 2
RC_CTRL_C = 130

# =============================================================================
# env parsing
# =============================================================================
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


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
    s = _envs(k, str(d)).replace("_", "")
    try:
        return int(float(s))
    except Exception:
        return int(d)


def _envf(k: str, d: float) -> float:
    s = _envs(k, str(d)).replace("_", "")
    try:
        return float(s)
    except Exception:
        return float(d)


def _parse_csv(s: str) -> list[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _dig(dct: Any, path: Sequence[str], default: Any = None) -> Any:
    cur: Any = dct
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return default if cur is None else cur


# =============================================================================
# repo root bootstrap (import reliability only)
# =============================================================================
def _bootstrap_repo_root() -> str:
    """
    Detect repo root via common markers and export REPO_ROOT.
    Never throws.
    """
    try:
        here = Path(__file__).resolve()
    except Exception:
        root_env = os.getenv("REPO_ROOT", "") or ""
        if root_env:
            os.environ.setdefault("REPO_ROOT", root_env)
        return root_env

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

    # Harmless: helps child module resolution if PYTHONPATH is not set.
    with contextlib.suppress(Exception):
        if root_s and root_s not in sys.path:
            sys.path.insert(0, root_s)
        src = root / "src"
        if src.is_dir():
            src_s = str(src)
            if src_s not in sys.path:
                sys.path.insert(0, src_s)

    return root_s


# =============================================================================
# policy: network allowed?
# =============================================================================
def _network_allowed() -> bool:
    if _envb("NO_NETWORK", False):
        return False
    if _envb("DISABLE_NETWORK", False):
        return False
    if _envb("SAFE_MODE", False):
        return False
    return not _envb("DRYRUN", False)


# =============================================================================
# Render detection (strict-by-default there)
# =============================================================================
def _render_like() -> bool:
    return (
        bool(_envs("RENDER", ""))
        or bool(_envs("RENDER_SERVICE_ID", ""))
        or bool(_envs("RENDER_SERVICE_NAME", ""))
    )


# =============================================================================
# DB DSN router (Render internal vs local external)
# =============================================================================
_DB_DSN_KEYS = ("DATABASE_URL", "POSTGRES_DSN", "PG_DSN", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL")


def _dsn_host(dsn: str) -> str:
    try:
        s = (dsn or "").strip()
        if not s:
            return ""
        if "://" not in s:
            s = "postgresql://" + s
        u = urlparse(s)
        return (u.hostname or "").strip()
    except Exception:
        return ""


def _is_ip(host: str) -> bool:
    h = (host or "").strip()
    if not h:
        return False
    try:
        socket.inet_pton(socket.AF_INET, h)
        return True
    except Exception:
        pass
    try:
        socket.inet_pton(socket.AF_INET6, h)
        return True
    except Exception:
        return False


def _looks_internal_host(host: str) -> bool:
    h = (host or "").strip()
    if not h:
        return False
    if _is_ip(h):
        return False
    # Render internal hostnames often have no dots.
    return "." not in h


def _ensure_sslmode_require(dsn: str) -> str:
    """
    If host endswith render.com and sslmode is missing, append sslmode=require.
    Safe no-op otherwise.
    """
    s = (dsn or "").strip()
    if not s:
        return ""
    try:
        raw = s if "://" in s else "postgresql://" + s
        u = urlparse(raw)
        host = (u.hostname or "").strip().lower()
        q = dict(parse_qsl(u.query, keep_blank_values=True))
        if host.endswith("render.com") and not q.get("sslmode"):
            q["sslmode"] = "require"
            rebuilt = urlunparse(
                (u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment)
            )
            return rebuilt if "://" in s else rebuilt.replace("postgresql://", "", 1)
    except Exception:
        pass
    return s


def _pick_canonical_db_dsn() -> tuple[str, str]:
    """
    Returns (dsn_value, source_key).
    Policy:
      - Render-like: prefer DATABASE_URL, then PG_DSN_INTERNAL, then POSTGRES_DSN, then PG_DSN, then PG_DSN_EXTERNAL
      - Local/dev: prefer PG_DSN_EXTERNAL, then POSTGRES_DSN, then DATABASE_URL, then PG_DSN, then PG_DSN_INTERNAL
      - Additionally: if local/dev and DATABASE_URL host looks internal-only and PG_DSN_EXTERNAL exists, pick external.
    """
    vals = {k: _envs(k, "").strip() for k in _DB_DSN_KEYS}
    render = _render_like()

    db_url = vals.get("DATABASE_URL", "")
    ext = vals.get("PG_DSN_EXTERNAL", "")

    if not render:
        if db_url and ext and _looks_internal_host(_dsn_host(db_url)):
            return ext, "PG_DSN_EXTERNAL"

    if render:
        for k in ("DATABASE_URL", "PG_DSN_INTERNAL", "POSTGRES_DSN", "PG_DSN", "PG_DSN_EXTERNAL"):
            v = vals.get(k, "")
            if v:
                return v, k
    else:
        for k in ("PG_DSN_EXTERNAL", "POSTGRES_DSN", "DATABASE_URL", "PG_DSN", "PG_DSN_INTERNAL"):
            v = vals.get(k, "")
            if v:
                return v, k

    return "", ""


def _db_router_env_overrides() -> tuple[dict[str, str], str]:
    """
    Child-process env overrides so strict check_env sees ONE canonical DSN only.
    We normalize by setting DATABASE_URL to the chosen DSN and blanking the others.
    """
    chosen, source = _pick_canonical_db_dsn()
    if not chosen:
        return {}, ""

    chosen = _ensure_sslmode_require(chosen)
    host = _dsn_host(chosen) or "unknown"

    overrides = {
        "DATABASE_URL": chosen,
        "POSTGRES_DSN": "",
        "PG_DSN": "",
        "PG_DSN_INTERNAL": "",
        "PG_DSN_EXTERNAL": "",
    }
    note = f"db_dsn_router: {source or 'unknown'} -> DATABASE_URL host={host}"
    return overrides, note


# =============================================================================
# DNS mode policy (strict on Render, soft on dev)
# =============================================================================
def _dns_mode() -> str:
    """
    Returns: "strict" | "soft" | "off"
    Default: strict on Render-like, else soft.
    Overrides:
      PREFLIGHT_DNS_MODE=strict|soft|off
      PREFLIGHT_DNS_STRICT=1 -> strict
      PREFLIGHT_DNS_OFF=1 -> off
    """
    if _envb("PREFLIGHT_DNS_OFF", False):
        return "off"
    if _envb("PREFLIGHT_DNS_STRICT", False):
        return "strict"

    raw = (_envs("PREFLIGHT_DNS_MODE", "") or "").strip().lower()
    if raw in {"0", "false", "off", "disable", "disabled", "none"}:
        return "off"
    if raw in {"soft", "warn", "optional", "relaxed"}:
        return "soft"
    if raw in {"strict", "hard", "required"}:
        return "strict"

    return "strict" if _render_like() else "soft"


# =============================================================================
# safe stderr/stdout writes (Windows-safe)
# =============================================================================
def _safe_write(stream, s: str) -> None:
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


def _safe_print_json(obj: dict[str, Any]) -> None:
    ensure_ascii = _envb("PREFLIGHT_JSON_ENSURE_ASCII", (os.name == "nt"))
    pretty = _envb("PREFLIGHT_JSON_PRETTY", False)
    try:
        s = json.dumps(obj, ensure_ascii=ensure_ascii, default=str, indent=(2 if pretty else None))
    except Exception:
        s = json.dumps({"ok": False, "error": "json_dump_failed"}, ensure_ascii=True)

    try:
        sys.stdout.write(s + "\n")
        sys.stdout.flush()
        return
    except Exception:
        with contextlib.suppress(Exception):
            b = (s + "\n").encode("utf-8", errors="replace")
            sys.stdout.buffer.write(b)  # type: ignore[attr-defined]
            sys.stdout.buffer.flush()  # type: ignore[attr-defined]


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write(sys.stderr, f"[preflight] {_ts()} {msg}")


# =============================================================================
# redaction / safety (aligned with check_env intent)
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)
_QP_SECRET_RE = re.compile(r"(?i)(password|pass|token|api_key|apikey|secret)=([^&\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")

_BASE58_ALPH = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

_REDACT_KEYS_CACHE: list[str] | None = None
_REDACT_KEYSET_CACHE: set[str] | None = None
_SECRET_VALUES_CACHE: list[str] | None = None


def _looks_like_base58_pubkey(s: str) -> bool:
    ss = (s or "").strip()
    if not (32 <= len(ss) <= 44):
        return False
    return all(ch in _BASE58_ALPH for ch in ss)


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
    )
    return any(n in kk for n in needles)


def _redact_keys_raw() -> list[str]:
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
        "SOL_RPC_URL",
        "RPC_URL",
        "SOL_RPC_URLS",
    ]
    extra = _parse_csv(_envs("PREFLIGHT_REDACT_KEYS", ""))
    out = base + extra
    seen: set[str] = set()
    uniq: list[str] = []
    for k in out:
        if k and k not in seen:
            uniq.append(k)
            seen.add(k)
    return uniq


def _refresh_redaction_cache() -> None:
    global _REDACT_KEYS_CACHE, _REDACT_KEYSET_CACHE, _SECRET_VALUES_CACHE

    keys = _redact_keys_raw()
    keyset = set(keys)

    for k in os.environ.keys():
        if _sensitive_key_name(k):
            keyset.add(k)

    keys = sorted(keyset)

    vals: list[str] = []
    for k in keys:
        v = os.getenv(k, "")
        if v:
            vv = str(v)
            if len(vv) >= 6:
                vals.append(vv)

    uniq_vals = sorted(set(vals), key=lambda s: (-len(s), s))
    _REDACT_KEYS_CACHE = keys
    _REDACT_KEYSET_CACHE = set(keys)
    _SECRET_VALUES_CACHE = uniq_vals


def _redact_keyset() -> set[str]:
    if _REDACT_KEYSET_CACHE is None:
        _refresh_redaction_cache()
    return _REDACT_KEYSET_CACHE or set()


def _secret_values() -> list[str]:
    if _SECRET_VALUES_CACHE is None:
        _refresh_redaction_cache()
    return _SECRET_VALUES_CACHE or []


def _mask_long_tokens_enabled() -> bool:
    return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)


def _mask_tokenish_match(m: re.Match) -> str:
    tok = m.group(0)
    if _looks_like_base58_pubkey(tok):
        return tok
    return "***"


def _sanitize(text: Any) -> str:
    if text is None:
        return ""
    s = str(text)

    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_HDR_RE.sub(r"\1***", s)
    s = _QP_SECRET_RE.sub(r"\1=***", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)

    for v in _secret_values():
        if v and v in s:
            s = s.replace(v, "***")

    if _mask_long_tokens_enabled():
        s = _LONG_TOKENISH_RE.sub(_mask_tokenish_match, s)

    return s


def _sanitize_json(obj: Any, *, depth: int = 0, max_depth: int = 12, max_items: int = 4000) -> Any:
    if depth > max_depth:
        return "***"

    if obj is None:
        return None

    if isinstance(obj, (bool, int, float)):
        return obj

    if isinstance(obj, str):
        return _sanitize(obj)

    if isinstance(obj, list):
        out_l: list[Any] = []
        for i, v in enumerate(obj):
            if i >= max_items:
                out_l.append("...truncated...")
                break
            out_l.append(
                _sanitize_json(v, depth=depth + 1, max_depth=max_depth, max_items=max_items)
            )
        return out_l

    if isinstance(obj, dict):
        out_d: dict[str, Any] = {}
        ks = list(obj.keys())
        for i, k in enumerate(ks):
            if i >= max_items:
                out_d["...truncated..."] = True
                break
            kk = str(k)
            v = obj.get(k)
            if kk.upper() in _redact_keyset() or _sensitive_key_name(kk):
                out_d[kk] = "***"
            else:
                out_d[kk] = _sanitize_json(
                    v, depth=depth + 1, max_depth=max_depth, max_items=max_items
                )
        return out_d

    return _sanitize(obj)


class _LiveSanitizer:
    def __init__(self, tail_chars: int = 4096):
        self._tail_chars = max(256, int(tail_chars))
        self._tail = ""

    def feed(self, chunk_text: str) -> str:
        raw = self._tail + (chunk_text or "")
        if not raw:
            return ""
        tail_s = _sanitize(self._tail)
        full_s = _sanitize(raw)
        out = full_s[len(tail_s) :] if full_s.startswith(tail_s) else full_s
        self._tail = raw[-self._tail_chars :]
        return out


# =============================================================================
# module detection + JSON extraction
# =============================================================================
def _module_exists(module_name: str) -> bool:
    try:
        return importlib.util.find_spec(module_name) is not None
    except Exception:
        return False


def _extract_json(text: str) -> dict[str, Any] | None:
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
            blob = ln[i : j + 1]
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
    modules: tuple[str, ...] = ()
    description: str = ""
    critical: bool = True
    timeout_s: float | None = None
    argv: tuple[str, ...] = ()
    env_overrides: dict[str, str] = field(default_factory=dict)
    optional_if_missing: bool = False
    skip_if_env_true: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    inline_py: str = ""

    def resolve_module(self) -> str | None:
        for m in self.modules:
            if _module_exists(m):
                return m
        return None


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
    attempts: int = 1
    warnings: list[str] = field(default_factory=list)
    fix_hints: list[str] = field(default_factory=list)
    child_json: dict[str, Any] | None = None
    stdout: str = ""
    stderr: str = ""

    def public(self, *, include_child_json: bool, include_stdio: bool) -> dict[str, Any]:
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
# subprocess runner
# =============================================================================
def _win_creationflags() -> int:
    # CREATE_NEW_PROCESS_GROUP (helps ctrl-c handling)
    return 0x00000200


def _inherit_pythonpath(env: dict[str, str]) -> None:
    root = (env.get("REPO_ROOT") or os.getenv("REPO_ROOT") or "").strip()
    if not root:
        return
    existing = (env.get("PYTHONPATH") or "").strip()
    parts = [p for p in existing.split(os.pathsep) if p] if existing else []
    if root not in parts:
        parts.insert(0, root)
    env["PYTHONPATH"] = os.pathsep.join(parts)


async def _kill_tree_windows(pid: int) -> None:
    if pid <= 0:
        return
    try:
        proc = await asyncio.create_subprocess_exec(
            "taskkill",
            "/PID",
            str(pid),
            "/T",
            "/F",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await asyncio.wait_for(proc.wait(), timeout=5.0)
    except Exception:
        return


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
                    _safe_write(sys.stderr, f"[{live_tag}] {out}".rstrip("\n"))

        remaining = cap_bytes - len(buf)
        if remaining > 0:
            buf.extend(chunk[:remaining])

    return bytes(buf)


async def _run_child(
    *,
    module_name: str = "",
    inline_py: str = "",
    timeout_s: float,
    argv: Sequence[str],
    env_overrides: dict[str, str],
    verbose: bool,
    json_only: bool,
    cap_bytes: int,
    cap_chars: int,
    live_tag: str,
) -> tuple[int, str, str, bool]:
    exe = sys.executable or "python"

    env = os.environ.copy()
    env.update({k: str(v) for k, v in (env_overrides or {}).items()})
    env.setdefault("PYTHONUNBUFFERED", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    if os.name == "nt":
        env.setdefault("PYTHONUTF8", "1")

    _inherit_pythonpath(env)

    cmd = [exe, "-c", inline_py] if inline_py else [exe, "-m", module_name, *list(argv)]

    kw: dict[str, Any] = {
        "stdout": asyncio.subprocess.PIPE,
        "stderr": asyncio.subprocess.PIPE,
        "env": env,
    }
    if os.name == "nt":
        kw["creationflags"] = _win_creationflags()
    else:
        kw["start_new_session"] = True

    timed_out = False
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, **kw)
    except NotImplementedError as e:
        err = f"NotImplementedError: {e} (Windows needs Proactor loop policy for subprocesses)"
        return RC_REQUIRED_FAILED, "", err, False
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        return RC_REQUIRED_FAILED, "", err, False

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

    out_raw = (out_b or b"").decode("utf-8", "ignore")
    err_raw = (err_b or b"").decode("utf-8", "ignore")

    if cap_chars > 0:
        if len(out_raw) > cap_chars:
            out_raw = out_raw[:cap_chars] + "..."
        if len(err_raw) > cap_chars:
            err_raw = err_raw[:cap_chars] + "..."

    if timed_out:
        if not err_raw:
            err_raw = f"timeout after {timeout_s:.0f}s"
        return 124, out_raw, err_raw, True

    rc = int(proc.returncode or 0)
    return rc, out_raw, err_raw, False


# =============================================================================
# retry + postcheck helpers
# =============================================================================
def _retry_policy() -> tuple[int, float, float, set[str]]:
    max_retries = max(0, _envi("PREFLIGHT_RETRY_MAX", 1))
    base_s = max(0.05, float(_envf("PREFLIGHT_RETRY_BASE_S", 0.6)))
    jitter_s = max(0.0, float(_envf("PREFLIGHT_RETRY_JITTER_S", 0.4)))
    steps = set(
        _parse_csv(
            _envs(
                "PREFLIGHT_RETRY_STEPS",
                "check_dns,check_rpc,check_router,check_jupiter,pg_probe,healthcheck",
            )
        )
    )
    return max_retries, base_s, jitter_s, steps


async def _sleep_backoff(attempt_idx: int, base_s: float, jitter_s: float) -> None:
    delay = base_s * (2 ** max(0, attempt_idx - 1))
    delay += random.random() * jitter_s
    await asyncio.sleep(min(delay, 8.0))


def _coerce_list(x: Any) -> list[str]:
    if isinstance(x, list):
        return [str(i) for i in x if str(i).strip()]
    if isinstance(x, tuple):
        return [str(i) for i in x if str(i).strip()]
    return []


def _child_ok(child: dict[str, Any] | None, rc: int) -> bool:
    if isinstance(child, dict) and "ok" in child:
        return bool(child.get("ok"))
    return rc == 0


def _postcheck(
    name: str, rc: int, out_s: str, err_s: str, child: dict[str, Any] | None
) -> tuple[bool, list[str], list[str]]:
    ok = _child_ok(child, rc)
    warnings: list[str] = []
    hints: list[str] = []

    if isinstance(child, dict):
        warnings.extend(_coerce_list(child.get("warnings")))
        hints.extend(_coerce_list(child.get("fix_hints") or child.get("hints")))

    blob_l = ((out_s or "") + "\n" + (err_s or "")).lower()

    if not ok:
        if "missing env" in blob_l or "missing environment" in blob_l:
            hints.append("Missing environment variables. Verify Render Secrets and .env wiring.")
        if "timeout" in blob_l or rc == 124:
            hints.append("Timeout. Increase PREFLIGHT_STEP_TIMEOUT_S or fix endpoint latency.")
        if (
            ("getaddrinfo" in blob_l)
            or ("could not resolve" in blob_l)
            or ("name or service not known" in blob_l)
        ):
            hints.append("DNS/network issue. Verify DNS + outbound access on this machine/service.")
        if "sslmode" in blob_l and "require" in blob_l:
            hints.append("Postgres on Render usually needs sslmode=require (normalize DSN).")
        if "jup" in blob_l and "401" in blob_l:
            hints.append("Jupiter auth failed. Set JUP_PRO_KEY/JUP_API_KEY (Render secret).")

    warnings = list(dict.fromkeys([w for w in warnings if w]))[:12]
    hints = list(dict.fromkeys([h for h in hints if h]))[:16]
    return ok, warnings, hints


# =============================================================================
# CONFIG GATEKEEPER (NEW): validate_config(C) is the single fail-fast gate
# =============================================================================
def _interpret_validate_config_result(
    res: Any,
) -> tuple[bool, list[str], list[str], dict[str, Any]]:
    """
    Accept a wide range of validate_config(C) return styles.
    Returns: (ok, errors, warnings, extra_json)
    """
    errors: list[str] = []
    warnings: list[str] = []
    extra: dict[str, Any] = {}

    if res is None:
        return True, [], [], {}

    if isinstance(res, bool):
        return bool(res), [], [], {}

    if isinstance(res, dict):
        ok = bool(res.get("ok")) if "ok" in res else True
        errors = _coerce_list(res.get("errors") or res.get("error") or [])
        warnings = _coerce_list(res.get("warnings") or res.get("warn") or [])
        if "ok" not in res:
            ok = len(errors) == 0
        extra = {
            k: v for k, v in res.items() if k not in {"ok", "errors", "error", "warnings", "warn"}
        }
        return ok, errors, warnings, extra

    if isinstance(res, (list, tuple)):
        seq = list(res)
        if len(seq) == 0:
            return True, [], [], {}
        if isinstance(seq[0], bool):
            ok = bool(seq[0])
            errors = _coerce_list(seq[1]) if len(seq) >= 2 else []
            warnings = _coerce_list(seq[2]) if len(seq) >= 3 else []
            extra = {"raw": seq[3:]} if len(seq) > 3 else {}
            return ok if "ok" else ok, errors, warnings, extra
        # assume it's a list of errors
        errors = [str(x) for x in seq if str(x).strip()]
        return (len(errors) == 0), errors, [], {}

    if isinstance(res, str):
        msg = res.strip()
        if not msg:
            return True, [], [], {}
        return False, [msg], [], {}

    # fallback: treat as ok, but record
    return (
        True,
        [],
        [f"validate_config returned unsupported type: {type(res).__name__}"],
        {"returned_type": type(res).__name__},
    )


def _config_gatekeeper(*, json_only: bool) -> StepResult:
    """
    Runs:
        from config import C, validate_config
        validate_config(C)
    Fail-fast semantics:
      - import failure or thrown exception => FAIL (critical)
      - returned errors => FAIL (critical)
    """
    t0 = time.monotonic()
    name = "validate_config"
    critical = True

    # Best-effort relax knobs for offline preflight runs (does NOT weaken production policy unless env already set)
    if not _network_allowed():
        os.environ.setdefault("SKIP_DSN_REQUIRED", "1")
        os.environ.setdefault("DISABLE_SCANNERS", "1")
        os.environ.setdefault("DISABLE_BUYS", "1")
        os.environ.setdefault("DISABLE_SELLS", "1")

    try:
        # REQUIRED BY USER: explicit wiring
        from config import C, validate_config  # type: ignore
    except Exception as e:
        dt = time.monotonic() - t0
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        msg = _sanitize(f"{type(e).__name__}: {e}")
        hints = ["config.py import failed. Fix module imports / PYTHONPATH / dependency wiring."]
        child = _sanitize_json(
            {"ok": False, "error": msg, "trace": _sanitize(tb)[-2400:], "fix_hints": hints}
        )
        _hlog(f"CONFIG FAIL: {msg}", json_only=json_only)
        return StepResult(
            name=name,
            module="config",
            ok=False,
            critical=critical,
            rc=1,
            seconds=float(dt),
            reason=msg,
            warnings=[],
            fix_hints=hints,
            attempts=1,
            child_json=child if isinstance(child, dict) else None,
        )

    try:
        res = validate_config(C)  # type: ignore[misc]
    except Exception as e:
        dt = time.monotonic() - t0
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        msg = _sanitize(f"{type(e).__name__}: {e}")
        hints = ["validate_config(C) raised. Fix config validation logic or env parsing."]
        child = _sanitize_json(
            {"ok": False, "error": msg, "trace": _sanitize(tb)[-2400:], "fix_hints": hints}
        )
        _hlog(f"CONFIG FAIL: {msg}", json_only=json_only)
        return StepResult(
            name=name,
            module="config.validate_config",
            ok=False,
            critical=critical,
            rc=1,
            seconds=float(dt),
            reason=msg,
            warnings=[],
            fix_hints=hints,
            attempts=1,
            child_json=child if isinstance(child, dict) else None,
        )

    ok, errors, warnings, extra = _interpret_validate_config_result(res)
    dt = time.monotonic() - t0

    fix_hints: list[str] = []
    if not ok:
        low = "\n".join(errors).lower()
        if "missing" in low and ("env" in low or "environment" in low or "variable" in low):
            fix_hints.append(
                "Missing env vars. Verify .env + Render Secrets match config contract."
            )
        if "dsn" in low or "database" in low or "postgres" in low:
            fix_hints.append(
                "Database config issue. Verify DATABASE_URL / PG_DSN_* selection and sslmode=require on Render."
            )
        if "redis" in low:
            fix_hints.append(
                "Redis config issue. Verify REDIS_URL/REDIS_TLS_URL and network policy."
            )
    # cap
    errors = list(dict.fromkeys([_sanitize(x) for x in errors if str(x).strip()]))[:80]
    warnings = list(dict.fromkeys([_sanitize(x) for x in warnings if str(x).strip()]))[:40]
    fix_hints = list(dict.fromkeys([_sanitize(x) for x in fix_hints if str(x).strip()]))[:20]

    child_payload = {
        "ok": bool(ok),
        "errors": errors,
        "warnings": warnings,
        "fix_hints": fix_hints,
        "extra": extra,
    }
    child_payload_s = _sanitize_json(child_payload)

    if ok:
        _hlog("CONFIG ok=1 (validate_config passed)", json_only=json_only)
        return StepResult(
            name=name,
            module="config.validate_config",
            ok=True,
            critical=critical,
            rc=0,
            seconds=float(dt),
            warnings=warnings,
            fix_hints=fix_hints,
            attempts=1,
            child_json=child_payload_s if isinstance(child_payload_s, dict) else None,
        )

    reason = errors[0][:240] if errors else "config_invalid"
    _hlog(f"CONFIG ok=0 (validate_config failed): {reason}", json_only=json_only)
    return StepResult(
        name=name,
        module="config.validate_config",
        ok=False,
        critical=critical,
        rc=1,
        seconds=float(dt),
        reason=reason,
        warnings=warnings,
        fix_hints=fix_hints,
        attempts=1,
        child_json=child_payload_s if isinstance(child_payload_s, dict) else None,
    )


# =============================================================================
# plan builder
# =============================================================================
def _profile_default() -> str:
    p = _envs("PREFLIGHT_PROFILE", "deploy").lower().strip()
    return p if p in {"fast", "deploy", "full"} else "deploy"


def _build_registry(step_timeout_s: float) -> list[StepSpec]:
    t = float(step_timeout_s)

    dns_inline = r"""
import json, os, re, socket, sys, time
from urllib.parse import urlparse

def _envs(k, d=""):
    v = os.getenv(k)
    return (v if v is not None else d) or ""

def _envi(k, d):
    s = (_envs(k, str(d)) or str(d)).replace("_","").strip()
    try:
        return int(float(s))
    except Exception:
        return int(d)

def _envb(k, d=False):
    v = (_envs(k, "") or "").strip().lower()
    if v == "":
        return bool(d)
    return v in {"1","true","yes","y","on","t"}

def _is_ip(host):
    try:
        socket.inet_pton(socket.AF_INET, host)
        return True
    except Exception:
        pass
    try:
        socket.inet_pton(socket.AF_INET6, host)
        return True
    except Exception:
        return False

def _host_from_url(u):
    try:
        if not u:
            return ""
        uu = u.strip()
        if "://" not in uu:
            uu = "https://" + uu
        return (urlparse(uu).hostname or "").strip()
    except Exception:
        return ""

def _looks_internal(host):
    h = (host or "").strip()
    if not h:
        return False
    if _is_ip(h):
        return False
    if "." not in h:
        return True
    if h.endswith(".internal") or ".internal." in h:
        return True
    return False

def _uniq(seq):
    seen=set(); out=[]
    for x in seq:
        x=(x or "").strip()
        if x and x not in seen:
            out.append(x); seen.add(x)
    return out

mode_raw = (_envs("PREFLIGHT_DNS_MODE","") or "").strip().lower()
render_like = _envb("RENDER", False) or bool(_envs("RENDER_SERVICE_ID","")) or bool(_envs("RENDER_SERVICE_NAME",""))
if mode_raw in {"", "auto"}:
    mode = "strict" if render_like else "soft"
elif mode_raw in {"0","false","off","disable","disabled","none"}:
    mode = "off"
elif mode_raw in {"soft","warn","optional","relaxed"}:
    mode = "soft"
else:
    mode = "strict"

attempts = max(1, _envi("PREFLIGHT_DNS_ATTEMPTS", 3 if os.name=="nt" else 2))
min_ok_env = _envi("PREFLIGHT_DNS_MIN_OK", -1)

skip_internal = _envb("PREFLIGHT_DNS_SKIP_INTERNAL", (mode == "soft"))

hosts = []
csv = (_envs("PREFLIGHT_DNS_HOSTS","") or "").strip()
if csv:
    for h in re.split(r"[;,]", csv):
        h=h.strip()
        if h:
            hosts.append(h)

for k in ["SOL_RPC_URL","RPC_URL","SOL_RPC_URLS","REDIS_URL","REDIS_TLS_URL","DATABASE_URL","POSTGRES_DSN","PG_DSN","PNL_DSN","JUP_BASE_URL","JUPITER_SWAP_API"]:
    raw = (_envs(k,"") or "").strip()
    if not raw:
        continue
    if k.endswith("_URLS"):
        for part in re.split(r"[;,]", raw):
            h = _host_from_url(part.strip())
            if h:
                hosts.append(h)
    else:
        h = _host_from_url(raw)
        if h:
            hosts.append(h)

if not hosts:
    hosts = ["api.jup.ag","public.jupiterapi.com"]

hosts = _uniq(hosts)

skipped=[]
filtered=[]
for h in hosts:
    if skip_internal and _looks_internal(h):
        skipped.append({"host":h,"reason":"internal_host_skipped"})
        continue
    filtered.append(h)

hosts = filtered

if mode == "off":
    print(json.dumps({
        "ok": True,
        "mode": mode,
        "min_ok": 0,
        "attempts": attempts,
        "hosts": [],
        "skipped": skipped,
        "warnings": ["DNS check disabled (PREFLIGHT_DNS_MODE=off)."],
        "fix_hints": []
    }, ensure_ascii=True))
    sys.exit(0)

results=[]
ok_count=0
fail_count=0
warnings=[]
fix_hints=[]

def resolve_once(host):
    infos = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    ips = []
    for it in infos:
        if not it or len(it) < 5:
            continue
        sockaddr = it[4]
        if sockaddr and isinstance(sockaddr, tuple) and sockaddr[0]:
            ips.append(str(sockaddr[0]))
    out=[]
    seen=set()
    for ip in ips:
        if ip not in seen:
            out.append(ip); seen.add(ip)
    return out[:10]

for h in hosts:
    t0=time.time()
    last_err=""
    ips=[]
    host_ok=False

    for a in range(1, attempts+1):
        try:
            ips = resolve_once(h)
            if ips:
                host_ok=True
                break
        except Exception as e:
            last_err=str(e)[:260]
        if a < attempts:
            time.sleep(min(0.2 * a, 0.7))

    sec = round(time.time()-t0, 3)
    if host_ok:
        ok_count += 1
        results.append({"host":h,"ok":True,"ips":ips,"attempts":a,"sec":sec})
    else:
        fail_count += 1
        results.append({"host":h,"ok":False,"err":last_err or "resolution_failed","attempts":attempts,"sec":sec})

if min_ok_env >= 0:
    min_ok = min_ok_env
else:
    min_ok = len(hosts) if mode == "strict" else 1

overall_ok = (ok_count >= max(0, min_ok))

if not overall_ok:
    fix_hints.append("DNS/network issue. Verify DNS + outbound access on this machine/service.")
else:
    if fail_count > 0:
        bad = [r["host"] for r in results if not r.get("ok")]
        warnings.append(f"DNS partial failures ({fail_count}/{len(results)}): {', '.join(bad[:6])}" + ("..." if len(bad)>6 else ""))

print(json.dumps({
    "ok": bool(overall_ok),
    "mode": mode,
    "render_like": bool(render_like),
    "min_ok": int(min_ok),
    "attempts": int(attempts),
    "ok_count": int(ok_count),
    "fail_count": int(fail_count),
    "hosts": results,
    "skipped": skipped,
    "warnings": warnings,
    "fix_hints": fix_hints,
}, ensure_ascii=True))
"""

    pg_inline = r"""
import asyncio, json, os, socket, time
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

def _envs(k, d=""):
    v = os.getenv(k)
    s = (v if v is not None else d) or ""
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s

def _mask_dsn(dsn: str) -> str:
    try:
        s = dsn.strip()
        if "://" not in s:
            s = "postgresql://" + s
        u = urlparse(s)
        host = u.hostname or ""
        port = u.port
        netloc = host + (f":{port}" if port else "")
        return urlunparse((u.scheme, netloc, u.path, u.params, u.query, u.fragment))
    except Exception:
        return "postgresql://***"

def _clean_dsn_and_ssl(dsn: str):
    raw = dsn.strip()
    if "://" not in raw:
        raw = "postgresql://" + raw
    u = urlparse(raw)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    sslmode = (q.pop("sslmode", "") or q.pop("ssl_mode", "") or "").strip().lower()
    host = (u.hostname or "").strip().lower()
    if host.endswith("render.com") and not sslmode:
        sslmode = "require"
    ssl_required = sslmode in {"require", "verify-ca", "verify-full"}
    cleaned = urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))
    return cleaned, ssl_required, (sslmode or "")

async def _amain():
    dsn = (
        _envs("DATABASE_URL","")
        or _envs("POSTGRES_DSN","")
        or _envs("PG_DSN","")
        or _envs("PG_DSN_EXTERNAL","")
        or _envs("PG_DSN_INTERNAL","")
    )

    if not dsn:
        print(json.dumps({
            "ok": False,
            "exit_code": 1,
            "error": "missing_dsn",
            "fix_hints": ["Set DATABASE_URL (or POSTGRES_DSN). For local runs with Render, PG_DSN_EXTERNAL is recommended."]
        }, ensure_ascii=True))
        return 1

    cleaned, ssl_required, sslmode = _clean_dsn_and_ssl(dsn)
    u = urlparse(cleaned if "://" in cleaned else "postgresql://" + cleaned)
    host = u.hostname or ""
    port = int(u.port or 5432)

    ips = []
    dns_ok = 0
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        ips = []
        for it in infos:
            if it and len(it) >= 5 and it[4] and isinstance(it[4], tuple) and it[4][0]:
                ips.append(str(it[4][0]))
        seen=set(); uniq=[]
        for ip in ips:
            if ip not in seen:
                uniq.append(ip); seen.add(ip)
        ips = uniq[:10]
        dns_ok = 1 if ips else 0
    except Exception:
        ips = []
        dns_ok = 0

    tcp_ok = 0
    tcp_err = ""
    try:
        s = socket.create_connection((host, port), timeout=4.0)
        s.close()
        tcp_ok = 1
    except Exception as e:
        tcp_ok = 0
        tcp_err = str(e)[:240]

    t0 = time.time()
    conn_ok = 0
    err = ""
    try:
        import asyncpg
        conn = await asyncpg.connect(cleaned, timeout=12.0, ssl=(True if ssl_required else None))
        await conn.fetchval("select 1;")
        await conn.close()
        conn_ok = 1
    except Exception as e:
        conn_ok = 0
        err = f"{type(e).__name__}: {e}"

    sec = round(time.time() - t0, 3)
    ok = bool(conn_ok)

    fix_hints = []
    if not ok:
        if "password authentication failed" in err.lower():
            fix_hints.append("DB auth failed. Verify username/password in DSN (Render credentials).")
        if "no route" in err.lower() or "timed out" in err.lower():
            fix_hints.append("Network timeout. Verify outbound access / firewall policy.")
        if "ssl" in err.lower() and not ssl_required:
            fix_hints.append("Try adding sslmode=require (Render Postgres typically requires SSL).")
        if not dns_ok:
            fix_hints.append("DNS failed. Use the external Render hostname locally (PG_DSN_EXTERNAL) or fix resolver.")
        if not tcp_ok and tcp_err:
            fix_hints.append("TCP connect failed. Verify host/port and network policy.")

    print(json.dumps({
        "ok": ok,
        "exit_code": (0 if ok else 1),
        "driver": "asyncpg",
        "dsn": _mask_dsn(dsn),
        "host": host,
        "port": port,
        "sslmode": sslmode,
        "ssl_enabled": (1 if ssl_required else 0),
        "ips": ips,
        "tcp_probe_ok": tcp_ok,
        "tcp_probe": ("ok" if tcp_ok else tcp_err or "failed"),
        "sec": sec,
        "error": ("" if ok else err),
        "fix_hints": fix_hints,
    }, ensure_ascii=True))
    return 0 if ok else 1

if __name__ == "__main__":
    raise SystemExit(asyncio.run(_amain()))
"""

    return [
        StepSpec(
            name="check_env",
            modules=("scripts.check_env",),
            description="Hard gate: env/contract sanity (secret-safe).",
            critical=True,
            timeout_s=min(t, 75.0),
            argv=("--json-only", "--validate-env"),
            tags=("core", "required"),
        ),
        StepSpec(
            name="check_dns",
            inline_py=dns_inline,
            description="DNS gate (Render strict; local soft by default).",
            critical=True,  # tuned at runtime
            timeout_s=min(t, 30.0),
            tags=("net", "required"),
        ),
        StepSpec(
            name="smoke_init",
            modules=("scripts.smoke_init",),
            description="Import/wiring smoke (fast).",
            critical=True,
            timeout_s=min(t, 45.0),
            argv=("--json-only",),
            optional_if_missing=True,
            tags=("core",),
        ),
        StepSpec(
            name="check_rpc",
            modules=("scripts.check_rpc",),
            description="RPC sanity (reachability).",
            critical=True,
            timeout_s=min(t, 75.0),
            argv=("--json-only",),
            optional_if_missing=True,
            tags=("rpc", "required"),
        ),
        StepSpec(
            name="check_router",
            modules=("scripts.check_router",),
            description="Router quote sanity (reachability).",
            critical=True,
            timeout_s=min(t, 75.0),
            argv=("--json-only",),
            optional_if_missing=True,
            tags=("router", "required"),
        ),
        StepSpec(
            name="check_jupiter",
            modules=("scripts.check_jupiter",),
            description="Jupiter API sanity (auth/quote/swap).",
            critical=True,
            timeout_s=min(t, 75.0),
            argv=("--json-only",),
            optional_if_missing=True,
            tags=("jupiter", "required"),
        ),
        StepSpec(
            name="pg_probe",
            inline_py=pg_inline,
            description="DB connectivity (asyncpg probe; avoids SQLAlchemy/greenlet).",
            critical=True,
            timeout_s=min(t, 90.0),
            tags=("db", "required"),
        ),
        StepSpec(
            name="healthcheck",
            modules=("scripts.healthcheck",),
            description="Infra probes (optional).",
            critical=False,
            timeout_s=min(t, 60.0),
            argv=("--json-only",),
            optional_if_missing=True,
            tags=("optional",),
        ),
    ]


def _profile_select(profile: str, registry: list[StepSpec]) -> list[StepSpec]:
    profile = (profile or "deploy").lower().strip()
    if profile not in {"fast", "deploy", "full"}:
        profile = "deploy"

    names_fast = {"check_env", "smoke_init"}
    names_deploy = {
        "check_env",
        "check_dns",
        "smoke_init",
        "check_rpc",
        "check_router",
        "check_jupiter",
        "pg_probe",
        "healthcheck",
    }
    names_full = set(names_deploy)

    wanted = (
        names_full if profile == "full" else (names_fast if profile == "fast" else names_deploy)
    )
    return [s for s in registry if s.name in wanted]


def _initial_policy_prune(plan: list[StepSpec]) -> tuple[list[StepSpec], list[dict[str, Any]]]:
    pruned: list[StepSpec] = []
    skips: list[dict[str, Any]] = []

    net_allowed = _network_allowed()
    net_steps = {
        "check_dns",
        "check_rpc",
        "check_router",
        "check_jupiter",
        "pg_probe",
        "healthcheck",
    }

    if not net_allowed:
        os.environ.setdefault("SKIP_DSN_REQUIRED", "1")

    for s in plan:
        if (not net_allowed) and (s.name in net_steps):
            skips.append({"name": s.name, "reason": "policy_skip:no_network_or_dryrun"})
            continue

        env_skip = None
        for k in s.skip_if_env_true:
            if _envb(k, False):
                env_skip = k
                break
        if env_skip:
            skips.append({"name": s.name, "reason": f"policy_skip:{env_skip}=1"})
            continue

        if s.inline_py:
            pruned.append(s)
            continue

        resolved = s.resolve_module()
        if resolved is None and s.optional_if_missing:
            skips.append({"name": s.name, "reason": "missing_module"})
            continue

        pruned.append(s)

    return pruned, skips


def _policy_prune_after_check_env(
    plan: list[StepSpec], check_env_json: dict[str, Any] | None
) -> tuple[list[StepSpec], list[dict[str, Any]]]:
    pruned: list[StepSpec] = []
    skips: list[dict[str, Any]] = []

    flags = _dig(check_env_json, ["info", "flags"], {}) if isinstance(check_env_json, dict) else {}
    policy = (
        _dig(check_env_json, ["info", "policy"], {}) if isinstance(check_env_json, dict) else {}
    )

    disable_scanners = int(flags.get("DISABLE_SCANNERS", 0) or 0) == 1
    disable_buys = int(flags.get("DISABLE_BUYS", 0) or 0) == 1
    disable_sells = int(flags.get("DISABLE_SELLS", 0) or 0) == 1
    db_required = int(policy.get("db_required", 0) or 0) == 1

    for s in plan:
        if s.name == "pg_probe" and (not db_required):
            skips.append({"name": s.name, "reason": "check_env_policy:db_required=0"})
            continue

        if disable_scanners and s.name in {"check_rpc"}:
            skips.append({"name": s.name, "reason": "check_env_policy:DISABLE_SCANNERS=1"})
            continue

        if disable_buys and disable_sells and s.name in {"check_router", "check_jupiter"}:
            skips.append(
                {"name": s.name, "reason": "check_env_policy:DISABLE_BUYS=1 & DISABLE_SELLS=1"}
            )
            continue

        pruned.append(s)

    return pruned, skips


# =============================================================================
# step runner
# =============================================================================
async def _run_step(
    spec: StepSpec,
    *,
    default_timeout_s: float,
    remaining_total_s: float | None,
    verbose: bool,
    json_only: bool,
    strict: bool,
    include_child_json: bool,
    include_stdio: bool,
) -> StepResult:
    t0 = time.monotonic()

    module_name = spec.resolve_module() if not spec.inline_py else ""
    if (not spec.inline_py) and (not module_name):
        dt = time.monotonic() - t0
        if spec.optional_if_missing:
            _hlog(
                f"STEP skip : {spec.name} (missing modules {list(spec.modules)})",
                json_only=json_only,
            )
            return StepResult(
                spec.name,
                spec.modules[0] if spec.modules else "",
                True,
                spec.critical,
                127,
                float(dt),
                skipped=True,
                reason="missing_module",
            )
        _hlog(f"STEP FAIL : {spec.name} missing modules {list(spec.modules)}", json_only=json_only)
        return StepResult(
            spec.name,
            spec.modules[0] if spec.modules else "",
            False,
            spec.critical,
            127,
            float(dt),
            skipped=True,
            reason="missing_module",
        )

    step_timeout = float(spec.timeout_s if spec.timeout_s is not None else default_timeout_s)
    step_timeout = max(8.0, step_timeout)
    if remaining_total_s is not None:
        step_timeout = min(step_timeout, max(1.0, float(remaining_total_s)))

    need_json_extract = bool(
        include_child_json or spec.name in {"check_env", "check_dns", "pg_probe"}
    )

    cap_bytes = max(8192, _envi("PREFLIGHT_MAX_BYTES", 196608))
    cap_chars_raw = _envi("PREFLIGHT_MAX_OUTPUT", 3500)

    if need_json_extract:
        cap_bytes = max(cap_bytes, 1048576)
        cap_chars = 0
    else:
        cap_chars = max(500, int(cap_chars_raw))

    env_overrides = dict(spec.env_overrides or {})
    argv = list(spec.argv or ())

    if spec.name in {"check_env", "pg_probe"} or ("db" in (spec.tags or ())):
        db_over, db_note = _db_router_env_overrides()
        if db_note:
            _hlog(db_note, json_only=json_only)
        env_overrides = {**db_over, **env_overrides}

    if spec.name == "check_env":
        if "--json-only" not in argv:
            argv.append("--json-only")
        env_overrides.setdefault("CHECK_ENV_JSON", "1")
        env_overrides.setdefault("CHECK_ENV_JSON_ONLY", "1")
        if strict and "--strict" not in argv:
            argv.append("--strict")

        if not _network_allowed():
            # offline policy: preflight should be runnable without live secrets
            env_overrides.setdefault("SKIP_DSN_REQUIRED", "1")
            env_overrides.setdefault("DISABLE_SCANNERS", "1")
            env_overrides.setdefault("DISABLE_BUYS", "1")
            env_overrides.setdefault("DISABLE_SELLS", "1")

        cfg_mod = _envs("CHECK_ENV_CONFIG_MODULE", "").strip()
        if cfg_mod and ("--config-module" not in " ".join(argv)):
            argv.append(f"--config-module={cfg_mod}")

    _hlog(
        f"STEP start: {spec.name} ({'inline' if spec.inline_py else module_name}) timeout={int(step_timeout)}s argv={argv}",
        json_only=json_only,
    )

    max_retries, base_s, jitter_s, retry_steps = _retry_policy()
    attempts = 0

    while True:
        attempts += 1
        rc, out_raw, err_raw, _timed_out = await _run_child(
            module_name=module_name or "",
            inline_py=spec.inline_py or "",
            timeout_s=step_timeout,
            argv=argv,
            env_overrides=env_overrides,
            verbose=verbose,
            json_only=json_only,
            cap_bytes=cap_bytes,
            cap_chars=cap_chars,
            live_tag=spec.name,
        )

        child_obj_raw: dict[str, Any] | None = None
        if include_child_json or spec.name in {"check_env", "check_dns", "pg_probe"}:
            child_obj_raw = _extract_json(out_raw) or _extract_json(err_raw)

        out_s = _sanitize(out_raw).strip()
        err_s = _sanitize(err_raw).strip()

        child_obj: dict[str, Any] | None = None
        if isinstance(child_obj_raw, dict):
            child_obj = _sanitize_json(child_obj_raw)

        ok, warnings, hints = _postcheck(spec.name, rc, out_s, err_s, child_obj)

        if ok:
            dt = time.monotonic() - t0
            _hlog(f"STEP done : {spec.name} ok=1 rc={rc} sec={dt:.2f}", json_only=json_only)
            return StepResult(
                name=spec.name,
                module=("inline" if spec.inline_py else (module_name or "")),
                ok=True,
                critical=spec.critical,
                rc=int(rc),
                seconds=float(dt),
                warnings=warnings,
                fix_hints=hints,
                attempts=attempts,
                child_json=(child_obj if isinstance(child_obj, dict) else None),
                stdout=(out_s if include_stdio else ""),
                stderr=(err_s if include_stdio else ""),
            )

        transient = (rc == 124) or ("timeout" in (err_s or "").lower())
        can_retry = (
            (not strict)
            and (spec.name in retry_steps)
            and (max_retries > 0)
            and (attempts <= max_retries)
            and transient
        )
        if can_retry:
            _hlog(
                f"STEP retry: {spec.name} attempt={attempts + 1}/{1 + max_retries}",
                json_only=json_only,
            )
            await _sleep_backoff(attempts, base_s, jitter_s)
            continue

        dt = time.monotonic() - t0
        reason = "timeout" if rc == 124 else ((err_s[:220] if err_s else "") or "failed")
        _hlog(f"STEP done : {spec.name} ok=0 rc={rc} sec={dt:.2f}", json_only=json_only)
        return StepResult(
            name=spec.name,
            module=("inline" if spec.inline_py else (module_name or "")),
            ok=False,
            critical=spec.critical,
            rc=int(rc),
            seconds=float(dt),
            reason=reason,
            warnings=warnings,
            fix_hints=hints,
            attempts=attempts,
            child_json=(child_obj if isinstance(child_obj, dict) else None),
            stdout=(out_s if include_stdio else ""),
            stderr=(err_s if include_stdio else ""),
        )


# =============================================================================
# CLI parsing
# =============================================================================
class _ArgParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValueError(message)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    ap = _ArgParser(description="Run sniper preflight checks (P0 LOCK-IN).")
    ap.add_argument("--force", action="store_true", help="Run even if auto-skip policy would skip.")
    ap.add_argument("--strict", action="store_true", help="No retries; pass strict to check_env.")
    ap.add_argument("--profile", type=str, default="", help="fast|deploy|full")
    ap.add_argument("--timeout", type=float, default=_envf("PREFLIGHT_STEP_TIMEOUT_S", 90.0))
    ap.add_argument("--total-timeout", type=float, default=_envf("PREFLIGHT_TOTAL_TIMEOUT_S", 0.0))
    ap.add_argument("--json", action="store_true", help="Emit JSON to stdout.")
    ap.add_argument(
        "--json-only", action="store_true", help="Only JSON, suppress human stderr logs."
    )
    ap.add_argument(
        "--verbose", action="store_true", help="Stream child output live to stderr (sanitized)."
    )
    ap.add_argument("--list", action="store_true", help="List available steps and exit.")
    ap.add_argument("--steps", type=str, default="", help="Comma-separated allowlist of steps.")
    ap.add_argument("--skip", type=str, default="", help="Comma-separated skiplist of steps.")
    ap.add_argument(
        "--config-module",
        type=str,
        default="",
        help="Pass-through to check_env: module name that provides get_config() (sets CHECK_ENV_CONFIG_MODULE).",
    )
    return ap.parse_args(list(argv) if argv is not None else None)


# =============================================================================
# main orchestration
# =============================================================================
async def amain(argv: Sequence[str] | None = None) -> int:
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())  # type: ignore[attr-defined]

    with contextlib.suppress(Exception):
        _bootstrap_repo_root()
    with contextlib.suppress(Exception):
        _refresh_redaction_cache()

    try:
        args = _parse_args(argv)
    except Exception as e:
        json_only = _envb("PREFLIGHT_JSON_ONLY", False)
        emit_json = _envb("PREFLIGHT_JSON", True)

        payload = {
            "ts": _ts(),
            "ok": False,
            "exit_code": RC_REQUIRED_FAILED,
            "error": "cli_parse_error",
            "message": _sanitize(str(e)),
            "argv": [_sanitize(x) for x in (list(argv) if argv is not None else sys.argv[1:])][
                :200
            ],
            "contract": {
                "script": SCRIPT_NAME,
                "version": VERSION,
                "exit_codes": {
                    "ok": RC_OK,
                    "optional_failed": RC_OPTIONAL_FAILED,
                    "required_failed": RC_REQUIRED_FAILED,
                    "ctrl_c": RC_CTRL_C,
                },
                "no_network_default": True,
            },
        }
        if emit_json:
            _safe_print_json(payload)
        if not json_only:
            _hlog(f"CLI parse error: {_sanitize(str(e))}", json_only=False)
        return RC_REQUIRED_FAILED

    if args.config_module:
        os.environ["CHECK_ENV_CONFIG_MODULE"] = str(args.config_module).strip()

    dns_mode = _dns_mode()
    os.environ["PREFLIGHT_DNS_MODE"] = dns_mode

    json_only = bool(args.json_only or _envb("PREFLIGHT_JSON_ONLY", False))
    emit_json = bool(args.json or args.json_only or _envb("PREFLIGHT_JSON", True))
    verbose = bool(args.verbose or _envb("PREFLIGHT_VERBOSE", False))
    fail_on_optional = bool(_envb("PREFLIGHT_FAIL_ON_OPTIONAL", False))

    include_child_json = bool(_envb("PREFLIGHT_CHILD_JSON", False))
    include_stdio = bool(_envb("PREFLIGHT_INCLUDE_CHILD_STDIO", False))

    start_prechecks = _envb("START_PRECHECKS", False)

    strict_requested = bool(args.strict or _envb("START_PRECHECKS_STRICT", False))
    strict = bool(strict_requested)

    timeout_s = max(10.0, float(args.timeout))
    total_timeout_s = max(0.0, float(args.total_timeout))

    profile = (args.profile or _profile_default()).lower().strip()
    if profile not in {"fast", "deploy", "full"}:
        profile = _profile_default()

    explicit_intent = bool(
        args.force
        or args.strict
        or args.profile
        or args.json
        or args.json_only
        or args.verbose
        or args.steps
        or args.skip
        or args.list
        or args.config_module
    )

    if _envb("PREFLIGHT_SKIP", False):
        if emit_json:
            _safe_print_json(
                {
                    "ts": _ts(),
                    "ok": True,
                    "skipped": True,
                    "reason": "PREFLIGHT_SKIP=1",
                    "contract": {
                        "script": SCRIPT_NAME,
                        "version": VERSION,
                        "exit_codes": {
                            "ok": RC_OK,
                            "optional_failed": RC_OPTIONAL_FAILED,
                            "required_failed": RC_REQUIRED_FAILED,
                            "ctrl_c": RC_CTRL_C,
                        },
                        "no_network_default": True,
                    },
                }
            )
        _hlog("PREFLIGHT_SKIP=1 -> skipping preflight.", json_only=json_only)
        return RC_OK

    required_by_env = _envb("PREFLIGHT_REQUIRED", _render_like())
    if (not required_by_env) and (not start_prechecks) and (not explicit_intent):
        _hlog(
            "START_PRECHECKS!=1 and no explicit intent -> skipping preflight (use --force or set START_PRECHECKS=1)",
            json_only=json_only,
        )
        if emit_json:
            _safe_print_json(
                {
                    "ts": _ts(),
                    "ok": True,
                    "skipped": True,
                    "reason": "START_PRECHECKS!=1",
                    "contract": {
                        "script": SCRIPT_NAME,
                        "version": VERSION,
                        "exit_codes": {
                            "ok": RC_OK,
                            "optional_failed": RC_OPTIONAL_FAILED,
                            "required_failed": RC_REQUIRED_FAILED,
                            "ctrl_c": RC_CTRL_C,
                        },
                        "no_network_default": True,
                    },
                }
            )
        return RC_OK

    # -------------------------------------------------------------------------
    # CONFIG GATEKEEPER (FAIL FAST): validate_config(C)
    # -------------------------------------------------------------------------
    gate = _config_gatekeeper(json_only=json_only)
    preface_results: list[StepResult] = [gate]
    if not gate.ok:
        summary_fail: dict[str, Any] = {
            "ts": _ts(),
            "ok": False,
            "exit_code": RC_REQUIRED_FAILED,
            "critical_failed": True,
            "optional_failed": False,
            "fail_on_optional": bool(fail_on_optional),
            "profile": profile,
            "strict": bool(strict),
            "strict_requested": bool(strict_requested),
            "dns_mode": dns_mode,
            "render_like": bool(_render_like()),
            "network_allowed": bool(_network_allowed()),
            "timeout_s": float(timeout_s),
            "total_timeout_s": float(total_timeout_s),
            "required_by_env": bool(required_by_env),
            "meta": {
                "python": _sanitize(sys.version.split()[0]),
                "platform": _sanitize(platform.platform()),
                "hostname": _sanitize(socket.gethostname()),
                "pid": os.getpid(),
                "cwd": _sanitize(os.getcwd()),
                "repo_root": _sanitize(os.getenv("REPO_ROOT", "")),
            },
            "effective_skips": [],
            "results": [
                x.public(include_child_json=include_child_json, include_stdio=include_stdio)
                for x in preface_results
            ],
            "contract": {
                "script": SCRIPT_NAME,
                "version": VERSION,
                "exit_codes": {
                    "ok": RC_OK,
                    "optional_failed": RC_OPTIONAL_FAILED,
                    "required_failed": RC_REQUIRED_FAILED,
                    "ctrl_c": RC_CTRL_C,
                },
                "no_network_default": True,
            },
        }
        if emit_json:
            _safe_print_json(summary_fail)
        _hlog("SUMMARY ok=0 (config validation failed)", json_only=json_only)
        return RC_REQUIRED_FAILED

    registry = _build_registry(step_timeout_s=timeout_s)

    if args.list:
        _hlog("AVAILABLE_STEPS:", json_only=json_only)
        for s in registry:
            resolved = "inline" if s.inline_py else (s.resolve_module() or "")
            _hlog(
                f" - {s.name:18} module={resolved or (s.modules[0] if s.modules else 'inline')} "
                f"critical={int(s.critical)} optional_if_missing={int(s.optional_if_missing)} argv={list(s.argv)} :: {s.description}",
                json_only=json_only,
            )
        return RC_OK

    base_plan = _profile_select(profile, registry)

    only_steps = set(_parse_csv(args.steps or _envs("PREFLIGHT_STEPS", "")))
    skip_steps = set(_parse_csv(args.skip or _envs("PREFLIGHT_SKIP_STEPS", "")))

    if dns_mode == "off":
        skip_steps.add("check_dns")

    plan: list[StepSpec] = []
    for s in base_plan:
        if only_steps and (s.name not in only_steps):
            continue
        if s.name in skip_steps:
            continue
        plan.append(s)

    tuned: list[StepSpec] = []
    for s in plan:
        if s.name == "check_dns":
            dns_is_strict = dns_mode == "strict"
            tuned.append(replace(s, critical=bool(dns_is_strict)))
        else:
            tuned.append(s)
    plan = tuned

    if not plan:
        _hlog("No steps selected (after profile/--steps/--skip gating).", json_only=json_only)
        if emit_json:
            _safe_print_json({"ts": _ts(), "ok": True, "skipped": True, "reason": "no_steps"})
        return RC_OK

    plan, skips_1 = _initial_policy_prune(plan)

    _hlog(
        f"BOOT profile={profile} strict={int(strict)} dns_mode={dns_mode} render_like={int(_render_like())} "
        f"network_allowed={int(_network_allowed())} timeout_s={int(timeout_s)} total_timeout_s={int(total_timeout_s)} "
        f"plan={[s.name for s in plan]}",
        json_only=json_only,
    )

    results: list[StepResult] = list(preface_results)
    skips_final: list[dict[str, Any]] = list(skips_1)
    t_start = time.monotonic()
    critical_failed = False
    optional_failed = False

    if plan and plan[0].name != "check_env":
        plan = sorted(plan, key=lambda x: (0 if x.name == "check_env" else 1, x.name))

    check_env_json: dict[str, Any] | None = None

    for idx, spec in enumerate(plan):
        elapsed = time.monotonic() - t_start
        remaining_total: float | None = None
        if total_timeout_s > 0:
            remaining_total = float(total_timeout_s - elapsed)
            if remaining_total <= 0:
                critical_failed = True
                results.append(
                    StepResult(
                        spec.name,
                        "inline" if spec.inline_py else (spec.resolve_module() or ""),
                        False,
                        spec.critical,
                        124,
                        0.0,
                        skipped=True,
                        reason=f"total_timeout>{total_timeout_s:.0f}s",
                        fix_hints=["Increase PREFLIGHT_TOTAL_TIMEOUT_S or reduce step timeouts."],
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
            include_stdio=include_stdio,
        )
        results.append(r)

        if spec.name == "check_env" and isinstance(r.child_json, dict):
            check_env_json = r.child_json
            if idx < (len(plan) - 1):
                remaining = plan[idx + 1 :]
                remaining2, skips_2 = _policy_prune_after_check_env(remaining, check_env_json)
                skips_final.extend(skips_2)
                plan = plan[: idx + 1] + remaining2

        if not r.ok:
            if spec.critical:
                critical_failed = True
            else:
                optional_failed = True
            if strict:
                break

    passed = (not critical_failed) and (not (optional_failed and fail_on_optional))

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": bool(passed),
        "exit_code": (
            RC_OK if passed else (RC_REQUIRED_FAILED if critical_failed else RC_OPTIONAL_FAILED)
        ),
        "critical_failed": bool(critical_failed),
        "optional_failed": bool(optional_failed),
        "fail_on_optional": bool(fail_on_optional),
        "profile": profile,
        "strict": bool(strict),
        "strict_requested": bool(strict_requested),
        "dns_mode": dns_mode,
        "render_like": bool(_render_like()),
        "network_allowed": bool(_network_allowed()),
        "timeout_s": float(timeout_s),
        "total_timeout_s": float(total_timeout_s),
        "required_by_env": bool(required_by_env),
        "meta": {
            "python": _sanitize(sys.version.split()[0]),
            "platform": _sanitize(platform.platform()),
            "hostname": _sanitize(socket.gethostname()),
            "pid": os.getpid(),
            "cwd": _sanitize(os.getcwd()),
            "repo_root": _sanitize(os.getenv("REPO_ROOT", "")),
        },
        "effective_skips": skips_final,
        "results": [
            x.public(include_child_json=include_child_json, include_stdio=include_stdio)
            for x in results
        ],
        "contract": {
            "script": SCRIPT_NAME,
            "version": VERSION,
            "exit_codes": {
                "ok": RC_OK,
                "optional_failed": RC_OPTIONAL_FAILED,
                "required_failed": RC_REQUIRED_FAILED,
                "ctrl_c": RC_CTRL_C,
            },
            "no_network_default": True,
        },
    }

    if emit_json:
        _safe_print_json(summary)

    if passed:
        _hlog("SUMMARY ok=1 (preflight passed)", json_only=json_only)
        return RC_OK
    if critical_failed:
        _hlog("SUMMARY ok=0 (required step failed)", json_only=json_only)
        return RC_REQUIRED_FAILED

    _hlog("SUMMARY ok=0 (only optional steps failed and FAIL_ON_OPTIONAL=1)", json_only=json_only)
    return RC_OPTIONAL_FAILED


def main(argv: Sequence[str] | None = None) -> None:
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

    try:
        rc = asyncio.run(amain(argv))
    except KeyboardInterrupt:
        rc = RC_CTRL_C
    except Exception as e:
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        _hlog(
            f"FAIL unhandled err={_sanitize(type(e).__name__)}: {_sanitize(str(e))} trace={_sanitize(tb)[-2200:]}",
            json_only=False,
        )
        rc = RC_REQUIRED_FAILED
    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
