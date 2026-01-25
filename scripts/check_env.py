# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/check_env.py — God-Tier Env Validator (P0 LOCK-IN, Phase 0–7)
=====================================================================

Purpose
-------
This script is a *deploy gatekeeper* used by scripts.preflight.

Hard rule (no drift)
--------------------
config.py is the single source of truth.
If config.validate_environment(...) / validate_config(...) succeeds, this script must not fail later
due to re-implementing a drifting env contract.

Accordingly, check_env performs:
  (A) Canonical config validation (fatal on failure when required)
  (B) Diagnostics (warning by default; promotable to fatal via --strict / CHECK_ENV_STRICT=1)

Safety guarantees
-----------------
- No network calls by default (DNS optional via --dns / CHECK_ENV_DNS=1)
- No secret leakage:
  * key-name masking
  * URL credential masking + query stripping
  * header token masking
  * exact secret-value replacement (env values)
  * optional long-token masking (keeps Solana mints visible)
  * env-file validation NEVER prints raw line contents
- Crash-proof:
  * public APIs never raise
  * stable JSON output when requested

Exit codes (contract)
---------------------
0  OK
1  Fatal issues found (or CLI parse error)

Flags (also mirrored by env toggles)
------------------------------------
--json             => CHECK_ENV_JSON=1
--json-only        => CHECK_ENV_JSON_ONLY=1 and CHECK_ENV_JSON=1
--dns              => CHECK_ENV_DNS=1
--strict           => CHECK_ENV_STRICT=1
--show-env         => CHECK_ENV_SHOW_ENV=1
--failfast         => CHECK_ENV_FAILFAST=1
--validate-env     => CHECK_ENV_VALIDATE_ENV=1
--env-file PATH    => CHECK_ENV_ENV_FILE=PATH
--skip-config      => CHECK_ENV_SKIP_CONFIG=1
--require-config   => CHECK_ENV_REQUIRE_CONFIG=1
--config-module M  => CHECK_ENV_CONFIG_MODULE=M

Optional debug toggles
----------------------
CHECK_ENV_INCLUDE_CONFIG_DETAIL=1
  Include sanitized config validation detail in JSON.

CHECK_ENV_CAPTURE_CONFIG_OUTPUT=1
  Capture (sanitized) stdout/stderr produced during config validation (kept short + redacted).

Notes
-----
- stdlib-only; safe on Windows + Render
- stable JSON schema for preflight/start.sh gating
"""

import argparse
import contextlib
import importlib
import importlib.util
import inspect
import io
import json
import math
import os
import platform
import re
import socket
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

__all__ = ["run_checks", "report_as_json", "validate_environment", "cli", "main"]

# =============================================================================
# bootstrap (repo root)
# =============================================================================


def _bootstrap_repo_root() -> str:
    """
    Detect repo root via common markers and export REPO_ROOT.
    Keep consistent with scripts/preflight.py behavior.
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
    root_path: Path | None = None

    for candidate in candidates:
        try:
            has_marker = any((candidate / m).exists() for m in markers)
            has_src = (candidate / "src").is_dir()
            if has_marker or has_src:
                root_path = candidate
                break
        except Exception:
            continue

    if root_path is None:
        root_path = here.parents[1] if len(here.parents) > 1 else here.parent

    root_s = str(root_path)
    os.environ.setdefault("REPO_ROOT", root_s)

    try:
        if root_s and root_s not in sys.path:
            sys.path.insert(0, root_s)
        src = root_path / "src"
        if src.is_dir():
            src_s = str(src)
            if src_s not in sys.path:
                sys.path.insert(0, src_s)
    except Exception:
        pass

    return root_s


_REPO_ROOT = _bootstrap_repo_root()

# =============================================================================
# constants / parsing
# =============================================================================

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

RC_OK = 0
RC_FATAL = 1  # IMPORTANT: 0/1 contract

VERSION = "v14.0"
SCRIPT_ID = "scripts.check_env"

_URL_CRED_RE = re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://)([^@\s/]+@)")
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_HDR_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_BEARER_ANY_RE = re.compile(r"(?i)\b(bearer)\s+([A-Za-z0-9\-._~+/]+=*)")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|passwd|pass|authorization)\b(\s*[:=]\s*)([^\s,;]+)"
)
_QP_SECRET_RE = re.compile(r"(?i)(password|pass|token|api_key|apikey|secret)=([^&\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")

_BASE58_ALPH = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

KNOWN_SOL_MINT = "So11111111111111111111111111111111111111112"
KNOWN_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
KNOWN_BAD_USDC_TYPO = "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v"

_ENV_KEY_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# refreshed at runtime
_REDACT_KEY_SET: set[str] = set()
_SECRET_VALUES: list[str] = []


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
    s = raw.lower().strip()
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
        x = float(s)
        if not math.isfinite(x):
            return float(d)
        return x
    except Exception:
        return float(d)


def _parse_csv(s: str) -> list[str]:
    if not s:
        return []
    parts = [p.strip() for p in s.replace(";", ",").split(",")]
    return [p for p in parts if p]


def _clamp_int(x: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(x)))
    except Exception:
        return lo


def _bool01(x: bool) -> int:
    return 1 if bool(x) else 0


def _safe_write_line(stream: Any, s: str) -> None:
    try:
        if not s.endswith("\n"):
            s += "\n"
        stream.write(s)
        stream.flush()
    except UnicodeEncodeError:
        try:
            enc = getattr(stream, "encoding", None) or "utf-8"
            buf = s.encode(enc, errors="replace")
            if hasattr(stream, "buffer"):
                stream.buffer.write(buf)  # type: ignore[attr-defined]
                stream.buffer.flush()  # type: ignore[attr-defined]
            else:
                stream.write(buf.decode(enc, errors="replace"))
                stream.flush()
        except Exception:
            return
    except Exception:
        return


def _safe_dump_json(payload: dict[str, Any], *, json_only: bool) -> None:
    """
    STDOUT JSON emitter. Never throws. Stable formatting:
    - json_only => compact
    - else => indented
    """
    try:
        if json_only:
            out = json.dumps(
                payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True, default=str
            )
        else:
            out = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        sys.stdout.write(out + "\n")
        sys.stdout.flush()
    except Exception:
        try:
            sys.stdout.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
            sys.stdout.flush()
        except Exception:
            _safe_write_line(sys.stdout, json.dumps(payload, ensure_ascii=False, default=str))


# =============================================================================
# redaction / safety
# =============================================================================


def _is_sensitive_key(k: str) -> bool:
    kk = (k or "").upper()
    return any(
        needle in kk
        for needle in (
            "PASSWORD",
            "PASS",
            "SECRET",
            "TOKEN",
            "API_KEY",
            "APIKEY",
            "AUTH",
            "PRIVATE",
            "MNEMONIC",
            "SEED",
            "SIGNATURE",
            "KEYPAIR",
            "WALLET",
            "DATABASE_URL",
            "DSN",
            "REDIS_URL",
        )
    )


def _looks_like_mint(s: str) -> bool:
    ss = (s or "").strip()
    if not (32 <= len(ss) <= 44):
        return False
    return all(ch in _BASE58_ALPH for ch in ss)


def _mask_urlish(u: str) -> str:
    if not u:
        return ""
    s = _URL_CRED_RE.sub(r"\1***@", str(u))
    if "?" in s:
        s = s.split("?", 1)[0] + "?..."
    return s


def _mask_long_tokens_enabled() -> bool:
    if "CHECK_ENV_MASK_LONG_TOKENS" in os.environ:
        return _envb("CHECK_ENV_MASK_LONG_TOKENS", True)
    if "PREFLIGHT_MASK_LONG_TOKENS" in os.environ:
        return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)
    return True


def _redact_keys() -> set[str]:
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
        "SNIPER_DB_DSN",
        "PNL_DB_DSN",
        "REDIS_URL",
        "REDIS_TLS_URL",
        "SOL_RPC_URL",
        "RPC_URL",
        "SOL_RPC_URLS",
        "LOG_REDACT_KEYS",
    ]
    extra = _parse_csv(_envs("CHECK_ENV_REDACT_KEYS", ""))
    out = base + [x for x in extra if x]

    uniq: set[str] = set()
    for key in out:
        kk = (key or "").strip().upper()
        if kk:
            uniq.add(kk)
    return uniq


def _collect_secret_values(redact_keys: set[str]) -> list[str]:
    """
    Build a list of exact secret values to replace in any emitted strings.
    Sorted by length desc to prevent partial masking gaps.
    """
    values: set[str] = set()
    try:
        for key in sorted(redact_keys):
            val = os.getenv(key, "") or ""
            if val:
                vv = _strip_wrapping_quotes(val)
                if 6 <= len(vv) <= 8192:
                    values.add(vv)

        for key, val in os.environ.items():
            if not val:
                continue
            if _is_sensitive_key(key):
                vv = _strip_wrapping_quotes(val)
                if 6 <= len(vv) <= 8192:
                    values.add(vv)
    except Exception:
        pass

    out = sorted(values, key=lambda s: (-len(s), s))
    return out[:400]


def _refresh_redaction() -> None:
    global _REDACT_KEY_SET, _SECRET_VALUES
    try:
        _REDACT_KEY_SET = _redact_keys()
        _SECRET_VALUES = _collect_secret_values(_REDACT_KEY_SET)
    except Exception:
        _REDACT_KEY_SET = set()
        _SECRET_VALUES = []


def _mask_tokenish_match(m: re.Match) -> str:
    tok = m.group(0)
    if _looks_like_mint(tok):
        return tok
    return "***"


def _sanitize_text(text: Any) -> str:
    """
    Aggressively redact secrets from any text emitted to stdout/stderr/json.
    Never throws. Always returns str.
    """
    if text is None:
        return ""
    s = str(text)

    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_HDR_RE.sub(r"\1***", s)
    s = _BEARER_ANY_RE.sub(r"\1 ***", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)
    s = _QP_SECRET_RE.sub(r"\1=***", s)

    for sec in _SECRET_VALUES:
        try:
            if sec and sec in s:
                s = s.replace(sec, "***")
        except Exception:
            continue

    if _mask_long_tokens_enabled():
        s = _LONG_TOKENISH_RE.sub(_mask_tokenish_match, s)

    return s


def _mask_path_basename(p: str) -> str:
    if not p:
        return ""
    s = str(p).strip().strip('"').strip("'").replace("\\", "/")
    parts = [x for x in s.split("/") if x]
    if not parts:
        return "***"
    if len(parts) == 1:
        return parts[-1]
    return f".../{parts[-2]}/{parts[-1]}"


def _mask_value(k: str, v: str | None) -> Any:
    if v is None:
        return None
    vv = _strip_wrapping_quotes(str(v))
    if vv == "":
        return ""
    ku = (k or "").strip().upper()

    if "PATH" in ku and vv:
        return _sanitize_text(_mask_path_basename(vv))

    if _is_sensitive_key(ku) or (ku in _REDACT_KEY_SET):
        return "***"

    for sec in _SECRET_VALUES:
        try:
            if sec and vv == sec:
                return "***"
        except Exception:
            pass

    if "://" in vv:
        return _mask_urlish(vv)

    if len(vv) > 240:
        vv = vv[:240] + "…"

    return _sanitize_text(vv)


def _sanitize_mapping_shallow(obj: Any, *, limit_items: int = 80) -> Any:
    """
    Best-effort sanitize of dict/list return details without leaking secrets.
    Shallow by design to avoid huge payloads and accidental sensitive deep structures.
    """
    try:
        if obj is None:
            return None
        if isinstance(obj, (str, int, float, bool)):
            return _sanitize_text(obj) if isinstance(obj, str) else obj
        if isinstance(obj, dict):
            out: dict[str, Any] = {}
            for idx, (k, v) in enumerate(obj.items()):
                if idx >= limit_items:
                    out["__truncated__"] = True
                    break
                ks = _sanitize_text(k)
                if isinstance(v, str):
                    out[ks] = _mask_value(ks, v)
                else:
                    out[ks] = _sanitize_mapping_shallow(v, limit_items=20)
            return out
        if isinstance(obj, list):
            out_list: list[Any] = []
            for it in obj[:limit_items]:
                out_list.append(_sanitize_mapping_shallow(it, limit_items=20))
            if len(obj) > limit_items:
                out_list.append({"__truncated__": True})
            return out_list
        return _sanitize_text(obj)
    except Exception:
        return _sanitize_text(obj)


# =============================================================================
# report model
# =============================================================================


@dataclass
class Issue:
    kind: str  # "fatal" | "warn"
    code: str
    section: str
    detail: Any
    hint: str = ""


@dataclass
class Report:
    ok: bool = True
    fatals: list[Issue] = field(default_factory=list)
    warns: list[Issue] = field(default_factory=list)
    info: dict[str, Any] = field(default_factory=dict)

    def fatal(self, code: str, section: str, detail: Any, hint: str = "") -> None:
        if code in _warn_only_codes():
            self.warn(code, section, detail, hint=hint)
            return
        self.ok = False
        self.fatals.append(Issue("fatal", code, section, detail, hint=hint))

    def warn(self, code: str, section: str, detail: Any, hint: str = "") -> None:
        self.warns.append(Issue("warn", code, section, detail, hint=hint))


def _warn_only_codes() -> set[str]:
    return set(_parse_csv(_envs("CHECK_ENV_WARN_ONLY", "")))


def _maybe_failfast(rep: Report) -> bool:
    return bool(rep.fatals) and _envb("CHECK_ENV_FAILFAST", False)


def _safe_section(rep: Report, section: str, fn, *, critical: bool = False) -> None:
    try:
        fn()
    except BaseException as exc:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        payload = {
            "type": type(exc).__name__,
            "error": _sanitize_text(exc),
            "trace": _sanitize_text(tb)[-6000:],
        }
        if critical:
            rep.fatal(
                "section_exception",
                section,
                payload,
                hint="Fix the exception above; this section must be stable.",
            )
        else:
            rep.warn(
                "section_exception",
                section,
                payload,
                hint="Fix the exception above; this section should be stable.",
            )


def _safe_call(rep: Report, section: str, fn, *, critical: bool = False) -> Any:
    try:
        return fn()
    except BaseException as exc:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        payload = {
            "type": type(exc).__name__,
            "error": _sanitize_text(exc),
            "trace": _sanitize_text(tb)[-6000:],
        }
        if critical:
            rep.fatal(
                "section_exception",
                section,
                payload,
                hint="Fix the exception above; this section must be stable.",
            )
        else:
            rep.warn(
                "section_exception",
                section,
                payload,
                hint="Fix the exception above; this section should be stable.",
            )
        return None


# =============================================================================
# canonical policy (prefer config.py)
# =============================================================================


def _repo_has_config_py() -> bool:
    try:
        rr = Path(_REPO_ROOT) if _REPO_ROOT else Path.cwd()
        if (rr / "config.py").exists():
            return True
        if (rr / "src" / "config.py").exists():
            return True
    except Exception:
        pass

    try:
        spec = importlib.util.find_spec("config")
        if spec and getattr(spec, "origin", None):
            origin = str(spec.origin or "")
            if origin and origin != "built-in":
                return True
    except Exception:
        pass

    return False


def _config_validation_required() -> bool:
    if "CHECK_ENV_REQUIRE_CONFIG" in os.environ:
        return _envb("CHECK_ENV_REQUIRE_CONFIG", True)
    return _repo_has_config_py()


def _call_compat(fn: Any, base_kwargs: dict[str, Any]) -> Any:
    """
    Call a function with only kwargs it accepts.
    - Supports callables that accept **kwargs (VAR_KEYWORD).
    - Falls back to smaller calls if wrappers/signatures lie.
    """
    try:
        sig = inspect.signature(fn)
        params = sig.parameters.values()
        accepts_varkw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
        if accepts_varkw:
            return fn(**base_kwargs)

        accepted = {p.name for p in params}
        kwargs = {k: v for k, v in base_kwargs.items() if k in accepted}
        try:
            return fn(**kwargs) if kwargs else fn()
        except TypeError:
            return fn()
    except Exception:
        try:
            return fn(**base_kwargs)
        except Exception:
            return fn()


def _pick_first_callable(mod: Any, names: tuple[str, ...]) -> tuple[str, Any] | None:
    for name in names:
        fn = getattr(mod, name, None)
        if callable(fn):
            return name, fn
    return None


def _extract_cfg_summary(cfg: Any) -> dict[str, Any]:
    """
    Secret-safe, small summary of config object.
    """
    out: dict[str, Any] = {"type": type(cfg).__name__}
    try:
        if isinstance(cfg, dict):
            for k in (
                "phase",
                "MASTER_PHASE",
                "STRATEGY_PHASE",
                "ENV",
                "env",
                "mode",
                "DRYRUN",
                "dryrun",
            ):
                if k in cfg:
                    out[k] = _mask_value(k, str(cfg.get(k)))
            return out

        for attr in (
            "phase",
            "MASTER_PHASE",
            "STRATEGY_PHASE",
            "DRYRUN",
            "dryrun",
            "ENV",
            "env",
            "mode",
        ):
            if hasattr(cfg, attr):
                try:
                    out[attr] = _mask_value(attr, str(getattr(cfg, attr)))
                except Exception:
                    continue
    except Exception:
        pass
    return out


def _phase_from_cfg(cfg: Any | None) -> int | None:
    try:
        if cfg is None:
            return None
        if isinstance(cfg, dict):
            for key in ("phase", "MASTER_PHASE", "STRATEGY_PHASE"):
                if key in cfg and str(cfg.get(key, "")).strip() != "":
                    return _clamp_int(int(float(str(cfg.get(key)))), 0, 7)
        else:
            for attr in ("phase", "MASTER_PHASE", "STRATEGY_PHASE"):
                if hasattr(cfg, attr):
                    val = getattr(cfg, attr)
                    if str(val).strip() != "":
                        return _clamp_int(int(float(str(val))), 0, 7)
    except Exception:
        return None
    return None


def _coerce_validation_result(result: Any) -> tuple[bool, dict[str, Any]]:
    """
    Normalize validate_* return styles into (ok, details).

    Supported:
      - None (ok)
      - bool
      - dict: {ok: bool} OR {errors: [...]} OR {fatals: [...]} etc
      - tuple: (ok, details) or (ok, errors)
      - list: errors (ok if empty)
    """
    if result is None:
        return True, {"style": "none"}

    if isinstance(result, bool):
        return bool(result), {"style": "bool", "value": bool(result)}

    if isinstance(result, dict):
        if "ok" in result:
            return bool(result.get("ok")), {"style": "dict", "value": result}
        errs = result.get("errors")
        fats = result.get("fatals")
        if isinstance(errs, list) and errs:
            return False, {"style": "dict_errors", "value": result}
        if isinstance(fats, list) and fats:
            return False, {"style": "dict_fatals", "value": result}
        return True, {"style": "dict", "value": result}

    if isinstance(result, (list, tuple)):
        if isinstance(result, list):
            ok = len(result) == 0
            return ok, {"style": "list", "errors": result[:200]}

        if len(result) >= 2 and isinstance(result[0], bool):
            return bool(result[0]), {"style": "tuple", "detail": result[1]}
        if len(result) == 1 and isinstance(result[0], bool):
            return bool(result[0]), {"style": "tuple1", "value": bool(result[0])}
        return True, {"style": "tuple_unknown", "value": list(result)[:50]}

    return True, {"style": "unknown", "type": type(result).__name__}


def _capture_config_output_enabled() -> bool:
    return _envb("CHECK_ENV_CAPTURE_CONFIG_OUTPUT", False)


def _capture_streams_context() -> tuple[io.StringIO, io.StringIO, Any]:
    """
    Returns (stdout_buf, stderr_buf, context_manager) that redirects streams.
    """
    out_buf = io.StringIO()
    err_buf = io.StringIO()
    ctx = contextlib.ExitStack()
    ctx.enter_context(contextlib.redirect_stdout(out_buf))
    ctx.enter_context(contextlib.redirect_stderr(err_buf))
    return out_buf, err_buf, ctx


def _validate_config_canonical(rep: Report) -> dict[str, Any]:
    """
    Canonical validation path: match preflight by delegating to config.py.

    Priority order:
      1) config.validate_environment()  (preflight-style gate)
      2) builder + validate_config(cfg)
      3) validate_config() with implicit build (if supported)

    Returns:
      { ok: bool, phase: int|None, summary: dict|None }
    """
    sec = "config"
    if _envb("CHECK_ENV_SKIP_CONFIG", False):
        rep.info["config"] = {"enabled": 0, "skipped": 1}
        rep.info.setdefault("policy", {})["canonical_config_ok"] = 0
        return {"ok": False, "phase": None, "summary": None}

    module_name = _envs("CHECK_ENV_CONFIG_MODULE", "config") or "config"
    required = _config_validation_required()

    rep.info["config"] = {
        "enabled": 1,
        "required": _bool01(required),
        "module": _sanitize_text(module_name),
        "builder": None,
        "validator": None,
        "validator_style": None,
        "ok": 0,
        "phase": None,
        "summary": None,
    }

    try:
        mod = importlib.import_module(module_name)
    except Exception as exc:
        detail = {
            "error": f"{type(exc).__name__}: {_sanitize_text(exc)}",
            "module": _sanitize_text(module_name),
        }
        if required:
            rep.fatal(
                "config_import_failed",
                sec,
                detail,
                hint="config module must be importable (same as preflight). Fix module path/import errors.",
            )
        else:
            rep.warn("config_import_failed", sec, detail, hint="Optional config import failed.")
        rep.info.setdefault("policy", {})["canonical_config_ok"] = 0
        return {"ok": False, "phase": None, "summary": None}

    # 1) Preferred: config.validate_environment()
    validate_env_pick = _pick_first_callable(mod, ("validate_environment",))
    if validate_env_pick:
        name, fn = validate_env_pick
        rep.info["config"]["validator"] = name
        rep.info["config"]["validator_style"] = "validate_environment"

        out_buf: io.StringIO | None = None
        err_buf: io.StringIO | None = None
        ctx = None
        try:
            if _capture_config_output_enabled():
                out_buf, err_buf, ctx = _capture_streams_context()
                ctx.__enter__()  # type: ignore[union-attr]

            res = _call_compat(fn, {"env": os.environ, "environ": os.environ, "strict": True})

            ok = True
            payload: Any = None
            if isinstance(res, tuple) and len(res) >= 1 and isinstance(res[0], bool):
                ok = bool(res[0])
                payload = res[1] if len(res) >= 2 else None
            elif isinstance(res, bool):
                ok = bool(res)
            elif isinstance(res, dict):
                ok = bool(res.get("ok", True))
                payload = res
            else:
                ok = True
                payload = res

            if not ok:
                raise ValueError("config.validate_environment returned not-ok")

            rep.info["config"]["ok"] = 1
            rep.info.setdefault("policy", {})["canonical_config_ok"] = 1

            phase_val: int | None = None
            try:
                if isinstance(payload, dict):
                    summ = None
                    if isinstance(payload.get("info"), dict):
                        summ = payload.get("info", {}).get("summary")
                    if summ is None:
                        summ = payload.get("summary")
                    if isinstance(summ, dict) and "phase" in summ:
                        phase_val = _clamp_int(int(float(str(summ.get("phase")))), 0, 7)
            except Exception:
                phase_val = None

            rep.info["config"]["phase"] = phase_val
            if payload is not None and _envb("CHECK_ENV_INCLUDE_CONFIG_DETAIL", False):
                rep.info["config"]["validate_detail"] = _sanitize_mapping_shallow(payload)

            return {"ok": True, "phase": phase_val, "summary": None}
        except Exception as exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            detail = {
                "error": f"{type(exc).__name__}: {_sanitize_text(exc)}",
                "trace": _sanitize_text(tb)[-6000:],
            }
            if required:
                rep.fatal(
                    "config_validate_failed",
                    sec,
                    detail,
                    hint="Canonical config validation failed (same contract as preflight). Fix env vars to satisfy config.py.",
                )
            else:
                rep.warn(
                    "config_validate_failed", sec, detail, hint="Optional config validation failed."
                )
            rep.info.setdefault("policy", {})["canonical_config_ok"] = 0
            rep.info["config"]["ok"] = 0
            return {"ok": False, "phase": None, "summary": None}
        finally:
            try:
                if ctx is not None:
                    ctx.__exit__(None, None, None)  # type: ignore[union-attr]
            except Exception:
                pass
            if _capture_config_output_enabled() and out_buf is not None and err_buf is not None:
                out_s = _sanitize_text(out_buf.getvalue())[:4000]
                err_s = _sanitize_text(err_buf.getvalue())[:4000]
                if out_s or err_s:
                    rep.info["config"]["captured_output"] = {
                        "stdout": out_s,
                        "stderr": err_s,
                        "truncated": _bool01(
                            len(out_buf.getvalue()) > 4000 or len(err_buf.getvalue()) > 4000
                        ),
                    }

    # 2) builder + validate_config
    builder_pick = _pick_first_callable(
        mod,
        (
            "build_config",
            "get_config",
            "load_config",
            "parse_config",
            "make_config",
            "config_from_env",
            "config",
        ),
    )
    validator_pick = _pick_first_callable(
        mod, ("validate_config", "validate", "validate_env", "validate_settings")
    )

    rep.info["config"]["builder"] = builder_pick[0] if builder_pick else None
    rep.info["config"]["validator"] = validator_pick[0] if validator_pick else None

    cfg: Any | None = None
    build_err: str | None = None

    if builder_pick:
        _, builder_fn = builder_pick
        base_kwargs = {
            "env": os.environ,
            "environ": os.environ,
            "strict": _envb("CHECK_ENV_CONFIG_STRICT", True),
            "validate": False,
            "path": _envs("CHECK_ENV_ENV_FILE", "") or "",
            "env_file": _envs("CHECK_ENV_ENV_FILE", "") or "",
        }
        try:
            cfg = _call_compat(builder_fn, base_kwargs)
        except Exception as exc:
            build_err = f"{type(exc).__name__}: {_sanitize_text(exc)}"
            cfg = None

    if build_err:
        rep.info["config"]["build_error"] = build_err

    if cfg is None and not validator_pick:
        detail = {
            "module": _sanitize_text(module_name),
            "note": "No config builder and no validate_config found.",
        }
        if required:
            rep.fatal(
                "config_builder_missing",
                sec,
                detail,
                hint="Expose build_config/get_config/load_config and validate_config in config.py (preflight expects this).",
            )
        else:
            rep.warn("config_builder_missing", sec, detail)
        rep.info.setdefault("policy", {})["canonical_config_ok"] = 0
        return {"ok": False, "phase": None, "summary": None}

    if validator_pick:
        _, validate_fn = validator_pick
        base_kwargs = {
            "config": cfg,
            "cfg": cfg,
            "settings": cfg,
            "strict": _envb("CHECK_ENV_CONFIG_STRICT", True),
            "env": os.environ,
            "environ": os.environ,
            "phase": None,
        }

        out_buf: io.StringIO | None = None
        err_buf: io.StringIO | None = None
        ctx = None
        try:
            if _capture_config_output_enabled():
                out_buf, err_buf, ctx = _capture_streams_context()
                ctx.__enter__()  # type: ignore[union-attr]

            result = _call_compat(validate_fn, base_kwargs)
            ok, detail = _coerce_validation_result(result)
            rep.info["config"]["validator_style"] = _sanitize_text(detail.get("style", ""))

            if _envb("CHECK_ENV_INCLUDE_CONFIG_DETAIL", False):
                rep.info["config"]["validate_detail"] = _sanitize_mapping_shallow(
                    detail.get("value", detail.get("detail", detail.get("errors", None)))
                )

            if not ok:
                raise ValueError("validate_config returned not-ok")

        except Exception as exc:
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            detail_out = {
                "error": f"{type(exc).__name__}: {_sanitize_text(exc)}",
                "trace": _sanitize_text(tb)[-6000:],
            }
            if required:
                rep.fatal(
                    "config_validate_failed",
                    sec,
                    detail_out,
                    hint="Canonical config validation failed (same contract as preflight). Fix env vars to satisfy config.py validate_config.",
                )
            else:
                rep.warn(
                    "config_validate_failed",
                    sec,
                    detail_out,
                    hint="Optional config validation failed.",
                )
            rep.info.setdefault("policy", {})["canonical_config_ok"] = 0
            rep.info["config"]["ok"] = 0
            rep.info["config"]["summary"] = _extract_cfg_summary(cfg) if cfg is not None else None
            rep.info["config"]["phase"] = _phase_from_cfg(cfg)
            return {
                "ok": False,
                "phase": rep.info["config"]["phase"],
                "summary": rep.info["config"]["summary"],
            }
        finally:
            try:
                if ctx is not None:
                    ctx.__exit__(None, None, None)  # type: ignore[union-attr]
            except Exception:
                pass
            if _capture_config_output_enabled() and out_buf is not None and err_buf is not None:
                out_s = _sanitize_text(out_buf.getvalue())[:4000]
                err_s = _sanitize_text(err_buf.getvalue())[:4000]
                if out_s or err_s:
                    rep.info["config"]["captured_output"] = {
                        "stdout": out_s,
                        "stderr": err_s,
                        "truncated": _bool01(
                            len(out_buf.getvalue()) > 4000 or len(err_buf.getvalue()) > 4000
                        ),
                    }

    if not validator_pick and cfg is not None:
        rep.warn(
            "config_validator_missing",
            sec,
            {"note": "Config builder succeeded but validate_config not found."},
            hint="Prefer exposing validate_config(cfg) so preflight/check_env share the same gatekeeper.",
        )

    rep.info["config"]["ok"] = 1
    rep.info["config"]["summary"] = _extract_cfg_summary(cfg) if cfg is not None else None
    rep.info["config"]["phase"] = _phase_from_cfg(cfg)
    rep.info.setdefault("policy", {})["canonical_config_ok"] = 1

    return {
        "ok": True,
        "phase": rep.info["config"]["phase"],
        "summary": rep.info["config"]["summary"],
    }


def _canonical_ok(rep: Report) -> bool:
    try:
        pol = rep.info.get("policy") or {}
        return bool(pol.get("canonical_config_ok"))
    except Exception:
        return False


def _phase_from_env() -> int:
    p = _envi("MASTER_PHASE", _envi("STRATEGY_PHASE", 7))
    return _clamp_int(p, 0, 7)


def _determine_phase(rep: Report) -> int:
    try:
        cfg_phase = rep.info.get("config", {}).get("phase", None)
        if cfg_phase is not None and str(cfg_phase).strip() != "":
            return _clamp_int(int(float(str(cfg_phase))), 0, 7)
    except Exception:
        pass
    return _phase_from_env()


def _buys_effectively_possible(phase: int) -> bool:
    if _envb("DRYRUN", False):
        return False
    if _envb("KILL_SWITCH", False) or _envb("DISABLE_BUYS", False):
        return False
    return phase >= 2


def _scanning_effectively_possible(phase: int) -> bool:
    if _envb("KILL_SWITCH", False) or _envb("DISABLE_SCANNERS", False):
        return False
    return phase >= 1


def _db_required() -> bool:
    return not _envb("SKIP_DSN_REQUIRED", False)


def _finalize(rep: Report) -> None:
    phase = _determine_phase(rep)
    rep.info["summary"] = {
        "fatals": len(rep.fatals),
        "warnings": len(rep.warns),
        "phase": phase,
        "buys_possible": _bool01(_buys_effectively_possible(phase)),
        "scanning_possible": _bool01(_scanning_effectively_possible(phase)),
        "db_required": _bool01(_db_required()),
        "config_required": _bool01(_config_validation_required()),
        "canonical_config_ok": _bool01(_canonical_ok(rep)),
    }


# =============================================================================
# diagnostics (warnings-first when canonical config is OK)
# =============================================================================


def _warn_or_fatal(rep: Report, code: str, section: str, detail: Any, *, hint: str = "") -> None:
    """
    If canonical config validation passed, diagnostics should not fail the run
    (to prevent drift). They become warnings (unless strict promotion later).
    If canonical config is NOT OK (or skipped), treat certain diagnostics as fatal.
    """
    if _canonical_ok(rep):
        rep.warn(code, section, detail, hint=hint)
    else:
        rep.fatal(code, section, detail, hint=hint)


def _validate_runtime(rep: Report) -> None:
    rep.info["runtime"] = {
        "ts": _ts(),
        "script": SCRIPT_ID,
        "version": VERSION,
        "python": _sanitize_text(sys.version.split()[0]),
        "platform": _sanitize_text(platform.platform()),
        "os_name": _sanitize_text(os.name),
        "pid": os.getpid(),
        "cwd": _sanitize_text(os.getcwd()),
        "repo_root": _sanitize_text(_REPO_ROOT),
        "stdout_encoding": getattr(sys.stdout, "encoding", None) or "",
        "stderr_encoding": getattr(sys.stderr, "encoding", None) or "",
        "PYTHONUTF8": _envs("PYTHONUTF8", ""),
        "PYTHONIOENCODING": _envs("PYTHONIOENCODING", ""),
        "PYTHONUNBUFFERED": _envs("PYTHONUNBUFFERED", ""),
        "PYTHONHASHSEED": _envs("PYTHONHASHSEED", ""),
        "RENDER": _bool01(_envb("RENDER", False)),
        "RENDER_SERVICE_ID_set": _bool01(bool(_envs("RENDER_SERVICE_ID", ""))),
        "hostname": _sanitize_text(socket.gethostname()),
    }

    try:
        maj, minor = sys.version_info[:2]
        if (maj, minor) < (3, 10):
            rep.warn(
                "python_old",
                "runtime",
                {"python": sys.version.split()[0]},
                hint="Use Python 3.10+ for best compatibility.",
            )
    except Exception:
        pass

    if _envs("PYTHONUNBUFFERED", "") not in {"1", "true", "TRUE"}:
        rep.warn(
            "python_unbuffered_not_set",
            "runtime",
            {"PYTHONUNBUFFERED": _envs("PYTHONUNBUFFERED", "")},
            hint="Set PYTHONUNBUFFERED=1 for real-time logs (Render + local consistency).",
        )


def _placeholderish(v: str) -> bool:
    s = (v or "").strip().lower()
    if not s:
        return False
    bad = {
        "changeme",
        "replace_me",
        "replace-me",
        "your_key",
        "your-api-key",
        "yourapikey",
        "none",
        "null",
        "todo",
        "tbd",
    }
    if s in bad:
        return True
    return "example" in s


def _host_from_urlish(s: str) -> str:
    if not s:
        return ""
    t = s.strip()
    if "://" in t:
        t = t.split("://", 1)[1]
    if "@" in t:
        t = t.split("@", 1)[1]
    t = t.split("/", 1)[0].split("?", 1)[0]
    t = t.split(":", 1)[0]
    return t.strip()


def _parse_qs_sslmode(dsn: str) -> str:
    try:
        parsed = urlparse(dsn)
        q = parse_qs(parsed.query or "")
        val = (q.get("sslmode") or [""])[0]
        return (val or "").lower().strip()
    except Exception:
        low = (dsn or "").lower()
        return "require" if "sslmode=require" in low else ""


def _dns_probe_one(host: str) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False, "host": host, "ipv4": None, "ipv6": None, "error": ""}
    if not host:
        out["error"] = "empty_host"
        return out
    try:
        try:
            infos4 = socket.getaddrinfo(host, 443, family=socket.AF_INET, type=socket.SOCK_STREAM)
            if infos4:
                out["ipv4"] = infos4[0][4][0]
        except Exception as exc4:
            out["ipv4"] = None
            out["error"] = f"AF_INET:{type(exc4).__name__}:{_sanitize_text(exc4)}"

        try:
            infos6 = socket.getaddrinfo(host, 443, family=socket.AF_INET6, type=socket.SOCK_STREAM)
            if infos6:
                out["ipv6"] = infos6[0][4][0]
        except Exception as exc6:
            msg = f"AF_INET6:{type(exc6).__name__}:{_sanitize_text(exc6)}"
            out["error"] = (out["error"] + " | " + msg).strip(" |")

        out["ok"] = bool(out["ipv4"] or out["ipv6"])
        if out["ok"]:
            out["error"] = ""
        return out
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}:{_sanitize_text(exc)}"
        return out


def _dns_checks(rep: Report) -> None:
    sec = "dns"
    if not _envb("CHECK_ENV_DNS", False):
        rep.info["dns"] = {"enabled": 0}
        return

    hosts: set[str] = set()

    jup_base = _envs(
        "JUP_BASE_URL", _envs("JUPITER_BASE_URL", "https://api.jup.ag") or "https://api.jup.ag"
    )
    hosts.add(_host_from_urlish(jup_base) or "api.jup.ag")
    hosts.add("public.jupiterapi.com")

    rpc = _envs("SOL_RPC_URL", _envs("RPC_URL", ""))
    if rpc:
        hosts.add(_host_from_urlish(rpc))

    redis_url = _envs("REDIS_URL", _envs("REDIS_TLS_URL", ""))
    if redis_url:
        hosts.add(_host_from_urlish(redis_url))

    for key in ("DATABASE_URL", "POSTGRES_DSN", "PG_DSN", "PNL_DSN"):
        val = _envs(key, "")
        if val:
            hosts.add(_host_from_urlish(val))

    fatal_dns = _envb("CHECK_ENV_DNS_REQUIRED", False)

    out: dict[str, Any] = {"enabled": 1, "required": _bool01(fatal_dns), "results": {}}
    any_fail = False

    for host in sorted({h for h in hosts if h}):
        res = _dns_probe_one(host)
        out["results"][host] = res
        if not res.get("ok"):
            any_fail = True
            if fatal_dns and not _canonical_ok(rep):
                rep.fatal(
                    "dns_resolution_failed",
                    sec,
                    {"host": host, "detail": res},
                    hint="DNS/network issue. Verify outbound access.",
                )
            else:
                rep.warn(
                    "dns_resolution_failed",
                    sec,
                    {"host": host, "detail": res},
                    hint="DNS/network issue. Verify outbound access.",
                )

    out["ok"] = _bool01(not any_fail)
    rep.info["dns"] = out


# =============================================================================
# env file validation (NEVER echo raw secrets/values)
# =============================================================================


def _strip_inline_comment(raw: str) -> str:
    s = raw
    in_squote = False
    in_dquote = False
    out_chars: list[str] = []
    idx = 0
    while idx < len(s):
        ch = s[idx]
        if ch == "'" and not in_dquote:
            in_squote = not in_squote
            out_chars.append(ch)
            idx += 1
            continue
        if ch == '"' and not in_squote:
            in_dquote = not in_dquote
            out_chars.append(ch)
            idx += 1
            continue
        if ch == "#" and not in_squote and not in_dquote:
            break
        out_chars.append(ch)
        idx += 1
    return "".join(out_chars).rstrip()


def _env_file_path_default() -> str:
    p = _envs("CHECK_ENV_ENV_FILE", "")
    if p:
        return p
    rr = Path(_REPO_ROOT) if _REPO_ROOT else Path.cwd()
    ex = rr / ".env.example"
    if ex.exists():
        return str(ex)
    dot = rr / ".env"
    if dot.exists():
        return str(dot)
    return str(ex)


def _parse_env_file_keys(path_s: str) -> tuple[dict[str, list[int]], list[dict[str, Any]]]:
    """
    Parse key occurrences and structural issues WITHOUT recording raw line contents.
    """
    key_lines: dict[str, list[int]] = {}
    issues: list[dict[str, Any]] = []

    p = Path(path_s)
    try:
        txt = p.read_text(encoding="utf-8", errors="replace")
    except FileNotFoundError:
        issues.append({"line": 0, "kind": "missing_file"})
        return key_lines, issues
    except Exception as exc:
        issues.append(
            {
                "line": 0,
                "kind": "read_error",
                "error": f"{type(exc).__name__}:{_sanitize_text(exc)}",
            }
        )
        return key_lines, issues

    for line_no, raw in enumerate(txt.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        cooked = _strip_inline_comment(raw).strip()
        if not cooked:
            continue

        if cooked.lower().startswith("export "):
            cooked = cooked[7:].strip()

        if "=" not in cooked:
            issues.append({"line": line_no, "kind": "no_equals"})
            continue

        key = cooked.split("=", 1)[0].strip()
        if not key:
            issues.append({"line": line_no, "kind": "blank_key"})
            continue
        if not _ENV_KEY_RE.match(key):
            issues.append({"line": line_no, "kind": "bad_key", "key": _sanitize_text(key[:200])})
        key_lines.setdefault(key, []).append(line_no)

    return key_lines, issues


def _validate_env_file(rep: Report) -> None:
    sec = "env_file"
    if not _envb("CHECK_ENV_VALIDATE_ENV", False):
        rep.info["env_file"] = {"enabled": 0}
        return

    path_s = _envs("CHECK_ENV_ENV_FILE", "") or _env_file_path_default()
    key_lines, issues = _parse_env_file_keys(path_s)

    dupes = {k: v for k, v in key_lines.items() if len(v) >= 2}
    bad_keys = [it for it in issues if it.get("kind") in {"bad_key", "blank_key"}]
    noeq = [it for it in issues if it.get("kind") == "no_equals"]
    missing = [it for it in issues if it.get("kind") == "missing_file"]
    readerr = [it for it in issues if it.get("kind") == "read_error"]

    rep.info["env_file"] = {
        "enabled": 1,
        "path": _sanitize_text(path_s),
        "keys": len(key_lines),
        "dupe_keys": len(dupes),
        "issues": issues[:200],
    }

    if missing:
        rep.warn(
            "env_file_missing",
            sec,
            {"path": _sanitize_text(path_s)},
            hint="If you want env-file validation, ensure the file exists.",
        )
        return
    if readerr:
        rep.warn("env_file_read_error", sec, {"path": _sanitize_text(path_s), "issue": readerr[:1]})

    if bad_keys:
        rep.warn(
            "env_file_bad_keys",
            sec,
            {"count": len(bad_keys), "examples": bad_keys[:5]},
            hint="Env keys should match ^[A-Za-z_][A-Za-z0-9_]*$",
        )

    if noeq:
        rep.warn(
            "env_file_lines_no_equals",
            sec,
            {"count": len(noeq), "examples": noeq[:10]},
            hint="Use KEY=value lines (comments start with #).",
        )

    strict = _envb("CHECK_ENV_STRICT", False) or _envb("STRICT_ENV", False)
    if dupes:
        detail = {"count": len(dupes), "dupes": {k: v[:10] for k, v in list(dupes.items())[:120]}}
        if strict and not _canonical_ok(rep):
            rep.fatal(
                "env_file_duplicate_keys", sec, detail, hint="Remove duplicate key assignments."
            )
        else:
            rep.warn(
                "env_file_duplicate_keys",
                sec,
                detail,
                hint="Duplicates found. In strict mode this may become fatal.",
            )


# =============================================================================
# diagnostics (non-canonical checks; warn-first if canonical OK)
# =============================================================================


def _validate_urls_basic(rep: Report) -> None:
    sec = "urls"
    jup_base = _envs(
        "JUP_BASE_URL", _envs("JUPITER_BASE_URL", "https://api.jup.ag") or "https://api.jup.ag"
    )
    if _placeholderish(jup_base):
        _warn_or_fatal(
            rep,
            "jup_base_url_placeholder",
            sec,
            {"JUP_BASE_URL": _mask_value("JUP_BASE_URL", jup_base)},
        )

    if jup_base and "://" in jup_base and jup_base.startswith("http://"):
        host = _host_from_urlish(jup_base)
        if host not in {"127.0.0.1", "localhost"} and not _envb("JUP_ALLOW_HTTP_NONLOCAL", False):
            rep.warn(
                "jup_http_nonlocal",
                sec,
                {"host": host},
                hint="Prefer https or set JUP_ALLOW_HTTP_NONLOCAL=1",
            )

    rpc = _envs("SOL_RPC_URL", _envs("RPC_URL", ""))
    if rpc and _placeholderish(rpc):
        _warn_or_fatal(rep, "rpc_placeholder", sec, {"rpc": _mask_value("SOL_RPC_URL", rpc)})

    dsn = _envs("DATABASE_URL", _envs("POSTGRES_DSN", _envs("PG_DSN", "")))
    if (
        dsn
        and _host_from_urlish(dsn).endswith(".render.com")
        and _parse_qs_sslmode(dsn) != "require"
    ):
        rep.warn(
            "render_dsn_missing_sslmode",
            sec,
            {"dsn": _mask_value("DATABASE_URL", dsn)},
            hint="Add ?sslmode=require (or rely on start.sh normalization).",
        )

    rep.info["urls"] = {
        "JUP_BASE_URL": _mask_value("JUP_BASE_URL", jup_base),
        "SOL_RPC_URL": _mask_value("SOL_RPC_URL", rpc) if rpc else "",
        "DATABASE_URL": _mask_value("DATABASE_URL", dsn) if dsn else "",
    }


def _validate_jupiter_endpoints(rep: Report) -> None:
    """
    Local heuristic checks (warning by default) that DO NOT drift/override config.py.
    We only ensure the *shape* looks sane and provide actionable hints.

    NOTE: /swap/v1/quote is valid for Jupiter Swap API v1 quoting. Do NOT treat "swap" in path as wrong.
    """
    sec = "jupiter"

    quote_api = _envs("JUPITER_QUOTE_API", "")
    swap_api = _envs("JUPITER_SWAP_API", "")

    def _endswith_path(url: str, suffix: str) -> bool:
        try:
            p = urlparse(url).path or ""
            return p.rstrip("/").endswith(suffix.rstrip("/"))
        except Exception:
            return url.rstrip("/").endswith(suffix.rstrip("/"))

    out: dict[str, Any] = {
        "JUPITER_QUOTE_API": _mask_value("JUPITER_QUOTE_API", quote_api) if quote_api else "",
        "JUPITER_SWAP_API": _mask_value("JUPITER_SWAP_API", swap_api) if swap_api else "",
        "quote_ok": None,
        "swap_ok": None,
        "notes": [],
    }

    if quote_api:
        ok_quote = _endswith_path(quote_api, "/quote")
        out["quote_ok"] = _bool01(ok_quote)
        if not ok_quote:
            _warn_or_fatal(
                rep,
                "jupiter_quote_api_suspicious",
                sec,
                {"JUPITER_QUOTE_API": _mask_value("JUPITER_QUOTE_API", quote_api)},
                hint="Expected quote endpoint path to end with /quote (e.g. https://api.jup.ag/swap/v1/quote).",
            )

    if swap_api:
        ok_swap = _endswith_path(swap_api, "/swap")
        out["swap_ok"] = _bool01(ok_swap)
        if not ok_swap:
            _warn_or_fatal(
                rep,
                "jupiter_swap_api_suspicious",
                sec,
                {"JUPITER_SWAP_API": _mask_value("JUPITER_SWAP_API", swap_api)},
                hint="Expected swap endpoint path to end with /swap (e.g. https://api.jup.ag/swap/v1/swap).",
            )

    if quote_api and swap_api and quote_api.rstrip("/") == swap_api.rstrip("/"):
        rep.warn(
            "jupiter_quote_swap_equal",
            sec,
            {"note": "Quote and swap URLs are identical; this is usually wrong."},
        )

    rep.info["jupiter"] = out


def _validate_integrations(rep: Report) -> None:
    sec = "integrations"

    notion_enabled = _envb("NOTION_LOG_ENABLED", False) or _envb("NOTION_ENABLED", False)
    if notion_enabled and (not _envs("NOTION_TOKEN", "") or not _envs("NOTION_DATABASE_ID", "")):
        rep.warn(
            "notion_missing_creds",
            sec,
            "Notion enabled but NOTION_TOKEN/NOTION_DATABASE_ID not set",
        )

    tg_enabled = _envb("TELEGRAM_ENABLED", False) or bool(_envs("TELEGRAM_CHAT_ID", ""))
    if tg_enabled and not _envs("TELEGRAM_BOT_TOKEN", ""):
        rep.warn("telegram_missing_token", sec, "Telegram enabled but TELEGRAM_BOT_TOKEN not set")

    discord_enabled = _envb("DISCORD_ENABLED", False) or bool(_envs("DISCORD_WEBHOOK_URL", ""))
    if discord_enabled and (
        not _envs("DISCORD_WEBHOOK_URL", "") and not _envs("DISCORD_TOKEN", "")
    ):
        rep.warn(
            "discord_missing_creds",
            sec,
            "Discord enabled but missing DISCORD_WEBHOOK_URL or DISCORD_TOKEN",
        )

    ai_enabled = _envb("AI_ENABLED", False) or _envb("OPENAI_ENABLED", False)
    if ai_enabled and not _envs("OPENAI_API_KEY", ""):
        rep.warn("openai_missing_key", sec, "AI enabled but OPENAI_API_KEY not set")

    redis_url = _envs("REDIS_URL", _envs("REDIS_TLS_URL", ""))
    if redis_url and not (redis_url.startswith("redis://") or redis_url.startswith("rediss://")):
        rep.warn("redis_url_suspicious", sec, {"REDIS_URL": _mask_value("REDIS_URL", redis_url)})

    rep.info["integrations"] = {
        "notion_enabled": _bool01(notion_enabled),
        "telegram_enabled": _bool01(tg_enabled),
        "discord_enabled": _bool01(discord_enabled),
        "ai_enabled": _bool01(ai_enabled),
        "redis_set": _bool01(bool(redis_url)),
    }


def _validate_mints(rep: Report) -> None:
    sec = "mints"
    sol = (_envs("SOL_MINT", KNOWN_SOL_MINT) or KNOWN_SOL_MINT).strip()
    usdc_raw = (_envs("USDC_MINT", KNOWN_USDC_MINT) or KNOWN_USDC_MINT).strip()
    usdc = KNOWN_USDC_MINT if usdc_raw == KNOWN_BAD_USDC_TYPO else usdc_raw

    rep.info["mints"] = {
        "SOL_MINT": sol,
        "USDC_MINT": usdc,
        "SOL_MINT_is_valid": _bool01(_looks_like_mint(sol)),
        "USDC_MINT_is_valid": _bool01(_looks_like_mint(usdc)),
        "USDC_MINT_was_typo": _bool01(usdc_raw == KNOWN_BAD_USDC_TYPO),
    }

    if not _looks_like_mint(sol):
        rep.warn("sol_mint_invalid", sec, {"SOL_MINT": sol})
    if not _looks_like_mint(usdc):
        rep.warn("usdc_mint_invalid", sec, {"USDC_MINT": usdc})
    if usdc_raw == KNOWN_BAD_USDC_TYPO:
        rep.warn("usdc_mint_typo", sec, {"bad": KNOWN_BAD_USDC_TYPO, "fixed_to": KNOWN_USDC_MINT})
    if sol == usdc:
        rep.warn(
            "mints_equal",
            sec,
            {"note": "SOL_MINT and USDC_MINT are equal; this is almost certainly wrong."},
        )


def _validate_logging(rep: Report) -> None:
    sec = "logging"
    lvl = _envs("LOG_LEVEL", "INFO").upper()
    if lvl and lvl not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        rep.warn(
            "log_level_unknown",
            sec,
            {"LOG_LEVEL": _sanitize_text(lvl)},
            hint="Set LOG_LEVEL to INFO/DEBUG/WARNING/ERROR.",
        )
    rep.info["logging"] = {
        "LOG_LEVEL": _sanitize_text(lvl),
        "LOG_JSON": _mask_value("LOG_JSON", os.getenv("LOG_JSON")),
        "LOG_TIME_UTC": _mask_value("LOG_TIME_UTC", os.getenv("LOG_TIME_UTC")),
        "LOG_INCLUDE_SRC": _mask_value("LOG_INCLUDE_SRC", os.getenv("LOG_INCLUDE_SRC")),
    }


def _env_snapshot(rep: Report) -> None:
    if not _envb("CHECK_ENV_SHOW_ENV", False):
        return

    keys = sorted(
        {
            "MASTER_PHASE",
            "STRATEGY_PHASE",
            "DRYRUN",
            "KILL_SWITCH",
            "DISABLE_BUYS",
            "DISABLE_SELLS",
            "DISABLE_SCANNERS",
            "SCANNER_MODULES",
            "SOL_RPC_URL",
            "RPC_URL",
            "SOL_RPC_URLS",
            "JUP_BASE_URL",
            "JUP_ALT_BASE_URLS",
            "JUPITER_QUOTE_API",
            "JUPITER_SWAP_API",
            "PF_TRADE_SOL",
            "BUY_SOL_PER_TRADE",
            "ENTRY_MIN_INITIAL_BUY_SOL",
            "PF_SLIPPAGE_BPS",
            "SLIPPAGE_BPS",
            "JUP_SLIPPAGE_BPS",
            "EXIT_STOP_LOSS_PCT",
            "SKIP_DSN_REQUIRED",
            "DATABASE_URL",
            "POSTGRES_DSN",
            "PG_DSN",
            "PNL_DSN",
            "NOTION_LOG_ENABLED",
            "NOTION_ENABLED",
            "NOTION_TOKEN",
            "NOTION_DATABASE_ID",
            "TELEGRAM_ENABLED",
            "TELEGRAM_BOT_TOKEN",
            "TELEGRAM_CHAT_ID",
            "DISCORD_ENABLED",
            "DISCORD_WEBHOOK_URL",
            "DISCORD_TOKEN",
            "OPENAI_API_KEY",
            "AI_ENABLED",
            "REDIS_URL",
            "REDIS_TLS_URL",
            "SOL_MINT",
            "USDC_MINT",
            "CHECK_ENV_CONFIG_MODULE",
            "KEYPAIR_PATH",
        }
    )

    snap: dict[str, Any] = {}
    for key in keys:
        snap[key] = _mask_value(key, os.getenv(key))
    rep.info["env_snapshot"] = snap


# =============================================================================
# strict policy (warning promotion)
# =============================================================================


def _apply_strict(rep: Report) -> None:
    if not _envb("CHECK_ENV_STRICT", False) and not _envb("STRICT_ENV", False):
        return

    promote = {
        "render_dsn_missing_sslmode",
        "jup_http_nonlocal",
        "dns_resolution_failed",
        "python_unbuffered_not_set",
        "env_file_duplicate_keys",
        "rpc_placeholder",
        "jup_base_url_placeholder",
        "jupiter_quote_api_suspicious",
        "jupiter_swap_api_suspicious",
    }
    promote |= set(_parse_csv(_envs("CHECK_ENV_STRICT_PROMOTE", "")))

    keep_warns: list[Issue] = []
    for w in rep.warns:
        if w.code in promote:
            rep.fatal(f"strict_{w.code}", w.section, w.detail, hint=w.hint)
        else:
            keep_warns.append(w)
    rep.warns = keep_warns


# =============================================================================
# runner (public APIs)
# =============================================================================


def run_checks() -> Report:
    _refresh_redaction()
    rep = Report(ok=True)

    _safe_section(rep, "runtime", lambda: _validate_runtime(rep), critical=True)
    if _maybe_failfast(rep):
        _finalize(rep)
        return rep

    _safe_call(rep, "config", lambda: _validate_config_canonical(rep), critical=True)
    if _maybe_failfast(rep):
        _finalize(rep)
        return rep

    _safe_section(rep, "urls", lambda: _validate_urls_basic(rep), critical=False)
    _safe_section(rep, "jupiter", lambda: _validate_jupiter_endpoints(rep), critical=False)
    _safe_section(rep, "integrations", lambda: _validate_integrations(rep), critical=False)
    _safe_section(rep, "mints", lambda: _validate_mints(rep), critical=False)
    _safe_section(rep, "logging", lambda: _validate_logging(rep), critical=False)
    _safe_section(rep, "dns", lambda: _dns_checks(rep), critical=False)
    _safe_section(rep, "env_file", lambda: _validate_env_file(rep), critical=False)
    _safe_section(rep, "env_snapshot", lambda: _env_snapshot(rep), critical=False)
    _safe_section(rep, "strict", lambda: _apply_strict(rep), critical=False)

    _finalize(rep)
    return rep


def _issues_to_dict(items: list[Issue]) -> list[dict[str, Any]]:
    return [
        {"code": i.code, "section": i.section, "detail": i.detail, "hint": i.hint} for i in items
    ]


def _issues_to_text(items: list[Issue], *, cap: int) -> list[str]:
    out: list[str] = []
    for i in items[:cap]:
        if i.hint:
            out.append(_sanitize_text(f"{i.section}.{i.code}: {i.hint}"))
        else:
            out.append(_sanitize_text(f"{i.section}.{i.code}"))
    return out


def _collect_fix_hints(rep: Report) -> list[str]:
    hints: list[str] = []
    for it in rep.fatals + rep.warns:
        if it.hint and it.hint.strip():
            hints.append(_sanitize_text(it.hint.strip()))

    out: list[str] = []
    seen: set[str] = set()
    for h in hints:
        if h and h not in seen:
            out.append(h)
            seen.add(h)
        if len(out) >= 18:
            break
    return out


def report_as_json(rep: Report) -> dict[str, Any]:
    fix_hints = _collect_fix_hints(rep)
    return {
        "ts": _ts(),
        "ok": bool(rep.ok),
        "exit_code": RC_OK if rep.ok else RC_FATAL,
        "fatals": _issues_to_dict(rep.fatals),
        "warnings": _issues_to_dict(rep.warns),
        "warnings_text": _issues_to_text(rep.warns, cap=80),
        "fatals_text": _issues_to_text(rep.fatals, cap=80),
        "fix_hints": fix_hints,
        "info": rep.info,
        "contract": {
            "script": SCRIPT_ID,
            "version": VERSION,
            "no_network_default": True,
            "exit_codes": {"ok": RC_OK, "fatal": RC_FATAL},
            "canonical_source": "config.py validate_environment/validate_config(...)",
            "drift_policy": "If canonical config ok, diagnostics are warnings unless strict promotion.",
            "config_detail_default": 0,
            "config_detail_toggle": "CHECK_ENV_INCLUDE_CONFIG_DETAIL=1",
        },
    }


def validate_environment() -> tuple[bool, dict[str, Any]]:
    rep = run_checks()
    payload = report_as_json(rep)
    return bool(rep.ok), payload


# =============================================================================
# CLI
# =============================================================================


class _ArgParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # noqa: D401
        raise ValueError(message)


def _make_parser() -> argparse.ArgumentParser:
    ap = _ArgParser(description="God-Tier env validator (Phase 0–7 LOCK-IN).", add_help=False)
    ap.add_argument(
        "--json", action="store_true", help="Emit JSON to stdout (default true via env)."
    )
    ap.add_argument("--json-only", action="store_true", help="Only JSON, no human stderr summary.")
    ap.add_argument(
        "--dns",
        action="store_true",
        help="Run DNS checks (warn-only unless CHECK_ENV_DNS_REQUIRED=1).",
    )
    ap.add_argument("--strict", action="store_true", help="Promote selected warnings to fatal.")
    ap.add_argument("--show-env", action="store_true", help="Include masked env snapshot in JSON.")
    ap.add_argument("--failfast", action="store_true", help="Stop after first fatal section.")
    ap.add_argument(
        "--validate-env",
        action="store_true",
        help="Validate env file (.env/.env.example) for duplicates/format issues.",
    )
    ap.add_argument(
        "--env-file",
        default="",
        help="Env file to validate (and optionally pass to config loader).",
    )
    ap.add_argument(
        "--skip-config", action="store_true", help="Skip config import/validation (debug only)."
    )
    ap.add_argument(
        "--require-config", action="store_true", help="Force config validation required."
    )
    ap.add_argument(
        "--config-module",
        default="",
        help="Config module import path (sets CHECK_ENV_CONFIG_MODULE).",
    )
    ap.add_argument("-h", "--help", action="store_true", help="Show help and exit 0.")
    return ap


def _parse_args(argv: list[str]) -> argparse.Namespace:
    ap = _make_parser()
    return ap.parse_args(argv)


def _apply_cli_env_overrides(argv: list[str]) -> None:
    if "--json" in argv:
        os.environ["CHECK_ENV_JSON"] = "1"
    if "--json-only" in argv:
        os.environ["CHECK_ENV_JSON_ONLY"] = "1"
        os.environ["CHECK_ENV_JSON"] = "1"
    if "--dns" in argv:
        os.environ["CHECK_ENV_DNS"] = "1"
    if "--strict" in argv:
        os.environ["CHECK_ENV_STRICT"] = "1"
    if "--show-env" in argv:
        os.environ["CHECK_ENV_SHOW_ENV"] = "1"
    if "--failfast" in argv:
        os.environ["CHECK_ENV_FAILFAST"] = "1"
    if "--validate-env" in argv:
        os.environ["CHECK_ENV_VALIDATE_ENV"] = "1"
    if "--skip-config" in argv:
        os.environ["CHECK_ENV_SKIP_CONFIG"] = "1"
    if "--require-config" in argv:
        os.environ["CHECK_ENV_REQUIRE_CONFIG"] = "1"

    if "--env-file" in argv:
        try:
            idx = argv.index("--env-file")
            if idx + 1 < len(argv):
                os.environ["CHECK_ENV_ENV_FILE"] = argv[idx + 1]
        except Exception:
            pass
    for a in argv:
        if a.startswith("--env-file="):
            os.environ["CHECK_ENV_ENV_FILE"] = a.split("=", 1)[1].strip()
            break

    if "--config-module" in argv:
        try:
            idx = argv.index("--config-module")
            if idx + 1 < len(argv):
                os.environ["CHECK_ENV_CONFIG_MODULE"] = argv[idx + 1]
        except Exception:
            pass
    for a in argv:
        if a.startswith("--config-module="):
            os.environ["CHECK_ENV_CONFIG_MODULE"] = a.split("=", 1)[1].strip()
            break


def _report_unhandled_exception(exc: BaseException) -> Report:
    _refresh_redaction()
    rep = Report(ok=False)
    try:
        _validate_runtime(rep)
    except Exception:
        rep.info.setdefault("runtime", {})

    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    tb = _sanitize_text(tb)

    rep.fatal(
        "unhandled_exception",
        "runtime",
        {"type": type(exc).__name__, "error": _sanitize_text(exc), "trace": tb[-6000:]},
        hint="Fix the exception above. check_env must never crash and must always return JSON + exit_code.",
    )
    _finalize(rep)
    return rep


def _report_cli_error(msg: str, argv: list[str]) -> Report:
    _refresh_redaction()
    rep = Report(ok=False)
    _safe_section(rep, "runtime", lambda: _validate_runtime(rep), critical=False)

    rep.fatal(
        "cli_parse_error",
        "cli",
        {"message": _sanitize_text(msg), "argv": [_sanitize_text(a) for a in argv[:200]]},
        hint="Fix the CLI flags (preflight expects scripts/check_env.py --json-only to work).",
    )
    _finalize(rep)
    return rep


def cli(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding=stream.encoding or "utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass

    argsv = list(sys.argv[1:] if argv is None else argv)

    _apply_cli_env_overrides(argsv)
    _refresh_redaction()

    try:
        args = _parse_args(argsv)
    except Exception as exc:
        rep = _report_cli_error(str(exc), argsv)
        emit_json = _envb("CHECK_ENV_JSON", True)
        json_only = _envb("CHECK_ENV_JSON_ONLY", False)
        if emit_json:
            _safe_dump_json(report_as_json(rep), json_only=json_only)
        if not json_only:
            _safe_write_line(sys.stderr, "")
            _safe_write_line(sys.stderr, "check_env: FAILED (cli)")
            _safe_write_line(sys.stderr, f"  error: {_sanitize_text(exc)}")
        return RC_FATAL

    if getattr(args, "help", False):
        emit_json = _envb("CHECK_ENV_JSON", True) or args.json or args.json_only
        json_only = _envb("CHECK_ENV_JSON_ONLY", False) or args.json_only
        if emit_json:
            rep = Report(ok=True)
            _safe_section(rep, "runtime", lambda: _validate_runtime(rep), critical=False)
            rep.info["summary"] = {"fatals": 0, "warnings": 0, "phase": _phase_from_env()}
            _safe_dump_json(report_as_json(rep), json_only=json_only)
        if not json_only:
            _safe_write_line(sys.stderr, "")
            _safe_write_line(sys.stderr, "check_env.py usage:")
            _safe_write_line(sys.stderr, _make_parser().format_help().rstrip())
        return RC_OK

    if args.json:
        os.environ["CHECK_ENV_JSON"] = "1"
    if args.json_only:
        os.environ["CHECK_ENV_JSON_ONLY"] = "1"
        os.environ["CHECK_ENV_JSON"] = "1"
    if args.dns:
        os.environ["CHECK_ENV_DNS"] = "1"
    if args.strict:
        os.environ["CHECK_ENV_STRICT"] = "1"
    if args.show_env:
        os.environ["CHECK_ENV_SHOW_ENV"] = "1"
    if args.failfast:
        os.environ["CHECK_ENV_FAILFAST"] = "1"
    if args.validate_env:
        os.environ["CHECK_ENV_VALIDATE_ENV"] = "1"
    if args.env_file:
        os.environ["CHECK_ENV_ENV_FILE"] = args.env_file
    if args.skip_config:
        os.environ["CHECK_ENV_SKIP_CONFIG"] = "1"
    if args.require_config:
        os.environ["CHECK_ENV_REQUIRE_CONFIG"] = "1"
    if getattr(args, "config_module", ""):
        os.environ["CHECK_ENV_CONFIG_MODULE"] = str(args.config_module).strip()

    _refresh_redaction()

    try:
        rep = run_checks()
    except BaseException as exc:
        rep = _report_unhandled_exception(exc)

    emit_json = _envb("CHECK_ENV_JSON", True)
    json_only = _envb("CHECK_ENV_JSON_ONLY", False)

    if emit_json:
        _safe_dump_json(report_as_json(rep), json_only=json_only)

    if not json_only:
        _safe_write_line(sys.stderr, "")
        _safe_write_line(sys.stderr, f"check_env: {'OK' if rep.ok else 'FAILED'}")
        _safe_write_line(sys.stderr, f"  repo_root: {_sanitize_text(_REPO_ROOT)}")

        summ = rep.info.get("summary") or {}
        _safe_write_line(
            sys.stderr,
            "  phase: {phase} | buys_possible={buys_possible} | scanning_possible={scanning_possible} | "
            "db_required={db_required} | config_required={config_required} | canonical_config_ok={canonical_config_ok}".format(
                phase=summ.get("phase"),
                buys_possible=summ.get("buys_possible"),
                scanning_possible=summ.get("scanning_possible"),
                db_required=summ.get("db_required"),
                config_required=summ.get("config_required"),
                canonical_config_ok=summ.get("canonical_config_ok"),
            ),
        )

        if rep.fatals:
            _safe_write_line(sys.stderr, "")
            for f in rep.fatals[:200]:
                _safe_write_line(
                    sys.stderr, f"  - FATAL {f.section}.{f.code}: {_sanitize_text(f.detail)}"
                )
                if f.hint:
                    _safe_write_line(sys.stderr, f"           hint: {_sanitize_text(f.hint)}")

        if rep.warns:
            _safe_write_line(sys.stderr, "")
            _safe_write_line(sys.stderr, "warnings:")
            for w in rep.warns[:280]:
                _safe_write_line(
                    sys.stderr, f"  - WARN  {w.section}.{w.code}: {_sanitize_text(w.detail)}"
                )
                if w.hint:
                    _safe_write_line(sys.stderr, f"           hint: {_sanitize_text(w.hint)}")

    return RC_OK if rep.ok else RC_FATAL


def main() -> None:
    sys.exit(cli())


if __name__ == "__main__":
    main()
