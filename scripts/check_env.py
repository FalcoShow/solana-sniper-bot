from __future__ import annotations

"""
scripts/check_env.py — God-Tier Env Validator (P0 LOCK-IN, Phase 0–7)
=====================================================================

Single source of truth to validate runtime environment BEFORE booting the bot.

Hard guarantees
---------------
- NO network calls by default (optional DNS-only via --dns or CHECK_ENV_DNS=1)
- NO secrets leak:
    * aggressive masking by key-name
    * URL credential masking
    * query-param masking
    * header token masking
    * exact secret value replacement (env-values)
    * optional long-token masking (keeps Solana mints visible)
- Crash-proof: never throws; emits stable JSON when requested
- Tooling-friendly:
    - JSON -> STDOUT
    - Human summary -> STDERR (suppressed when JSON-only)

Exit codes
----------
0  OK (no fatals)
1  Fatal issues found (including CLI parse errors)

CLI flags override env toggles:
  --json         => CHECK_ENV_JSON=1
  --json-only    => CHECK_ENV_JSON_ONLY=1 and CHECK_ENV_JSON=1
  --dns          => CHECK_ENV_DNS=1
  --strict       => CHECK_ENV_STRICT=1
  --show-env     => CHECK_ENV_SHOW_ENV=1
  --failfast     => CHECK_ENV_FAILFAST=1

Env knobs (optional)
--------------------
CHECK_ENV_WARN_ONLY=code1,code2,...
CHECK_ENV_STRICT_PROMOTE=codeX,codeY,...         (add to default strict promotions)
CHECK_ENV_REDACT_KEYS=K1,K2,...                  (extra env keys to redact by exact value)
CHECK_ENV_MASK_LONG_TOKENS=0|1                   (defaults to PREFLIGHT_MASK_LONG_TOKENS if present else 1)
CHECK_ENV_ALLOW_MISSING_KEYFILES=0|1             (treat missing keypair paths as warn-only)
RPC_ALLOW_HTTP_NONLOCAL=0|1                      (warn if RPC is http:// nonlocal unless set)
JUP_ALLOW_HTTP_NONLOCAL=0|1                      (warn if Jupiter is http:// nonlocal unless set)

Notes
-----
This file is intentionally dependency-free (stdlib only) and safe to run on:
- local Windows shells (PowerShell, cmd)
- Render containers (Linux)
"""

import argparse
import base64
import binascii
import json
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

    # harmless; helps path resolution for dev tooling
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


_REPO_ROOT = _bootstrap_repo_root()

# =============================================================================
# constants / parsing
# =============================================================================
_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

RC_OK = 0
RC_FATAL = 1  # IMPORTANT: 0/1 contract

VERSION = "v3"
SCRIPT_ID = "scripts.check_env"

# masks and heuristics
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")  # scheme://user:pass@ -> scheme://***@
_APIKEY_HDR_RE = re.compile(r"(?i)(x-api-key:\s*)([^\s]+)")
_BEARER_RE = re.compile(r"(?i)(authorization:\s*bearer\s+)([^\s]+)")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)
_QP_SECRET_RE = re.compile(r"(?i)(password|pass|token|api_key|apikey|secret)=([^&\s]+)")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")

_BASE58_ALPH = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

# Solana canonical mints
KNOWN_SOL_MINT = "So11111111111111111111111111111111111111112"
KNOWN_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
# common typo seen in wild (one char/segment off)
KNOWN_BAD_USDC_TYPO = "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v"


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


def _clamp_int(x: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(x)))
    except Exception:
        return lo


def _bool01(x: bool) -> int:
    return 1 if bool(x) else 0


def _safe_write_line(stream, s: str) -> None:
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


# =============================================================================
# redaction / safety
# =============================================================================
def _is_sensitive_key(k: str) -> bool:
    kk = (k or "").upper()
    return any(x in kk for x in ("KEY", "TOKEN", "SECRET", "PASSWORD", "PASS", "PRIVATE"))


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
    # prefer explicit check_env knob, else inherit preflight knob, else default True
    if "CHECK_ENV_MASK_LONG_TOKENS" in os.environ:
        return _envb("CHECK_ENV_MASK_LONG_TOKENS", True)
    if "PREFLIGHT_MASK_LONG_TOKENS" in os.environ:
        return _envb("PREFLIGHT_MASK_LONG_TOKENS", True)
    return True


def _redact_keys() -> list[str]:
    """
    Keys whose *values* must never appear in logs or JSON outputs.
    (We also replace exact secret values regardless of key name.)
    """
    base = [
        # Jupiter / routing
        "JUP_PRO_KEY",
        "JUP_API_KEY",
        "JUP_KEY",
        # AI
        "OPENAI_API_KEY",
        # Alerts
        "TELEGRAM_BOT_TOKEN",
        "DISCORD_TOKEN",
        "DISCORD_WEBHOOK_URL",
        "NOTION_TOKEN",
        # Wallet material
        "WALLET_JSON",
        "WALLET_JSON_B64",
        "WALLET_PRIVATE_KEY",
        "KEYPAIR_B64",
        "KEYPAIR_PATH",
        # DSNs / URLs (contain creds)
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
    ]
    extra = _parse_csv(_envs("CHECK_ENV_REDACT_KEYS", ""))
    out = base + extra
    seen: set[str] = set()
    uniq: list[str] = []
    for k in out:
        if k and k not in seen:
            uniq.append(k)
            seen.add(k)
    return uniq


def _mask_tokenish_match(m: re.Match) -> str:
    tok = m.group(0)
    # do NOT mask public Solana mints (we want those visible)
    if _looks_like_mint(tok):
        return tok
    return "***"


def _sanitize_text(text: Any) -> str:
    """
    Sanitize arbitrary text for logs:
      - URL creds masking
      - header token masking
      - key=value secret masking
      - query-parameter secret masking
      - exact env secret value replacement
      - conservative long-token masking (optional; preserves mint-like strings)
    """
    if text is None:
        return ""
    s = str(text)

    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _APIKEY_HDR_RE.sub(r"\1***", s)
    s = _BEARER_RE.sub(r"\1***", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)
    s = _QP_SECRET_RE.sub(r"\1=***", s)

    # replace exact secret env values (strongest guarantee)
    for k in _redact_keys():
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _mask_long_tokens_enabled():
        s = _LONG_TOKENISH_RE.sub(_mask_tokenish_match, s)

    return s


def _mask_value(k: str, v: str | None) -> Any:
    """
    Mask values for JSON snapshots (never leak secrets).
    - Sensitive keys -> "***"
    - URLs -> creds masked + query trimmed
    - Long strings -> truncated
    """
    if v is None:
        return None
    vv = _strip_wrapping_quotes(str(v))
    if vv == "":
        return ""
    if _is_sensitive_key(k) or k in _redact_keys():
        return "***"
    if "://" in vv:
        return _mask_urlish(vv)
    if len(vv) > 240:
        return vv[:240] + "…"
    return vv


# =============================================================================
# phase / requirement policy
# =============================================================================
def _phase() -> int:
    p = _envi("MASTER_PHASE", _envi("STRATEGY_PHASE", 7))
    return _clamp_int(p, 0, 7)


def _buys_effectively_possible() -> bool:
    """
    Only hard-require wallet when buys are realistically possible:
      - not DRYRUN
      - not KILL_SWITCH / DISABLE_BUYS
      - phase >= 2 (routing/execution)
    """
    if _envb("DRYRUN", False):
        return False
    if _envb("KILL_SWITCH", False) or _envb("DISABLE_BUYS", False):
        return False
    return _phase() >= 2


def _scanning_effectively_possible() -> bool:
    if _envb("KILL_SWITCH", False) or _envb("DISABLE_SCANNERS", False):
        return False
    return _phase() >= 1


def _db_required() -> bool:
    # allow fully scanner-only mode without db in dev (explicit)
    return not _envb("SKIP_DSN_REQUIRED", False)


def _warn_only_codes() -> set[str]:
    return set(_parse_csv(_envs("CHECK_ENV_WARN_ONLY", "")))


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
        "example",
        "none",
        "null",
        "todo",
        "tbd",
    }
    if s in bad:
        return True
    return "example" in s


# =============================================================================
# helpers (paths / urls / dsns)
# =============================================================================
def _looks_like_json_blob(s: str) -> bool:
    ss = (s or "").lstrip()
    return ss.startswith("{") or ss.startswith("[")


def _looks_like_path(s: str) -> bool:
    ss = (s or "").strip()
    if not ss:
        return False
    if "/" in ss or "\\" in ss:
        return True
    return ss.lower().endswith((".json", ".key", ".keypair", ".txt"))


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


def _normalize_mint(m: str) -> str:
    mm = (m or "").strip()
    if mm == KNOWN_BAD_USDC_TYPO:
        return KNOWN_USDC_MINT
    return mm


def _decode_b64_preview(s: str, max_bytes: int = 65536) -> tuple[bool, str]:
    """
    Best-effort base64 decode check. No secrets output.
    Returns (ok, note).
    """
    if not s:
        return False, "empty"
    try:
        raw = base64.b64decode(s, validate=False)
        if len(raw) > max_bytes:
            return True, f"decoded_ok_bytes>{max_bytes}"
        try:
            json.loads(raw.decode("utf-8", "ignore"))
            return True, "decoded_ok_json_ok"
        except Exception:
            return True, "decoded_ok_json_unparsed"
    except (binascii.Error, ValueError):
        return False, "b64_decode_failed"
    except Exception:
        return False, "b64_decode_failed_other"


def _looks_like_base58_secret(s: str) -> bool:
    ss = (s or "").strip()
    if len(ss) < 60:
        return False
    return all(ch in _BASE58_ALPH for ch in ss)


def _file_exists_anywhere(p: str) -> bool:
    """
    Check existence:
      - absolute as-is
      - relative to cwd
      - relative to REPO_ROOT
    """
    try:
        if not p:
            return False
        pp = Path(p)
        if pp.is_absolute():
            return pp.exists()
        if (Path.cwd() / pp).exists():
            return True
        rr = Path(os.getenv("REPO_ROOT", "") or "")
        return bool(rr and (rr / pp).exists())
    except Exception:
        return False


def _dsn_sslmode(dsn: str) -> str:
    try:
        p = urlparse(dsn)
        q = parse_qs(p.query or "")
        v = (q.get("sslmode") or [""])[0]
        return (v or "").lower().strip()
    except Exception:
        low = (dsn or "").lower()
        return "require" if "sslmode=require" in low else ""


def _dsn_scheme_parts(dsn: str) -> tuple[str, str, str]:
    """
    Returns (base_scheme, full_scheme, driver_suffix)
      - postgresql://...          -> ("postgresql","postgresql","")
      - postgresql+asyncpg://...  -> ("postgresql","postgresql+asyncpg","asyncpg")
    Never throws.
    """
    try:
        scheme = (urlparse(dsn).scheme or "").lower().strip()
    except Exception:
        scheme = ""
    if "+" in scheme:
        base, rest = scheme.split("+", 1)
        return base, scheme, rest
    return scheme, scheme, ""


def _dsn_scheme_ok(dsn: str) -> bool:
    base, _, _ = _dsn_scheme_parts(dsn)
    return base in {"postgres", "postgresql"}


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


def _maybe_failfast(rep: Report) -> bool:
    return bool(rep.fatals) and _envb("CHECK_ENV_FAILFAST", False)


def _safe_section(rep: Report, section: str, fn, *, critical: bool = False) -> None:
    """
    Crash-proof wrapper: sections must never crash the process.
    If a section throws, record fatal/warn and continue (unless failfast).
    """
    try:
        fn()
    except BaseException as e:
        tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        payload = {
            "type": type(e).__name__,
            "error": _sanitize_text(e),
            "trace": _sanitize_text(tb)[-6000:],
        }
        if critical:
            rep.fatal(
                "section_exception",
                section,
                payload,
                hint="Fix the exception above; section must be stable.",
            )
        else:
            rep.warn(
                "section_exception",
                section,
                payload,
                hint="Fix the exception above; section must be stable.",
            )


# =============================================================================
# resolvers (MUST ALWAYS return (value, key))
# =============================================================================
def _resolve_trade_sol() -> tuple[float | None, str]:
    for k in ("PF_TRADE_SOL", "BUY_SOL_PER_TRADE", "TRADE_SOL", "ENTRY_MIN_INITIAL_BUY_SOL"):
        raw = _envs(k, "")
        if not raw:
            continue
        try:
            return float(raw), k
        except Exception:
            return None, k
    return None, ""


def _resolve_slippage_bps() -> tuple[int | None, str]:
    for k in ("PF_SLIPPAGE_BPS", "SLIPPAGE_BPS", "JUP_SLIPPAGE_BPS"):
        raw = _envs(k, "")
        if not raw:
            continue
        try:
            return int(float(raw)), k
        except Exception:
            return None, k
    return None, ""


def _resolve_max_concurrency() -> tuple[int, str]:
    for k in ("MAX_CONCURRENCY", "ENTRY_MOMO_MAX_PARALLEL"):
        raw = _envs(k, "")
        if raw:
            return _envi(k, 8), k
    return _envi("MAX_CONCURRENCY", 8), "MAX_CONCURRENCY"


# =============================================================================
# validators
# =============================================================================
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
        "RENDER": _bool01(bool(_envs("RENDER", ""))),
        "RENDER_SERVICE_ID_set": _bool01(bool(_envs("RENDER_SERVICE_ID", ""))),
    }

    # helpful warnings for log stability
    try:
        maj, min_ = sys.version_info[:2]
        if (maj, min_) < (3, 10):
            rep.warn(
                "python_old",
                "runtime",
                {"python": sys.version.split()[0]},
                hint="Use Python 3.10+ for best compatibility.",
            )
    except Exception:
        pass


def _validate_core_flags(rep: Report) -> None:
    sec = "flags"
    phase = _phase()

    rep.info["flags"] = {
        "MASTER_PHASE": phase,
        "STRATEGY_PHASE": _envi("STRATEGY_PHASE", phase),
        "DRYRUN": _bool01(_envb("DRYRUN", False)),
        "KILL_SWITCH": _bool01(_envb("KILL_SWITCH", False)),
        "DISABLE_BUYS": _bool01(_envb("DISABLE_BUYS", False)),
        "DISABLE_SELLS": _bool01(_envb("DISABLE_SELLS", False)),
        "DISABLE_SCANNERS": _bool01(_envb("DISABLE_SCANNERS", False)),
        "SKIP_DSN_REQUIRED": _bool01(_envb("SKIP_DSN_REQUIRED", False)),
        "ALLOW_SCHEMA_CREATE": _bool01(_envb("ALLOW_SCHEMA_CREATE", False)),
        "MAIN_FAST_FAIL": _bool01(_envb("MAIN_FAST_FAIL", False)),
        "MAIN_RESTART_ON_CRASH": _bool01(_envb("MAIN_RESTART_ON_CRASH", False)),
    }

    rep.info["scanners"] = {
        "ENABLE_RAYDIUM_SCANNER": _bool01(_envb("ENABLE_RAYDIUM_SCANNER", True)),
        "ENABLE_PUMP_SCANNER": _bool01(_envb("ENABLE_PUMP_SCANNER", True)),
        "ENABLE_DEXSCREENER_SCANNER": _bool01(_envb("ENABLE_DEXSCREENER_SCANNER", True)),
        "SCANNER_MODULES": _mask_value("SCANNER_MODULES", os.getenv("SCANNER_MODULES")),
    }

    rep.info["policy"] = {
        "phase": phase,
        "buys_possible": _bool01(_buys_effectively_possible()),
        "scanning_possible": _bool01(_scanning_effectively_possible()),
        "db_required": _bool01(_db_required()),
    }

    if _envb("KILL_SWITCH", False) and phase >= 1:
        rep.warn(
            "kill_switch_enabled",
            sec,
            "KILL_SWITCH=1 disables everything.",
            hint="Set KILL_SWITCH=0 to run.",
        )

    if _envb("DRYRUN", False) and not _envb("DISABLE_BUYS", False):
        rep.warn(
            "dryrun_buys_enabled",
            sec,
            "DRYRUN=1 but DISABLE_BUYS=0; buys should be suppressed by DRYRUN.",
            hint="This is usually OK if entry logic respects DRYRUN. If unsure, set DISABLE_BUYS=1 in DRYRUN.",
        )

    if _buys_effectively_possible() and _envb("DISABLE_SELLS", False):
        rep.warn(
            "buys_enabled_sells_disabled",
            sec,
            "Buys are possible but DISABLE_SELLS=1. This can trap positions.",
            hint="Set DISABLE_SELLS=0 for live runs (recommended).",
        )

    if _scanning_effectively_possible():
        enabled = [
            rep.info["scanners"]["ENABLE_RAYDIUM_SCANNER"],
            rep.info["scanners"]["ENABLE_PUMP_SCANNER"],
            rep.info["scanners"]["ENABLE_DEXSCREENER_SCANNER"],
        ]
        if sum(int(x) for x in enabled) == 0:
            rep.warn(
                "no_scanners_enabled",
                sec,
                "Phase>=1 but all scanners are disabled; you may never see candidates.",
                hint="Enable at least one scanner toggle.",
            )

    max_conc, src = _resolve_max_concurrency()
    rep.info["concurrency"] = {"MAX_CONCURRENCY": max_conc, "source": src}
    if max_conc <= 0:
        rep.fatal("bad_max_concurrency", sec, {"value": max_conc, "note": "Must be >=1"})
    elif max_conc > 64:
        rep.warn(
            "max_concurrency_high",
            sec,
            {"value": max_conc, "note": "May trigger RPC/Jupiter rate limits"},
        )


def _validate_rpc(rep: Report) -> None:
    sec = "rpc"
    rpc = _envs("SOL_RPC_URL", _envs("RPC_URL", ""))
    rpc_list = _envs("SOL_RPC_URLS", "")
    scanning = _scanning_effectively_possible()

    rep.info["rpc"] = {
        "SOL_RPC_URL_set": _bool01(bool(_envs("SOL_RPC_URL", ""))),
        "RPC_URL_set": _bool01(bool(_envs("RPC_URL", ""))),
        "SOL_RPC_URLS_set": _bool01(bool(rpc_list)),
        "resolved": _mask_value("SOL_RPC_URL", rpc) if rpc else "",
        "resolved_host": _host_from_urlish(rpc) if rpc else "",
    }

    if scanning and (not rpc) and (not rpc_list):
        rep.fatal(
            "missing_sol_rpc_url",
            sec,
            "Set SOL_RPC_URL (or RPC_URL) or SOL_RPC_URLS when scanners are enabled",
            hint="On Render, add SOL_RPC_URL as a secret env var (https endpoint).",
        )
        return

    def _check_one(url: str, label: str) -> None:
        if not url:
            return

        if _placeholderish(url) or _host_from_urlish(url).lower() in {
            "example.com",
            "rpc.example.com",
        }:
            rep.fatal("rpc_placeholder", sec, {"var": label, "value": _mask_value(label, url)})
            return

        if "://" not in url:
            rep.warn(
                "rpc_no_scheme",
                sec,
                {"var": label, "rpc": _sanitize_text(url)},
                hint="Prefer https://... format.",
            )
            return

        if not (url.startswith("https://") or url.startswith("http://")):
            rep.warn("rpc_bad_scheme", sec, {"var": label, "rpc": _sanitize_text(url)})

        host = _host_from_urlish(url)
        if url.startswith("http://"):
            allow_http = _envb("RPC_ALLOW_HTTP_NONLOCAL", False)
            if host not in {"127.0.0.1", "localhost"} and not allow_http:
                rep.warn(
                    "rpc_http_nonlocal",
                    sec,
                    {
                        "var": label,
                        "host": host,
                        "note": "Prefer https RPC or set RPC_ALLOW_HTTP_NONLOCAL=1",
                    },
                )

    if rpc and rpc_list:
        rep.warn(
            "rpc_both_single_and_list",
            sec,
            {
                "note": "Both SOL_RPC_URL/RPC_URL and SOL_RPC_URLS set. Ensure router selects deterministically."
            },
            hint="Prefer one method in production to reduce drift.",
        )

    if rpc:
        _check_one(rpc, "SOL_RPC_URL|RPC_URL")

    if rpc_list:
        parts = [p.strip() for p in rpc_list.replace("\n", ",").split(",") if p.strip()]
        rep.info["rpc"]["SOL_RPC_URLS_count"] = len(parts)
        if not parts:
            rep.warn("rpc_list_empty", sec, {"SOL_RPC_URLS": "(empty after parsing)"})
        if len(parts) > 12:
            rep.warn(
                "rpc_list_long",
                sec,
                {"count": len(parts), "note": "Very long RPC lists increase churn/complexity"},
            )
        for i, u in enumerate(parts[:12]):
            _check_one(u, f"SOL_RPC_URLS[{i}]")


def _validate_jupiter(rep: Report) -> None:
    sec = "jupiter"
    jup = _envs("JUP_BASE_URL", "https://api.jup.ag")
    alt = _envs("JUP_ALT_BASE_URLS", "")
    swap_api = _envs("JUPITER_SWAP_API", jup)

    qpath = _envs("JUP_QUOTE_PATH", "/swap/v1/quote")
    spath = _envs("JUP_SWAP_PATH", "/swap/v1/swap")

    def _ok_url(u: str) -> bool:
        return u.startswith("https://") or u.startswith("http://")

    if not _ok_url(jup):
        rep.fatal(
            "bad_jup_base_url",
            sec,
            {"JUP_BASE_URL": "must start with http(s)://", "value": _sanitize_text(jup)},
        )

    if _placeholderish(jup):
        rep.fatal(
            "jup_base_url_placeholder", sec, {"JUP_BASE_URL": _mask_value("JUP_BASE_URL", jup)}
        )

    allow_http = _envb("JUP_ALLOW_HTTP_NONLOCAL", False)
    for name, u in (("JUP_BASE_URL", jup), ("JUPITER_SWAP_API", swap_api)):
        if u.startswith("http://"):
            host = _host_from_urlish(u)
            if host not in {"127.0.0.1", "localhost"} and not allow_http:
                rep.warn(
                    "jup_http_nonlocal",
                    sec,
                    {
                        "var": name,
                        "host": host,
                        "note": "Prefer https or set JUP_ALLOW_HTTP_NONLOCAL=1",
                    },
                )

    if qpath and not qpath.startswith("/"):
        rep.warn("jup_quote_path_no_slash", sec, {"JUP_QUOTE_PATH": qpath})
    if spath and not spath.startswith("/"):
        rep.warn("jup_swap_path_no_slash", sec, {"JUP_SWAP_PATH": spath})

    has_key = bool(_envs("JUP_PRO_KEY", "") or _envs("JUP_API_KEY", "") or _envs("JUP_KEY", ""))
    only_direct = _envb("JUP_ONLY_DIRECT", False)

    rep.info["jupiter"] = {
        "JUP_BASE_URL": _mask_value("JUP_BASE_URL", jup),
        "JUP_ALT_BASE_URLS": _mask_value("JUP_ALT_BASE_URLS", alt),
        "JUPITER_SWAP_API": _mask_value("JUPITER_SWAP_API", swap_api),
        "JUP_QUOTE_PATH": qpath,
        "JUP_SWAP_PATH": spath,
        "has_api_key": _bool01(has_key),
        "AS_LEGACY": _bool01(_envb("JUP_AS_LEGACY", False)),
        "ALLOW_PUBLIC_FALLBACK": _bool01(_envb("JUP_ALLOW_PUBLIC_FALLBACK", False)),
        "ONLY_DIRECT": _bool01(only_direct),
        "USE_SHARED_ACCOUNTS": _bool01(_envb("JUP_USE_SHARED_ACCOUNTS", False)),
        "STRICT_AUTH": _bool01(_envb("ROUTER_STRICT_AUTH", True)),
    }

    if only_direct:
        rep.warn(
            "jup_only_direct_enabled",
            sec,
            "JUP_ONLY_DIRECT=1 can reduce route quality / fill rate on some pairs.",
            hint="If you see 'no route' often, set JUP_ONLY_DIRECT=0.",
        )

    if _buys_effectively_possible():
        host = _host_from_urlish(jup)
        if host in {"api.jup.ag", "public.jupiterapi.com"} and not has_key:
            rep.warn(
                "missing_jup_api_key",
                sec,
                "Live trading likely needs JUP_API_KEY/JUP_PRO_KEY. Without it you may hit auth/rate limits.",
                hint="Set JUP_PRO_KEY (preferred) or JUP_API_KEY as a secret env var in Render.",
            )


def _validate_trading_numbers(rep: Report) -> None:
    sec = "trade"
    trade_sol, trade_key = _resolve_trade_sol()
    slip_bps, slip_key = _resolve_slippage_bps()
    buys_possible = _buys_effectively_possible()

    if trade_key and trade_sol is None:
        rep.fatal("bad_trade_sol", sec, {trade_key: "must be a float"})
    if slip_key and slip_bps is None:
        rep.fatal("bad_slippage_bps", sec, {slip_key: "must be an int"})

    if buys_possible and trade_sol is None:
        rep.warn(
            "trade_sol_missing",
            sec,
            "No trade size found (PF_TRADE_SOL/BUY_SOL_PER_TRADE/ENTRY_MIN_INITIAL_BUY_SOL).",
            hint="Set BUY_SOL_PER_TRADE (or PF_TRADE_SOL).",
        )

    if trade_sol is not None:
        if trade_sol <= 0:
            rep.fatal("trade_sol_nonpositive", sec, {"value": trade_sol, "source": trade_key})
        elif buys_possible and trade_sol < 0.0005:
            rep.warn(
                "trade_sol_very_small",
                sec,
                {"value": trade_sol, "note": "May never fill / below practical minimum"},
            )
        elif buys_possible and trade_sol > 2.0:
            rep.warn("trade_sol_large", sec, {"value": trade_sol, "note": "Very high risk sizing"})

    if buys_possible and slip_bps is None:
        rep.warn(
            "slippage_missing",
            sec,
            "No slippage bps found (PF_SLIPPAGE_BPS/SLIPPAGE_BPS/JUP_SLIPPAGE_BPS).",
            hint="Set PF_SLIPPAGE_BPS (e.g., 50).",
        )

    if slip_bps is not None:
        if slip_bps <= 0:
            rep.fatal("slippage_bps_nonpositive", sec, {"value": slip_bps, "source": slip_key})
        elif slip_bps > 5000:
            rep.warn("slippage_bps_high", sec, {"value": slip_bps, "note": "Very high slippage"})
        elif slip_bps > 1000:
            rep.warn(
                "slippage_bps_very_high", sec, {"value": slip_bps, "note": "May cause bad fills"}
            )

    stop_loss = _envf("EXIT_STOP_LOSS_PCT", _envf("PF_SL_PCT", 0.0))
    if stop_loss:
        if stop_loss <= 0:
            rep.warn("stop_loss_nonpositive", sec, {"value": stop_loss})
        elif stop_loss > 35:
            rep.warn("stop_loss_high", sec, {"value": stop_loss, "note": "Very wide stop-loss"})

    stagn_max = _envi("EXIT_STAGNANT_MAX_MIN", 0)
    stagn_move = _envf("EXIT_STAGNANT_MIN_MOVE_PCT", 0.0)
    if stagn_max and stagn_max < 2:
        rep.warn(
            "stagnant_window_small",
            sec,
            {"EXIT_STAGNANT_MAX_MIN": stagn_max, "note": "May force premature exits"},
        )
    if stagn_move and stagn_move < 1.0:
        rep.warn("stagnant_min_move_low", sec, {"EXIT_STAGNANT_MIN_MOVE_PCT": stagn_move})

    # momentum knobs sanity (common "no buys" cause)
    momo_enabled = _envb("ENTRY_MOMENTUM_ENABLED", _envb("ENTRY_MOMO_ENABLED", False))
    momo_min_trades = _envi("ENTRY_MOMO_MIN_TRADES", 0)
    momo_min_up = _envf("ENTRY_MOMO_MIN_UP_PCT", 0.0)
    momo_window = _envi("ENTRY_MOMO_WINDOW_SEC", 0)
    if momo_enabled:
        if momo_window <= 0:
            rep.warn(
                "momo_window_bad",
                sec,
                {"ENTRY_MOMO_WINDOW_SEC": momo_window},
                hint="Set ENTRY_MOMO_WINDOW_SEC (e.g., 8).",
            )
        if momo_min_trades < 0:
            rep.warn("momo_min_trades_bad", sec, {"ENTRY_MOMO_MIN_TRADES": momo_min_trades})
        if momo_min_up < 0:
            rep.warn("momo_min_up_bad", sec, {"ENTRY_MOMO_MIN_UP_PCT": momo_min_up})

    # scanner threshold sanity (optional; only warns if present)
    for k, lo, hi, hint in (
        ("SCANNER_MIN_BUYS_5M", 0, 200, "If you see no candidates, lower this (e.g., 1–3)."),
        ("PF_SCAN_MIN_5M_BUYS", 0, 200, "If you see no candidates, lower this (e.g., 1–3)."),
        ("PF_SCAN_MIN_5M_VOL_USD", 0, 10_000_000, "Too high filters can cause no candidates."),
        ("PF_SCAN_MIN_LIQ_USD", 0, 100_000_000, "Too high filters can cause no candidates."),
    ):
        raw = _envs(k, "")
        if not raw:
            continue
        try:
            v = float(raw)
            if v < lo:
                rep.warn("threshold_low", sec, {k: v, "lo": lo})
            elif v > hi:
                rep.warn("threshold_high", sec, {k: v, "hi": hi}, hint=hint)
        except Exception:
            rep.warn(
                "threshold_bad", sec, {k: "must be numeric"}, hint=f"Fix {k} to a numeric value."
            )

    rep.info["trade"] = {
        "buys_possible": _bool01(buys_possible),
        "trade_sol": trade_sol,
        "trade_sol_source": trade_key or None,
        "slippage_bps": slip_bps,
        "slippage_bps_source": slip_key or None,
        "EXIT_STOP_LOSS_PCT": stop_loss,
        "EXIT_STAGNANT_MAX_MIN": stagn_max,
        "EXIT_STAGNANT_MIN_MOVE_PCT": stagn_move,
        "ENTRY_MOMENTUM_ENABLED": _bool01(momo_enabled),
        "ENTRY_MOMO_MIN_TRADES": momo_min_trades,
        "ENTRY_MOMO_MIN_UP_PCT": momo_min_up,
        "ENTRY_MOMO_WINDOW_SEC": momo_window,
    }


def _validate_wallet(rep: Report) -> None:
    sec = "wallet"
    wallet_json = _envs("WALLET_JSON", "")
    wallet_json_b64 = _envs("WALLET_JSON_B64", "")
    wallet_pk = _envs("WALLET_PRIVATE_KEY", "")
    keypair_path = _envs("KEYPAIR_PATH", "")
    keypair_b64 = _envs("KEYPAIR_B64", "")

    allow_missing_files = _envb("CHECK_ENV_ALLOW_MISSING_KEYFILES", False)

    wallet_ok = False
    mode = "none"
    notes: list[str] = []

    sources_set = [
        k
        for k in (
            "WALLET_JSON",
            "WALLET_JSON_B64",
            "WALLET_PRIVATE_KEY",
            "KEYPAIR_PATH",
            "KEYPAIR_B64",
        )
        if _envs(k, "")
    ]
    if len(sources_set) >= 2:
        rep.warn(
            "multiple_wallet_sources",
            sec,
            {"sources": sources_set},
            hint="Prefer exactly one wallet source in production to avoid ambiguity (KEYPAIR_PATH or WALLET_JSON_B64).",
        )

    if wallet_json:
        mode = "WALLET_JSON"
        try:
            json.loads(wallet_json)
            wallet_ok = True
        except Exception:
            if _looks_like_path(wallet_json):
                if _file_exists_anywhere(wallet_json):
                    wallet_ok = True
                    mode = "WALLET_JSON_PATH"
                else:
                    rep.warn(
                        "wallet_json_path_missing",
                        sec,
                        {"WALLET_JSON": _mask_value("WALLET_JSON", wallet_json)},
                    )
            else:
                rep.warn(
                    "wallet_json_invalid",
                    sec,
                    "WALLET_JSON set but not valid JSON",
                    hint="Prefer WALLET_JSON_B64 or KEYPAIR_PATH for Render secrets.",
                )

    elif wallet_json_b64:
        mode = "WALLET_JSON_B64"
        ok, note = _decode_b64_preview(wallet_json_b64)
        if ok:
            wallet_ok = True
            notes.append(note)
        else:
            rep.warn("wallet_json_b64_invalid", sec, {"note": note})

    elif keypair_b64:
        mode = "KEYPAIR_B64"
        ok, note = _decode_b64_preview(keypair_b64)
        if ok:
            wallet_ok = True
            notes.append(note)
        else:
            rep.warn("keypair_b64_invalid", sec, {"note": note})

    elif keypair_path:
        mode = "KEYPAIR_PATH"
        if _file_exists_anywhere(keypair_path):
            wallet_ok = True
        else:
            if allow_missing_files:
                wallet_ok = True
                mode = "KEYPAIR_PATH_MISSING_ALLOWED"
                notes.append("KEYPAIR_PATH missing but allowed by policy")
            else:
                rep.warn(
                    "keypair_path_missing",
                    sec,
                    {"KEYPAIR_PATH": _mask_value("KEYPAIR_PATH", keypair_path)},
                )
                mode = "KEYPAIR_PATH_MISSING"

    elif wallet_pk:
        if _looks_like_json_blob(wallet_pk):
            mode = "WALLET_PRIVATE_KEY_JSON"
            try:
                json.loads(wallet_pk)
                wallet_ok = True
            except Exception:
                rep.warn(
                    "wallet_private_key_json_invalid",
                    sec,
                    "WALLET_PRIVATE_KEY looks like JSON but failed to parse",
                )
        elif _looks_like_path(wallet_pk):
            mode = "WALLET_PRIVATE_KEY_PATH"
            if _file_exists_anywhere(wallet_pk):
                wallet_ok = True
            else:
                if allow_missing_files:
                    wallet_ok = True
                    mode = "WALLET_PRIVATE_KEY_PATH_MISSING_ALLOWED"
                    notes.append("WALLET_PRIVATE_KEY path missing but allowed by policy")
                else:
                    rep.warn(
                        "wallet_path_missing",
                        sec,
                        {
                            "var": "WALLET_PRIVATE_KEY",
                            "value": _mask_value("WALLET_PRIVATE_KEY", wallet_pk),
                        },
                    )
                    mode = "WALLET_PRIVATE_KEY_PATH_MISSING"
        elif _looks_like_base58_secret(wallet_pk):
            wallet_ok = True
            mode = "WALLET_PRIVATE_KEY_BASE58"
            rep.warn(
                "wallet_private_key_unvalidated",
                sec,
                "WALLET_PRIVATE_KEY looks like base58 secret; accepted as present but not validated here.",
                hint="Preferred: KEYPAIR_PATH or WALLET_JSON_B64 for deterministic parsing.",
            )
        else:
            rep.warn(
                "wallet_private_key_unknown_format",
                sec,
                {
                    "note": "Expected JSON array/object, keypair path, or base58 secret",
                    "value": "***",
                },
            )

    rep.info["wallet"] = {
        "mode": mode,
        "notes": notes[:10],
        "sources_set": sources_set,
        "WALLET_JSON_set": _bool01(bool(wallet_json)),
        "WALLET_JSON_B64_set": _bool01(bool(wallet_json_b64)),
        "WALLET_PRIVATE_KEY_set": _bool01(bool(wallet_pk)),
        "KEYPAIR_PATH_set": _bool01(bool(keypair_path)),
        "KEYPAIR_B64_set": _bool01(bool(keypair_b64)),
        "ALLOW_MISSING_KEYFILES": _bool01(allow_missing_files),
    }

    if _buys_effectively_possible() and not wallet_ok:
        rep.fatal(
            "wallet_missing_for_live_trading",
            sec,
            "No usable wallet configured (WALLET_JSON / WALLET_JSON_B64 / WALLET_PRIVATE_KEY / KEYPAIR_PATH / KEYPAIR_B64)",
            hint="Provide KEYPAIR_PATH or WALLET_JSON_B64 as Render secret; start.sh can bootstrap wallet.json.",
        )


def _validate_db(rep: Report) -> None:
    sec = "db"
    required = _db_required()

    dsn_candidates = [
        "DATABASE_URL",
        "POSTGRES_DSN",
        "PG_DSN",
        "PNL_DSN",
        "PG_DSN_INTERNAL",
        "PG_DSN_EXTERNAL",
    ]
    present = {k: bool(_envs(k, "")) for k in dsn_candidates}
    has_any = any(present.values())

    schemes: dict[str, Any] = {}
    hosts: dict[str, str] = {}
    for k in dsn_candidates:
        v = _envs(k, "")
        if not v:
            continue
        base, full, driver = _dsn_scheme_parts(v)
        schemes[k] = {"base": base, "scheme": full, "driver": driver or None}
        hosts[k] = _host_from_urlish(v)

    rep.info["db"] = {
        "required": _bool01(required),
        "has_any_dsn": _bool01(has_any),
        "present": present,
        "schemes": schemes,
        "hosts": hosts,
        "DATABASE_URL": _mask_value("DATABASE_URL", os.getenv("DATABASE_URL")),
        "POSTGRES_DSN": _mask_value("POSTGRES_DSN", os.getenv("POSTGRES_DSN")),
        "PG_DSN": _mask_value("PG_DSN", os.getenv("PG_DSN")),
        "PNL_DSN": _mask_value("PNL_DSN", os.getenv("PNL_DSN")),
        "PG_DSN_INTERNAL": _mask_value("PG_DSN_INTERNAL", os.getenv("PG_DSN_INTERNAL")),
        "PG_DSN_EXTERNAL": _mask_value("PG_DSN_EXTERNAL", os.getenv("PG_DSN_EXTERNAL")),
    }

    if required and not has_any:
        rep.fatal(
            "missing_db_dsn",
            sec,
            "Set one of DATABASE_URL / POSTGRES_DSN / PG_DSN / PNL_DSN (or set SKIP_DSN_REQUIRED=1)",
            hint="On Render: set DATABASE_URL or POSTGRES_DSN. Locally: set SKIP_DSN_REQUIRED=1 for scanner-only runs.",
        )
        return

    set_dsns = [k for k in dsn_candidates if _envs(k, "")]
    if len(set_dsns) >= 2:
        rep.warn(
            "multiple_db_dsns_set",
            sec,
            {"dsns": set_dsns},
            hint="Prefer a single canonical DSN in production (DATABASE_URL or POSTGRES_DSN). start.sh should normalize.",
        )

    for k in dsn_candidates:
        v = _envs(k, "")
        if not v:
            continue

        base, full, _driver = _dsn_scheme_parts(v)
        if base in {"sqlite"} or v.lower().startswith("sqlite"):
            rep.warn(
                "sqlite_detected",
                sec,
                {"var": k, "scheme": full},
                hint="SQLite should be legacy-only. Prefer Postgres for the AI/ledger brain.",
            )

        if not _dsn_scheme_ok(v):
            rep.warn(
                "dsn_scheme_suspicious",
                sec,
                {
                    "var": k,
                    "scheme": full or "(missing)",
                    "base": base or "(missing)",
                    "note": "Expected postgres:// or postgresql:// (or postgresql+driver://)",
                },
                hint="Set this DSN to a Postgres URL (Render provides postgresql://...).",
            )

        host = _host_from_urlish(v)
        if host.endswith(".render.com") and _dsn_sslmode(v) != "require":
            rep.warn(
                "render_dsn_missing_sslmode",
                sec,
                {"var": k, "host": host, "expected": "sslmode=require"},
                hint="Your start.sh may normalize DSNs. If this persists, add ?sslmode=require to the DSN.",
            )


def _validate_mints(rep: Report) -> None:
    sec = "mints"
    sol = _normalize_mint(_envs("SOL_MINT", KNOWN_SOL_MINT) or KNOWN_SOL_MINT)
    usdc_raw = _envs("USDC_MINT", KNOWN_USDC_MINT) or KNOWN_USDC_MINT
    usdc = _normalize_mint(usdc_raw)

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


def _validate_integrations(rep: Report) -> None:
    sec = "integrations"

    notion_enabled = _envb("NOTION_LOG_ENABLED", False) or _envb("NOTION_ENABLED", False)
    if notion_enabled and (not _envs("NOTION_TOKEN", "") or not _envs("NOTION_DATABASE_ID", "")):
        rep.warn(
            "notion_missing_creds",
            sec,
            "Notion logging enabled but NOTION_TOKEN/NOTION_DATABASE_ID not set",
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


def _dns_checks(rep: Report) -> None:
    sec = "dns"
    if not _envb("CHECK_ENV_DNS", False):
        rep.info["dns"] = {"enabled": 0}
        return

    hosts: set[str] = set()

    jup = _envs("JUP_BASE_URL", "https://api.jup.ag")
    hosts.add(_host_from_urlish(jup) or "api.jup.ag")
    hosts.add("public.jupiterapi.com")

    rpc = _envs("SOL_RPC_URL", _envs("RPC_URL", ""))
    if rpc:
        hosts.add(_host_from_urlish(rpc))

    redis_url = _envs("REDIS_URL", _envs("REDIS_TLS_URL", ""))
    if redis_url:
        hosts.add(_host_from_urlish(redis_url))

    for k in ("DATABASE_URL", "POSTGRES_DSN", "PG_DSN", "PNL_DSN"):
        v = _envs(k, "")
        if v:
            hosts.add(_host_from_urlish(v))

    out: dict[str, Any] = {"enabled": 1}
    for h in sorted({x for x in hosts if x}):
        try:
            ip = socket.getaddrinfo(h, 443)[0][4][0]
            out[h] = ip
        except Exception as e:
            out[h] = f"DNS_ERROR: {type(e).__name__}: {_sanitize_text(e)}"
            rep.warn(
                "dns_resolution_failed",
                sec,
                {"host": h},
                hint="Check networking/DNS; on Render verify outbound access.",
            )
    rep.info["dns"] = out


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
            "ENABLE_RAYDIUM_SCANNER",
            "ENABLE_PUMP_SCANNER",
            "ENABLE_DEXSCREENER_SCANNER",
            "SCANNER_MODULES",
            "SOL_RPC_URL",
            "RPC_URL",
            "SOL_RPC_URLS",
            "JUP_BASE_URL",
            "JUP_ALT_BASE_URLS",
            "JUP_QUOTE_PATH",
            "JUP_SWAP_PATH",
            "JUP_AS_LEGACY",
            "JUP_ALLOW_PUBLIC_FALLBACK",
            "JUP_ONLY_DIRECT",
            "JUP_USE_SHARED_ACCOUNTS",
            "ROUTER_STRICT_AUTH",
            "PF_TRADE_SOL",
            "BUY_SOL_PER_TRADE",
            "ENTRY_MIN_INITIAL_BUY_SOL",
            "PF_SLIPPAGE_BPS",
            "SLIPPAGE_BPS",
            "JUP_SLIPPAGE_BPS",
            "EXIT_STOP_LOSS_PCT",
            "PF_SL_PCT",
            "EXIT_STAGNANT_MAX_MIN",
            "EXIT_STAGNANT_MIN_MOVE_PCT",
            "SCANNER_MIN_BUYS_5M",
            "PF_SCAN_MIN_5M_BUYS",
            "SKIP_DSN_REQUIRED",
            "DATABASE_URL",
            "POSTGRES_DSN",
            "PG_DSN",
            "PNL_DSN",
            "NOTION_LOG_ENABLED",
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
        }
    )
    snap: dict[str, Any] = {}
    for k in keys:
        snap[k] = _mask_value(k, os.getenv(k))
    rep.info["env_snapshot"] = snap


def _apply_strict(rep: Report) -> None:
    if not _envb("CHECK_ENV_STRICT", False):
        return
    promote = {
        "render_dsn_missing_sslmode",
        "jup_http_nonlocal",
        "missing_jup_api_key",
        "dns_resolution_failed",
        "rpc_http_nonlocal",
        "rpc_no_scheme",
        "dsn_scheme_suspicious",
        "buys_enabled_sells_disabled",
        "multiple_wallet_sources",
        "multiple_db_dsns_set",
        "rpc_both_single_and_list",
        "rpc_list_empty",
    }
    promote |= set(_parse_csv(_envs("CHECK_ENV_STRICT_PROMOTE", "")))

    for w in list(rep.warns):
        if w.code in promote:
            rep.fatal(f"strict_{w.code}", w.section, w.detail, hint=w.hint)


def _build_diff_summary(rep: Report) -> None:
    missing_required: list[str] = []
    malformed: list[str] = []

    if _scanning_effectively_possible() and (
        not _envs("SOL_RPC_URL", "") and not _envs("RPC_URL", "") and not _envs("SOL_RPC_URLS", "")
    ):
        missing_required.append("SOL_RPC_URL|RPC_URL|SOL_RPC_URLS")

    if _buys_effectively_possible() and not any(
        _envs(k, "")
        for k in (
            "WALLET_JSON",
            "WALLET_JSON_B64",
            "WALLET_PRIVATE_KEY",
            "KEYPAIR_PATH",
            "KEYPAIR_B64",
        )
    ):
        missing_required.append(
            "WALLET_JSON|WALLET_JSON_B64|WALLET_PRIVATE_KEY|KEYPAIR_PATH|KEYPAIR_B64"
        )

    if _db_required() and not any(
        _envs(k, "")
        for k in (
            "DATABASE_URL",
            "POSTGRES_DSN",
            "PG_DSN",
            "PNL_DSN",
            "PG_DSN_INTERNAL",
            "PG_DSN_EXTERNAL",
        )
    ):
        missing_required.append(
            "DATABASE_URL|POSTGRES_DSN|PG_DSN|PNL_DSN|PG_DSN_INTERNAL|PG_DSN_EXTERNAL"
        )

    trade_sol, trade_key = _resolve_trade_sol()
    if trade_key and trade_sol is None:
        malformed.append(trade_key)

    slip_bps, slip_key = _resolve_slippage_bps()
    if slip_key and slip_bps is None:
        malformed.append(slip_key)

    rep.info["diff"] = {
        "missing_required": sorted(set(missing_required)),
        "malformed": sorted(set(malformed)),
    }


# =============================================================================
# runner
# =============================================================================
def run_checks() -> Report:
    rep = Report(ok=True)

    _safe_section(rep, "runtime", lambda: _validate_runtime(rep), critical=True)
    _safe_section(rep, "flags", lambda: _validate_core_flags(rep), critical=True)
    if _maybe_failfast(rep):
        _build_diff_summary(rep)
        return rep

    _safe_section(rep, "jupiter", lambda: _validate_jupiter(rep), critical=True)
    if _maybe_failfast(rep):
        _build_diff_summary(rep)
        return rep

    _safe_section(rep, "rpc", lambda: _validate_rpc(rep), critical=True)
    if _maybe_failfast(rep):
        _build_diff_summary(rep)
        return rep

    _safe_section(rep, "trade", lambda: _validate_trading_numbers(rep), critical=True)
    if _maybe_failfast(rep):
        _build_diff_summary(rep)
        return rep

    _safe_section(rep, "wallet", lambda: _validate_wallet(rep), critical=True)
    if _maybe_failfast(rep):
        _build_diff_summary(rep)
        return rep

    _safe_section(rep, "db", lambda: _validate_db(rep), critical=True)
    if _maybe_failfast(rep):
        _build_diff_summary(rep)
        return rep

    _safe_section(rep, "mints", lambda: _validate_mints(rep), critical=False)
    _safe_section(rep, "integrations", lambda: _validate_integrations(rep), critical=False)
    _safe_section(rep, "dns", lambda: _dns_checks(rep), critical=False)
    _safe_section(rep, "env_snapshot", lambda: _env_snapshot(rep), critical=False)
    _safe_section(rep, "strict", lambda: _apply_strict(rep), critical=False)

    _build_diff_summary(rep)

    rep.info["summary"] = {
        "fatals": len(rep.fatals),
        "warnings": len(rep.warns),
        "phase": _phase(),
        "buys_possible": _bool01(_buys_effectively_possible()),
        "scanning_possible": _bool01(_scanning_effectively_possible()),
        "db_required": _bool01(_db_required()),
    }
    return rep


def _collect_fix_hints(rep: Report) -> list[str]:
    hints: list[str] = []
    # prioritize fatal hints then warning hints
    for it in rep.fatals + rep.warns:
        if it.hint and it.hint.strip():
            hints.append(_sanitize_text(it.hint.strip()))

    diff = rep.info.get("diff") or {}
    miss = diff.get("missing_required") or []
    mal = diff.get("malformed") or []
    if miss:
        hints.append(f"Missing required groups: {miss}")
    if mal:
        hints.append(f"Malformed values: {mal}")

    # de-dup, cap
    out: list[str] = []
    seen: set[str] = set()
    for h in hints:
        if h and h not in seen:
            out.append(h)
            seen.add(h)
        if len(out) >= 18:
            break
    return out


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


def _as_json(rep: Report) -> dict[str, Any]:
    fix_hints = _collect_fix_hints(rep)
    return {
        "ts": _ts(),
        "ok": bool(rep.ok),
        "exit_code": RC_OK if rep.ok else RC_FATAL,
        "fatals": _issues_to_dict(rep.fatals),
        "warnings": _issues_to_dict(rep.warns),
        "warnings_text": _issues_to_text(rep.warns, cap=60),
        "fatals_text": _issues_to_text(rep.fatals, cap=60),
        "fix_hints": fix_hints,
        "info": rep.info,
        "contract": {
            "script": SCRIPT_ID,
            "version": VERSION,
            "no_network_default": True,
            "exit_codes": {"ok": RC_OK, "fatal": RC_FATAL},
        },
    }


class _ArgParser(argparse.ArgumentParser):
    """
    God-tier CLI parsing: never exit(2) with no JSON.
    Turn parse errors into a normal failure path (exit 1) with JSON available.
    """

    def error(self, message: str) -> None:  # noqa: D401
        raise ValueError(message)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    ap = _ArgParser(description="God-Tier env validator (Phase 0–7 LOCK-IN).")
    ap.add_argument(
        "--json", action="store_true", help="Emit JSON to stdout (default true via env)."
    )
    ap.add_argument("--json-only", action="store_true", help="Only JSON, no human stderr summary.")
    ap.add_argument("--dns", action="store_true", help="Run DNS checks (warn-only unless strict).")
    ap.add_argument("--strict", action="store_true", help="Promote selected warnings to fatal.")
    ap.add_argument("--show-env", action="store_true", help="Include masked env snapshot in JSON.")
    ap.add_argument("--failfast", action="store_true", help="Stop after first fatal section.")
    return ap.parse_args(argv)


def _report_unhandled_exception(e: BaseException) -> Report:
    rep = Report(ok=False)
    try:
        _validate_runtime(rep)
        _validate_core_flags(rep)
    except Exception:
        rep.info.setdefault("runtime", {})
        rep.info.setdefault("flags", {})

    tb = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    tb = _sanitize_text(tb)

    rep.fatal(
        "unhandled_exception",
        "runtime",
        {"type": type(e).__name__, "error": _sanitize_text(e), "trace": tb[-6000:]},
        hint="Fix the exception above. check_env must never crash and must always return JSON + exit_code.",
    )

    rep.info["summary"] = {
        "fatals": len(rep.fatals),
        "warnings": len(rep.warns),
        "phase": _phase(),
        "buys_possible": _bool01(_buys_effectively_possible()),
        "scanning_possible": _bool01(_scanning_effectively_possible()),
        "db_required": _bool01(_db_required()),
    }
    _build_diff_summary(rep)
    return rep


def _report_cli_error(msg: str, argv: list[str]) -> Report:
    rep = Report(ok=False)
    _safe_section(rep, "runtime", lambda: _validate_runtime(rep), critical=False)
    _safe_section(rep, "flags", lambda: _validate_core_flags(rep), critical=False)

    rep.fatal(
        "cli_parse_error",
        "cli",
        {"message": _sanitize_text(msg), "argv": [_sanitize_text(a) for a in argv[:200]]},
        hint="Fix the CLI flags (preflight expects scripts/check_env.py --json-only to work).",
    )
    _build_diff_summary(rep)
    rep.info.setdefault("summary", {})
    rep.info["summary"].update(
        {
            "fatals": len(rep.fatals),
            "warnings": len(rep.warns),
            "phase": _phase(),
            "buys_possible": _bool01(_buys_effectively_possible()),
            "scanning_possible": _bool01(_scanning_effectively_possible()),
            "db_required": _bool01(_db_required()),
        }
    )
    return rep


def main() -> None:
    # Keep output robust on Windows shells
    for stream in (sys.stdout, sys.stderr):
        try:
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding=stream.encoding or "utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass

    argv = list(sys.argv[1:])

    # Pre-scan CLI flags so JSON is still emitted even if parsing fails.
    # (This prevents "rc=2 with no output" class failures.)
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

    try:
        args = _parse_args(argv)
    except SystemExit:
        raise
    except Exception as e:
        # Convert CLI parse problems into normal report (exit 1) with JSON if requested.
        rep = _report_cli_error(str(e), argv)
        emit_json = _envb("CHECK_ENV_JSON", True)
        json_only = _envb("CHECK_ENV_JSON_ONLY", False)

        if emit_json:
            payload = _as_json(rep)
            try:
                sys.stdout.write(
                    json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n"
                )
                sys.stdout.flush()
            except Exception:
                _safe_write_line(sys.stdout, json.dumps(payload, ensure_ascii=False, default=str))

        if not json_only:
            _safe_write_line(sys.stderr, "")
            _safe_write_line(sys.stderr, "check_env: FAILED (cli)")
            _safe_write_line(sys.stderr, f"  error: {_sanitize_text(e)}")

        raise SystemExit(RC_FATAL) from None

    # CLI overrides -> env toggles (deterministic)
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

    try:
        rep = run_checks()
    except SystemExit:
        raise
    except BaseException as e:
        rep = _report_unhandled_exception(e)

    emit_json = _envb("CHECK_ENV_JSON", True)
    json_only = _envb("CHECK_ENV_JSON_ONLY", False)

    if emit_json:
        payload = _as_json(rep)
        try:
            sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")
            sys.stdout.flush()
        except Exception:
            _safe_write_line(sys.stdout, json.dumps(payload, ensure_ascii=False, default=str))

    if not json_only:
        _safe_write_line(sys.stderr, "")
        _safe_write_line(sys.stderr, f"check_env: {'OK' if rep.ok else 'FAILED'}")
        _safe_write_line(sys.stderr, f"  repo_root: {_sanitize_text(_REPO_ROOT)}")
        _safe_write_line(
            sys.stderr,
            f"  phase: {_phase()} | buys_possible={_bool01(_buys_effectively_possible())} | "
            f"scanning_possible={_bool01(_scanning_effectively_possible())} | db_required={_bool01(_db_required())}",
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

        diff = rep.info.get("diff") or {}
        _safe_write_line(sys.stderr, "")
        _safe_write_line(sys.stderr, f"diff.missing_required: {diff.get('missing_required', [])}")
        _safe_write_line(sys.stderr, f"diff.malformed:        {diff.get('malformed', [])}")

    raise SystemExit(RC_OK if rep.ok else RC_FATAL)


if __name__ == "__main__":
    main()
