#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/test_notion.py

God-Tier Notion Canary (SAFE by default; P0 quarantine tool)
==========================================================

Mission
-------
Prove your Notion integration + database contract are correct for logging,
WITHOUT leaking secrets and WITHOUT doing writes by default.

Modes
-----
1) Read-only (DEFAULT): validates
   - token works (GET /v1/users/me)
   - database accessible (GET /v1/databases/{id})
   - required properties exist + types are compatible (configurable strictness)

2) Write test (opt-in): creates ONE dummy page in the configured database
   - optional --cleanup archives that page afterward

Why read-only by default?
-------------------------
Health/preflight must never write. This script is dev-only/quarantine, but we still
default safe. If you explicitly want a write, pass --write.

CLI
---
python -m scripts.test_notion
python -m scripts.test_notion --write --cleanup
python scripts/test_notion.py --soft
python scripts/test_notion.py --json-only

Env (compatible with your bot)
------------------------------
Token:
  NOTION_API_KEY   (preferred)
  NOTION_TOKEN     (fallback)

Database:
  NOTION_DATABASE_ID

Property name mapping (env-driven; defaults match your existing script):
  NOTION_TITLE_PROP="Token"
  NOTION_PROP_TOKEN="Mint"
  NOTION_PROP_BALANCE="Balance"
  NOTION_PROP_ENTRY="Entry Price"
  NOTION_PROP_CURRENT="Current Price"
  NOTION_PROP_PROFIT="Profit"
  NOTION_PROP_REASON="Reason"
  NOTION_PROP_TIME="Timestamp"

Behavior knobs:
  NOTION_TEST_STRICT_PROPS=1|0        (default 1)
  NOTION_TEST_TIMEOUT_S=12
  NOTION_TEST_RETRIES=2
  NOTION_TEST_RETRY_BASE_S=0.35
  NOTION_TEST_NO_DOTENV=1             (disable dotenv auto-load)
  NOTION_TEST_MASK_LONG_TOKENS=1      (default 1)

Output:
  NOTION_TEST_JSON=1|0                (default 1)
  NOTION_TEST_JSON_ONLY=1|0
  NOTION_TEST_VERBOSE=1|0

Exit codes
----------
0 OK
1 Not OK (token/db/props failed)
2 Misconfigured (missing env / bad args)
3 Network error / Notion unreachable
4 Write test failed (only when --write)
"""

import argparse
import contextlib
import json
import os
import random
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# =============================================================================
# constants / exit codes
# =============================================================================
RC_OK = 0
RC_FAIL = 1
RC_MISCONFIG = 2
RC_NET = 3
RC_WRITE_FAIL = 4

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28").strip() or "2022-06-28"

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}


# =============================================================================
# repo-root bootstrap (for `python scripts/test_notion.py`)
# =============================================================================
def _bootstrap_repo_root() -> None:
    try:
        here = Path(__file__).resolve()
    except Exception:
        return
    markers = ("pyproject.toml", "requirements.txt", "setup.cfg", ".git")
    for p in [here.parent, *list(here.parents)]:
        with contextlib.suppress(Exception):
            if any((p / m).exists() for m in markers) or (p / "src").is_dir():
                root = str(p)
                if root and root not in sys.path:
                    sys.path.insert(0, root)
                os.environ.setdefault("REPO_ROOT", root)
                return


# =============================================================================
# env helpers
# =============================================================================
def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    s = _envs(k, "")
    if not s:
        return bool(d)
    sl = s.lower().strip()
    if sl in _TRUE:
        return True
    if sl in _FALSE:
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


def _maybe_load_dotenv() -> None:
    if _envb("NOTION_TEST_NO_DOTENV", False):
        return
    with contextlib.suppress(Exception):
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(override=False)


# =============================================================================
# safe output (never crash on cp1252 consoles)
# =============================================================================
def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


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
                file.buffer.write(b)  # type: ignore[attr-defined]
                file.buffer.flush()  # type: ignore[attr-defined]
                return
        with contextlib.suppress(Exception):
            file.write(b.decode(enc, errors="replace"))
            file.flush()
    except Exception:
        pass


def _hlog(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    _safe_write(sys.stderr, f"[test_notion] {_ts()} {msg}\n")


# =============================================================================
# redaction / safety
# =============================================================================
_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")
_LONG_TOKENISH_RE = re.compile(r"\b[A-Za-z0-9_\-]{28,}\b")
_KV_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|token|secret|password|pass)\b(\s*[:=]\s*)([^\s,;]+)"
)


def _sanitize(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _URL_CRED_RE.sub(r"\1***@", s)
    s = _KV_SECRET_RE.sub(r"\1\2***", s)

    # Replace actual env secrets if present
    for k in ("NOTION_API_KEY", "NOTION_TOKEN"):
        v = os.getenv(k, "")
        if v and len(v) >= 6:
            s = s.replace(v, "***")

    if _envb("NOTION_TEST_MASK_LONG_TOKENS", True):
        s = _LONG_TOKENISH_RE.sub("***", s)
    return s


def _mask_token(tok: str) -> str:
    tok = (tok or "").strip()
    if not tok:
        return ""
    if len(tok) <= 8:
        return "***"
    return tok[:3] + "…" + tok[-4:]


def _safe_jsonable(x: Any) -> Any:
    try:
        json.dumps(x, default=str)
        return x
    except Exception:
        return str(x)


# =============================================================================
# retry policy
# =============================================================================
def _retry_cfg() -> tuple[int, float]:
    retries = max(0, _envi("NOTION_TEST_RETRIES", 2))
    base = max(0.05, float(_envf("NOTION_TEST_RETRY_BASE_S", 0.35)))
    return retries, base


async def _sleep_backoff(attempt: int, base: float) -> None:
    import asyncio

    cap = 2.5
    t = min(cap, base * (1.6 ** max(0, attempt)))
    t *= 0.85 + random.random() * 0.35
    await asyncio.sleep(t)


# =============================================================================
# http client (requests preferred; urllib fallback)
# =============================================================================
def _http_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _http_request(
    method: str, url: str, *, token: str, json_body: dict[str, Any] | None, timeout_s: float
) -> tuple[int, Any]:
    url = str(url).strip()
    if not url:
        raise RuntimeError("empty_url")

    # Prefer requests
    with contextlib.suppress(Exception):
        import requests  # type: ignore

        r = requests.request(
            method.upper(), url, headers=_http_headers(token), json=json_body, timeout=timeout_s
        )
        ctype = (r.headers.get("content-type") or "").lower()
        if "application/json" in ctype:
            with contextlib.suppress(Exception):
                return int(r.status_code), r.json()
        return int(r.status_code), r.text

    # urllib fallback
    import urllib.request

    data = None
    if json_body is not None:
        data = json.dumps(json_body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method.upper())
    for k, v in _http_headers(token).items():
        req.add_header(k, v)

    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        code = int(getattr(resp, "status", 200))
        body = resp.read().decode("utf-8", "ignore")
        with contextlib.suppress(Exception):
            return code, json.loads(body)
        return code, body


# =============================================================================
# notion property contract helpers
# =============================================================================
def _prop_names_from_env() -> dict[str, str]:
    # These are the names in Notion (columns), not env var names.
    return {
        "title": _envs("NOTION_TITLE_PROP", "Token"),
        "mint": _envs("NOTION_PROP_TOKEN", "Mint"),
        "balance": _envs("NOTION_PROP_BALANCE", "Balance"),
        "entry": _envs("NOTION_PROP_ENTRY", "Entry Price"),
        "current": _envs("NOTION_PROP_CURRENT", "Current Price"),
        "profit": _envs("NOTION_PROP_PROFIT", "Profit"),
        "reason": _envs("NOTION_PROP_REASON", "Reason"),
        "time": _envs("NOTION_PROP_TIME", "Timestamp"),
    }


def _extract_db_properties(db_payload: Any) -> dict[str, Any]:
    if isinstance(db_payload, dict):
        props = db_payload.get("properties")
        if isinstance(props, dict):
            return props
    return {}


def _type_of_prop(p: Any) -> str:
    if isinstance(p, dict):
        t = p.get("type")
        if isinstance(t, str):
            return t
    return ""


def _title_value(s: str) -> dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": str(s)[:2000]}}]}


def _rich_text_value(s: str) -> dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": str(s)[:2000]}}]}


def _number_value(x: float) -> dict[str, Any]:
    return {"number": float(x)}


def _date_value(iso: str) -> dict[str, Any]:
    return {"date": {"start": str(iso)}}


def _make_prop_value(prop_type: str, sample: dict[str, Any]) -> dict[str, Any] | None:
    """
    Build a Notion property value matching the database property type.
    Supported: title, rich_text, number, date.
    Others: return None (we'll skip + report).
    """
    if prop_type == "title":
        return _title_value(sample["text"])
    if prop_type == "rich_text":
        return _rich_text_value(sample["text"])
    if prop_type == "number":
        return _number_value(sample["num"])
    if prop_type == "date":
        return _date_value(sample["iso"])
    return None


# =============================================================================
# models
# =============================================================================
@dataclass
class Step:
    key: str
    ok: bool
    status: int = 0
    error: str = ""
    details: dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0


# =============================================================================
# core checks
# =============================================================================
def _token_from_env() -> str:
    return _envs("NOTION_API_KEY", "") or _envs("NOTION_TOKEN", "")


def _database_id_from_env() -> str:
    return _envs("NOTION_DATABASE_ID", "")


async def _step_with_retries(
    key: str,
    fn,
    *,
    timeout_s: float,
    json_only: bool,
    verbose: bool,
) -> Step:
    import asyncio

    retries, base = _retry_cfg()
    last: Step | None = None

    for attempt in range(retries + 1):
        t0 = time.time()
        try:
            st: Step = await asyncio.to_thread(fn, timeout_s)  # run sync http safely
            st.duration_ms = int((time.time() - t0) * 1000)
            if verbose:
                _hlog(
                    f"{key}: ok={int(st.ok)} status={st.status} ({st.duration_ms}ms)",
                    json_only=json_only,
                )
            return st
        except Exception as e:
            dur = int((time.time() - t0) * 1000)
            last = Step(
                key=key,
                ok=False,
                status=0,
                error=_sanitize(f"{type(e).__name__}: {e}"),
                details={},
                duration_ms=dur,
            )
            if verbose:
                _hlog(f"{key}: error={last.error} ({dur}ms)", json_only=json_only)

            if attempt < retries:
                await _sleep_backoff(attempt, base)
                continue
            return last

    return last or Step(key=key, ok=False, error="unknown_error")


def _check_me(token: str):
    def _run(timeout_s: float) -> Step:
        code, data = _http_request(
            "GET", f"{NOTION_API}/users/me", token=token, json_body=None, timeout_s=timeout_s
        )
        ok = int(code) == 200
        det: dict[str, Any] = {}
        if isinstance(data, dict):
            det["id"] = data.get("id")
            det["type"] = data.get("type")
            det["name"] = (data.get("name") or "")[:120]
        return Step(
            key="token",
            ok=bool(ok),
            status=int(code),
            error="" if ok else _sanitize(str(data)[:400]),
            details=det,
        )

    return _run


def _check_database(token: str, database_id: str):
    def _run(timeout_s: float) -> Step:
        code, data = _http_request(
            "GET",
            f"{NOTION_API}/databases/{database_id}",
            token=token,
            json_body=None,
            timeout_s=timeout_s,
        )
        ok = int(code) == 200
        det: dict[str, Any] = {}
        if isinstance(data, dict):
            det["id"] = data.get("id")
            det["title"] = " ".join(
                [t.get("plain_text", "") for t in (data.get("title") or []) if isinstance(t, dict)]
            )[:180]
            det["url"] = (data.get("url") or "")[:220]
            det["properties_count"] = len(_extract_db_properties(data))
        return Step(
            key="database",
            ok=bool(ok),
            status=int(code),
            error="" if ok else _sanitize(str(data)[:500]),
            details=det,
        )

    return _run


def _check_properties(db_payload: Any, prop_names: dict[str, str], *, strict: bool) -> Step:
    props = _extract_db_properties(db_payload)
    missing: list[str] = []
    types: dict[str, str] = {}
    unsupported: list[str] = []

    # We consider these the core contract for your logger:
    required_keys = ["title", "mint", "balance", "entry", "current", "profit", "reason", "time"]

    for k in required_keys:
        nm = prop_names.get(k, "")
        if not nm:
            missing.append(f"{k}:<empty_name>")
            continue
        p = props.get(nm)
        if not isinstance(p, dict):
            missing.append(nm)
            continue
        t = _type_of_prop(p)
        types[nm] = t
        if t not in ("title", "rich_text", "number", "date"):
            unsupported.append(f"{nm}:{t or 'unknown'}")

    ok = (len(missing) == 0) and (len(unsupported) == 0)
    if not strict:
        ok = True  # report issues but don't fail

    det = {
        "strict": int(bool(strict)),
        "missing": missing,
        "unsupported_types": unsupported,
        "types": types,
    }
    err = ""
    if strict and (missing or unsupported):
        err = _sanitize(
            f"property_contract_failed missing={missing[:8]} unsupported={unsupported[:8]}"
        )
    return Step(key="properties", ok=bool(ok), status=200 if ok else 422, error=err, details=det)


def _write_dummy_page(
    token: str, database_id: str, db_payload: Any, prop_names: dict[str, str], *, cleanup: bool
):
    """
    Create ONE dummy page using compatible property types discovered from DB schema.
    If cleanup=True, archive the created page afterward.
    """
    props_schema = _extract_db_properties(db_payload)

    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    sample = {
        "text": f"TEST-{int(time.time())}",
        "mint": "So11111111111111111111111111111111111111112",
        "reason": "Notion connection test ✅",
        "iso": now_iso,
        "num": 1.2345,
    }

    # Build properties with type-aware mapping
    out_props: dict[str, Any] = {}

    def _set_prop(env_key: str, kind: str, sample_override: dict[str, Any] | None = None) -> None:
        nm = prop_names.get(env_key, "")
        if not nm:
            return
        schema = props_schema.get(nm)
        if not isinstance(schema, dict):
            return
        t = _type_of_prop(schema)

        s = dict(sample)
        if sample_override:
            s.update(sample_override)

        val = _make_prop_value(t, s)
        if val is None:
            return
        out_props[nm] = val

    # Title + core fields
    _set_prop("title", "text", {"text": sample["text"]})
    _set_prop("mint", "text", {"text": sample["mint"]})
    _set_prop("balance", "num", {"num": 1.2345})
    _set_prop("entry", "num", {"num": 0.0123})
    _set_prop("current", "num", {"num": 0.0456})
    _set_prop("profit", "num", {"num": round((0.0456 - 0.0123) * 1.2345, 6)})
    _set_prop("reason", "text", {"text": sample["reason"]})
    _set_prop("time", "date", {"iso": now_iso})

    body = {"parent": {"database_id": database_id}, "properties": out_props}

    code, data = _http_request(
        "POST",
        f"{NOTION_API}/pages",
        token=token,
        json_body=body,
        timeout_s=float(_envf("NOTION_TEST_TIMEOUT_S", 12.0)),
    )
    if int(code) not in (200, 201):
        return Step(
            key="write",
            ok=False,
            status=int(code),
            error=_sanitize(str(data)[:700]),
            details={
                "note": "page_create_failed",
                "properties_sent": sorted(list(out_props.keys()))[:40],
            },
        )

    page_id = data.get("id") if isinstance(data, dict) else None
    page_url = data.get("url") if isinstance(data, dict) else None
    det = {
        "page_id": page_id,
        "url": page_url,
        "properties_sent": sorted(list(out_props.keys()))[:40],
    }

    if cleanup and page_id:
        # Archive the page (Notion "delete" is archive=true)
        code2, data2 = _http_request(
            "PATCH",
            f"{NOTION_API}/pages/{page_id}",
            token=token,
            json_body={"archived": True},
            timeout_s=float(_envf("NOTION_TEST_TIMEOUT_S", 12.0)),
        )
        det["cleanup_status"] = int(code2)
        if int(code2) not in (200,):
            det["cleanup_error"] = _sanitize(str(data2)[:400])

    return Step(key="write", ok=True, status=int(code), error="", details=det)


# =============================================================================
# runner
# =============================================================================
async def _amain(args: argparse.Namespace) -> int:
    json_only = bool(args.json_only or _envb("NOTION_TEST_JSON_ONLY", False))
    emit_json = bool(
        args.json
        or args.json_only
        or _envb("NOTION_TEST_JSON", True)
        or _envb("NOTION_TEST_JSON_ONLY", False)
    )
    verbose = bool(args.verbose or _envb("NOTION_TEST_VERBOSE", False))

    token = _token_from_env() if not args.token else str(args.token).strip()
    database_id = _database_id_from_env() if not args.database_id else str(args.database_id).strip()

    if not token:
        msg = "Missing NOTION_API_KEY (or NOTION_TOKEN) / --token"
        if emit_json:
            print(json.dumps({"ok": False, "ts": _ts(), "error": msg}, ensure_ascii=False))
        _hlog(f"FAIL {msg}", json_only=json_only)
        return RC_MISCONFIG

    if not database_id:
        msg = "Missing NOTION_DATABASE_ID / --database-id"
        if emit_json:
            print(json.dumps({"ok": False, "ts": _ts(), "error": msg}, ensure_ascii=False))
        _hlog(f"FAIL {msg}", json_only=json_only)
        return RC_MISCONFIG

    strict_props = bool(_envb("NOTION_TEST_STRICT_PROPS", True))
    if args.soft:
        strict_props = False

    timeout_s = float(args.timeout or _envf("NOTION_TEST_TIMEOUT_S", 12.0))
    timeout_s = max(3.0, timeout_s)

    prop_names = _prop_names_from_env()

    summary: dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "mode": "write" if args.write else "read_only",
        "token_hint": _mask_token(token),
        "database_id": database_id[:8] + "…" + database_id[-4:]
        if len(database_id) > 14
        else database_id,
        "strict_props": int(bool(strict_props)),
        "steps": [],
    }

    # Step 1: token
    st_token = await _step_with_retries(
        "token", _check_me(token), timeout_s=timeout_s, json_only=json_only, verbose=verbose
    )
    # Step 2: database
    st_db = await _step_with_retries(
        "database",
        _check_database(token, database_id),
        timeout_s=timeout_s,
        json_only=json_only,
        verbose=verbose,
    )

    summary["steps"].append(asdict(st_token))
    summary["steps"].append(asdict(st_db))

    if not st_token.ok or not st_db.ok:
        summary["ok"] = False
        if emit_json:
            print(json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str))
        _hlog("NOT OK (token/db)", json_only=json_only)
        # classify network-ish failures
        if (st_token.status == 0 and st_token.error) or (st_db.status == 0 and st_db.error):
            return RC_NET
        return RC_FAIL

    # We need the raw db payload for property checks + optional writes.
    # Re-fetch once to get full body (requests already fetched; but we only stored partial in Step.details).
    def _fetch_db(timeout_s2: float) -> Step:
        code, data = _http_request(
            "GET",
            f"{NOTION_API}/databases/{database_id}",
            token=token,
            json_body=None,
            timeout_s=timeout_s2,
        )
        ok = int(code) == 200
        return Step(
            key="database_body",
            ok=bool(ok),
            status=int(code),
            error="" if ok else _sanitize(str(data)[:500]),
            details={"body": data},
        )

    st_db_body = await _step_with_retries(
        "database_body", _fetch_db, timeout_s=timeout_s, json_only=True, verbose=False
    )
    db_payload = st_db_body.details.get("body") if isinstance(st_db_body.details, dict) else None

    # Step 3: properties
    st_props = _check_properties(db_payload, prop_names, strict=strict_props)
    summary["steps"].append(asdict(st_props))

    if strict_props and not st_props.ok:
        summary["ok"] = False
        if emit_json:
            print(json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str))
        _hlog("NOT OK (properties contract)", json_only=json_only)
        return RC_FAIL

    # Optional Step 4: write
    if args.write:
        import asyncio

        try:
            st_write: Step = await asyncio.to_thread(
                _write_dummy_page,
                token,
                database_id,
                db_payload,
                prop_names,
                cleanup=bool(args.cleanup),
            )
        except Exception as e:
            st_write = Step(
                key="write",
                ok=False,
                status=0,
                error=_sanitize(f"{type(e).__name__}: {e}"),
                details={},
            )
        summary["steps"].append(asdict(st_write))
        if not st_write.ok:
            summary["ok"] = False
            if emit_json:
                print(json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str))
            _hlog("NOT OK (write test failed)", json_only=json_only)
            return RC_WRITE_FAIL

    summary["ok"] = True
    if emit_json:
        print(json.dumps(_safe_jsonable(summary), ensure_ascii=False, default=str))
    _hlog("OK", json_only=json_only)
    return RC_OK


def main() -> None:
    ap = argparse.ArgumentParser(description="God-Tier Notion canary (safe by default).")
    ap.add_argument(
        "--token", type=str, default="", help="Override token (else NOTION_API_KEY/NOTION_TOKEN)."
    )
    ap.add_argument(
        "--database-id", type=str, default="", help="Override DB id (else NOTION_DATABASE_ID)."
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        help="HTTP timeout seconds (default env NOTION_TEST_TIMEOUT_S=12).",
    )

    ap.add_argument(
        "--soft",
        action="store_true",
        help="Do not fail if properties are missing/unsupported (report only).",
    )
    ap.add_argument(
        "--write", action="store_true", help="Opt-in: create a dummy page in the database."
    )
    ap.add_argument(
        "--cleanup", action="store_true", help="If --write, archive the created page afterward."
    )

    ap.add_argument("--json", action="store_true", help="Emit JSON to STDOUT.")
    ap.add_argument("--json-only", action="store_true", help="Only JSON output (no human logs).")
    ap.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose human logs to STDERR (or env NOTION_TEST_VERBOSE=1).",
    )

    args = ap.parse_args()

    _bootstrap_repo_root()
    _maybe_load_dotenv()

    # Windows asyncio stability
    with contextlib.suppress(Exception):
        import asyncio

        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]

    import asyncio

    try:
        rc = asyncio.run(_amain(args))
    except KeyboardInterrupt:
        rc = 130
    except Exception as e:
        # Safe failure
        msg = _sanitize(f"{type(e).__name__}: {e}")
        if (
            args.json
            or args.json_only
            or _envb("NOTION_TEST_JSON", True)
            or _envb("NOTION_TEST_JSON_ONLY", False)
        ):
            print(json.dumps({"ok": False, "ts": _ts(), "error": msg[:900]}, ensure_ascii=False))
        else:
            _hlog(f"FAIL unhandled {msg}", json_only=False)
        rc = RC_FAIL

    raise SystemExit(int(rc))


if __name__ == "__main__":
    main()
