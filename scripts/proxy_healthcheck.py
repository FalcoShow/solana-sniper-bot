from __future__ import annotations

"""
scripts/proxy_healthcheck.py — Proxy Egress Sanity Check (P0 LOCK-IN)
=====================================================================

Purpose
-------
Verify outbound HTTP(S) connectivity *through your configured proxy*.
This is a surgical tool for debugging Render/local egress routing issues
without touching any trading logic.

What it checks
--------------
- Proxy env is present and parseable via utils_pkg.proxy_client.proxy_cfg()
- A single GET request succeeds through the proxy (default: ipify)
- Returns structured output (JSON) for automation / CI / Render shells

Security
--------
- Never prints proxy credentials
- Masks URLs that might include user:pass@host

Exit codes
----------
0  success (HTTP 200 and response parsed)
1  failure (missing proxy config, request error, or non-200)
130 Ctrl-C

Usage
-----
python -m scripts.proxy_healthcheck
python -m scripts.proxy_healthcheck --json
python -m scripts.proxy_healthcheck --json-only
python -m scripts.proxy_healthcheck --url https://api.ipify.org?format=json
python -m scripts.proxy_healthcheck --timeout 20 --retries 2

Env
---
PROXY_HEALTH_URL=https://api.ipify.org?format=json
PROXY_HEALTH_TIMEOUT_S=20
PROXY_HEALTH_RETRIES=1
PROXY_HEALTH_JSON=1
PROXY_HEALTH_JSON_ONLY=1

Notes
-----
- The proxy config comes from utils_pkg.proxy_client.proxy_cfg()
  which is expected to return: (proxy_url, proxy_auth, insecure_ssl_bool)
"""

import argparse
import asyncio
import contextlib
import json
import os
import re
import sys
import time
import traceback
from typing import Any

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_URL_CRED_RE = re.compile(r"(://)([^/@\s]+)@")  # masks user:pass@ in URLs


# =============================================================================
# small helpers
# =============================================================================
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


def _mask_url(u: str) -> str:
    if not u:
        return ""
    out = _URL_CRED_RE.sub(r"\1***@", str(u))
    # drop query details for safety/noise
    if "?" in out:
        out = out.split("?", 1)[0] + "?..."
    return out


def _short_exc(e: BaseException, n: int = 500) -> str:
    s = f"{type(e).__name__}: {e}"
    return (s[:n] + "…") if len(s) > n else s


def _safe_json(x: Any) -> Any:
    try:
        json.dumps(x, default=str)
        return x
    except Exception:
        return str(x)


def _stderr(msg: str, *, json_only: bool) -> None:
    if json_only:
        return
    try:
        sys.stderr.write(f"[proxy_healthcheck] {_ts()} {msg}\n")
        sys.stderr.flush()
    except Exception:
        pass


# =============================================================================
# proxy_cfg import (signature drift tolerant)
# =============================================================================
def _load_proxy_cfg():
    with contextlib.suppress(Exception):
        from utils_pkg.proxy_client import proxy_cfg  # type: ignore

        return proxy_cfg
    return None


def _get_proxy_tuple() -> tuple[str | None, Any, bool]:
    """
    Returns (proxy_url, proxy_auth, insecure_ssl_bool).

    We allow proxy_cfg() to evolve, but we will normalize here.
    """
    fn = _load_proxy_cfg()
    if not callable(fn):
        return None, None, False

    val = fn()
    # expected: (proxy, proxy_auth, insecure)
    if isinstance(val, tuple | list) and len(val) >= 3:
        proxy = val[0]
        proxy_auth = val[1]
        insecure = bool(val[2])
        return (str(proxy) if proxy else None), proxy_auth, insecure

    # tolerate older shapes
    if isinstance(val, tuple | list) and len(val) == 2:
        proxy = val[0]
        proxy_auth = val[1]
        return (str(proxy) if proxy else None), proxy_auth, False

    # unknown
    return None, None, False


# =============================================================================
# main logic
# =============================================================================
async def _one_request(
    *,
    url: str,
    proxy: str,
    proxy_auth: Any,
    insecure_ssl: bool,
    timeout_s: float,
) -> dict[str, Any]:
    try:
        import aiohttp  # type: ignore
    except Exception as e:
        raise RuntimeError("aiohttp is required for proxy_healthcheck (pip install aiohttp)") from e

    timeout = aiohttp.ClientTimeout(total=max(1.0, float(timeout_s)))

    connector = aiohttp.TCPConnector(ssl=False) if insecure_ssl else None

    t0 = time.time()
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as sess:
        async with sess.get(url, proxy=proxy, proxy_auth=proxy_auth) as resp:
            text = await resp.text()
            ms = int((time.time() - t0) * 1000)

            payload: dict[str, Any] = {
                "status": int(resp.status),
                "elapsed_ms": ms,
                "content_type": (resp.headers.get("content-type") or "")[:120],
            }

            # parse json if possible
            obj = None
            with contextlib.suppress(Exception):
                obj = json.loads(text)

            if isinstance(obj, dict):
                payload["json"] = _safe_json(obj)
            else:
                # keep only a short snippet
                payload["text"] = (text[:280] + "…") if len(text) > 280 else text

            return payload


async def _amain(args: argparse.Namespace) -> int:
    json_only = bool(args.json_only or _envb("PROXY_HEALTH_JSON_ONLY", False))
    emit_json = bool(args.json or _envb("PROXY_HEALTH_JSON", False) or json_only)

    url = (args.url or _envs("PROXY_HEALTH_URL", "https://api.ipify.org?format=json")).strip()
    timeout_s = float(args.timeout or _envf("PROXY_HEALTH_TIMEOUT_S", 20.0))
    retries = int(args.retries if args.retries is not None else _envi("PROXY_HEALTH_RETRIES", 1))

    proxy, proxy_auth, insecure = _get_proxy_tuple()

    base: dict[str, Any] = {
        "ts": _ts(),
        "ok": False,
        "url": _mask_url(url),
        "timeout_s": float(timeout_s),
        "retries": int(retries),
        "proxy_set": bool(proxy),
        "proxy_masked": _mask_url(proxy or ""),
        "insecure_ssl": bool(insecure),
    }

    if not proxy:
        base["error"] = (
            "Proxy env vars missing/unparseable (expected PROXY_* set for utils_pkg.proxy_client.proxy_cfg())"
        )
        if emit_json:
            print(json.dumps(_safe_json(base), ensure_ascii=False, default=str))
        else:
            _stderr(base["error"], json_only=False)
        return 1

    last_err: str | None = None
    attempt_payload: dict[str, Any] | None = None

    tries = max(1, retries)
    for i in range(tries):
        try:
            if not json_only:
                _stderr(
                    f"attempt {i + 1}/{tries} url={_mask_url(url)} proxy={_mask_url(proxy)} insecure_ssl={int(insecure)}",
                    json_only=False,
                )

            attempt_payload = await _one_request(
                url=url,
                proxy=proxy,
                proxy_auth=proxy_auth,
                insecure_ssl=insecure,
                timeout_s=timeout_s,
            )

            status = int(attempt_payload.get("status") or 0)
            ok = status == 200

            base["attempt"] = i + 1
            base["response"] = _safe_json(attempt_payload)
            base["ok"] = bool(ok)

            if ok:
                break

            last_err = f"non-200 status={status}"
        except TimeoutError:
            last_err = "timeout"
        except Exception as e:
            last_err = _short_exc(e)

        # small backoff between retries
        if i < tries - 1:
            await asyncio.sleep(min(1.5, 0.25 * (i + 1)))

    if last_err and not base.get("ok"):
        base["error"] = last_err

    if emit_json:
        print(json.dumps(_safe_json(base), ensure_ascii=False, default=str))
    else:
        if base.get("ok"):
            _stderr("OK: proxy request succeeded", json_only=False)
            _stderr(
                f"status={base.get('response', {}).get('status')} elapsed_ms={base.get('response', {}).get('elapsed_ms')}",
                json_only=False,
            )
        else:
            _stderr(f"FAIL: {base.get('error', 'unknown_error')}", json_only=False)

    return 0 if base.get("ok") else 1


def main() -> None:
    ap = argparse.ArgumentParser(description="Proxy egress sanity check (through configured proxy)")
    ap.add_argument(
        "--url", type=str, default="", help="Probe URL (default: PROXY_HEALTH_URL or ipify)"
    )
    ap.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        help="Total request timeout seconds (0 uses env/default)",
    )
    ap.add_argument(
        "--retries", type=int, default=None, help="Retries (default: PROXY_HEALTH_RETRIES or 1)"
    )
    ap.add_argument("--json", action="store_true", help="Emit JSON result to STDOUT")
    ap.add_argument("--json-only", action="store_true", help="Only JSON output (no human logs)")
    args = ap.parse_args()

    try:
        rc = asyncio.run(_amain(args))
    except KeyboardInterrupt:
        rc = 130
    except Exception:
        err = _short_exc(Exception(traceback.format_exc()))
        payload = {"ok": False, "ts": _ts(), "error": err}
        if (
            args.json
            or args.json_only
            or _envb("PROXY_HEALTH_JSON", False)
            or _envb("PROXY_HEALTH_JSON_ONLY", False)
        ):
            print(json.dumps(_safe_json(payload), ensure_ascii=False, default=str))
        else:
            _stderr(f"FAIL: unhandled {err}", json_only=False)
        rc = 1

    raise SystemExit(rc)


if __name__ == "__main__":
    main()
