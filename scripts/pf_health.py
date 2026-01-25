#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pf_health.py

Pump.fun API Health Probe (P0 health tool, Render+local identical)
=================================================================

Purpose
-------
A small, deterministic health probe for Pump.fun’s frontend API TRPC endpoint.
Used to answer: “is PF reachable from *this* runtime right now?”

Design goals (P0 contract)
--------------------------
- Works the same locally and on Render.
- Secret-safe logs (no tokens printed).
- Deterministic retry loop with clear exit codes.
- Supports multiple base URLs (failover) via env/CLI.
- Prefers utils_pkg.pump_client.pump_post (canonical client), but can fall back
  to a minimal aiohttp client if needed.

Env
---
PUMPFUN_API_BASES   Comma-separated list of base URLs
                   default: https://frontend-api.pump.fun
PUMPFUN_API_BASE    Single base URL (legacy); used if BASES not set

Optional:
PUMPFUN_HEALTH_ENDPOINT  override endpoint path (default below)

CLI
---
python -m scripts.pf_health
python -m scripts.pf_health --once
python -m scripts.pf_health --bases "https://frontend-api.pump.fun,https://frontend-api.pump.fun"
python -m scripts.pf_health --max-wait 60 --interval 5 --timeout 15
python -m scripts.pf_health --json

Exit codes
----------
0 success
1 all bases failed / timed out
2 missing deps (requests/aiohttp) or missing pump_post and no fallback
3 invalid args
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# -----------------------------------------------------------------------------
# Repo root + dotenv (local only)
# -----------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)
except Exception:
    pass

# -----------------------------------------------------------------------------
# Prefer canonical pump client
# -----------------------------------------------------------------------------
_pump_post = None
_pump_client_err: str | None = None
try:
    from utils_pkg.pump_client import pump_post as _pump_post  # type: ignore
except Exception as e:
    _pump_post = None
    _pump_client_err = f"{type(e).__name__}: {e}"

# -----------------------------------------------------------------------------
# Optional fallback deps
# -----------------------------------------------------------------------------
_HAVE_AIOHTTP = False
try:
    import aiohttp  # type: ignore

    _HAVE_AIOHTTP = True
except Exception:
    _HAVE_AIOHTTP = False

_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    s = v if v is not None else d
    s = str(s).strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    return s


def _envb(k: str, d: bool = False) -> bool:
    v = os.getenv(k)
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return d


def _hlog(msg: str, *, quiet: bool) -> None:
    if quiet:
        return
    sys.stderr.write(f"[pf_health] {_ts()} {msg}\n")
    sys.stderr.flush()


def _parse_bases(raw: str) -> list[str]:
    parts = [p.strip() for p in (raw or "").split(",")]
    bases: list[str] = []
    for p in parts:
        if not p:
            continue
        if not (p.startswith("http://") or p.startswith("https://")):
            # default to https if user forgot scheme
            p = "https://" + p.lstrip("/")
        bases.append(p.rstrip("/"))
    # de-dup preserving order
    seen = set()
    out: list[str] = []
    for b in bases:
        if b not in seen:
            out.append(b)
            seen.add(b)
    return out


def _default_endpoint() -> str:
    return (
        _envs("PUMPFUN_HEALTH_ENDPOINT", "/api/trpc/board.latest?batch=1")
        or "/api/trpc/board.latest?batch=1"
    )


def _default_body() -> dict[str, Any]:
    # This is the minimal payload your original script used
    return {"0": {"json": {}}}


@dataclass
class ProbeResult:
    ok: bool
    base: str
    endpoint: str
    elapsed_s: float
    bytes: int
    head: str
    error: str | None = None


async def _probe_with_canonical_client(
    endpoint: str,
    body: dict[str, Any],
    *,
    timeout_s: float,
) -> str:
    """
    Uses utils_pkg.pump_client.pump_post which should already honor
    PUMPFUN_API_BASES / internal routing in your codebase.
    """
    assert _pump_post is not None
    txt = await _pump_post(endpoint, json_body=body, timeout=timeout_s)  # type: ignore[misc]
    return txt if isinstance(txt, str) else str(txt)


async def _probe_with_aiohttp(
    base: str,
    endpoint: str,
    body: dict[str, Any],
    *,
    timeout_s: float,
) -> str:
    if not _HAVE_AIOHTTP:
        raise RuntimeError("aiohttp not installed (and canonical pump_post unavailable).")

    url = f"{base}{endpoint}"
    timeout = aiohttp.ClientTimeout(total=timeout_s)
    async with aiohttp.ClientSession(timeout=timeout) as sess:
        async with sess.post(url, json=body) as resp:
            txt = await resp.text()
            # treat non-200 as failure but include response head for debugging
            if resp.status < 200 or resp.status >= 300:
                raise RuntimeError(f"HTTP {resp.status}: {txt[:300]}")
            return txt


async def probe_once(
    bases: Sequence[str],
    endpoint: str,
    body: dict[str, Any],
    *,
    timeout_s: float,
    quiet: bool,
) -> ProbeResult:
    """
    Try bases in order. If canonical client exists, it will likely manage bases itself,
    but we still iterate to give deterministic “which base works” behavior for fallback.
    """
    t0 = time.perf_counter()

    # If canonical client exists, we just call it once; it should do the right thing.
    if _pump_post is not None:
        try:
            txt = await _probe_with_canonical_client(endpoint, body, timeout_s=timeout_s)
            elapsed = time.perf_counter() - t0
            head = txt[:240].replace("\n", " ")
            # base unknown (internal selection), so report env bases summary
            base_hint = (
                _envs("PUMPFUN_API_BASES", "")
                or _envs("PUMPFUN_API_BASE", "")
                or (bases[0] if bases else "")
            )
            return ProbeResult(
                True,
                base_hint or "auto",
                endpoint,
                elapsed,
                len(txt.encode("utf-8", "ignore")),
                head,
                None,
            )
        except Exception as e:
            elapsed = time.perf_counter() - t0
            err = f"{type(e).__name__}: {e}"
            head = ""
            # fall through to try aiohttp direct per-base (if available)
            _hlog(f"canonical pump_post failed: {err}", quiet=quiet)

    # Fallback: per-base direct POST
    last_err: str | None = None
    for base in bases:
        t1 = time.perf_counter()
        try:
            txt = await _probe_with_aiohttp(base, endpoint, body, timeout_s=timeout_s)
            elapsed = time.perf_counter() - t1
            head = txt[:240].replace("\n", " ")
            return ProbeResult(
                True, base, endpoint, elapsed, len(txt.encode("utf-8", "ignore")), head, None
            )
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            _hlog(f"base failed {base}: {last_err}", quiet=quiet)

    elapsed_all = time.perf_counter() - t0
    return ProbeResult(
        False,
        bases[0] if bases else "none",
        endpoint,
        elapsed_all,
        0,
        "",
        last_err or "unknown error",
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pump.fun health probe (TRPC board.latest).")
    p.add_argument("--bases", default="", help="Comma-separated base URLs (overrides env).")
    p.add_argument(
        "--endpoint",
        default="",
        help="Endpoint path (default env PUMPFUN_HEALTH_ENDPOINT or board.latest).",
    )
    p.add_argument(
        "--timeout", type=float, default=float(_envs("PUMPFUN_HEALTH_TIMEOUT_S", "20") or "20")
    )
    p.add_argument(
        "--interval", type=float, default=float(_envs("PUMPFUN_HEALTH_INTERVAL_S", "10") or "10")
    )
    p.add_argument(
        "--max-wait",
        type=float,
        default=float(_envs("PUMPFUN_HEALTH_MAX_WAIT_S", "0") or "0"),
        help="Max seconds to keep retrying; 0 = forever unless --once.",
    )
    p.add_argument("--once", action="store_true", help="Run a single probe attempt and exit.")
    p.add_argument("--json", action="store_true", help="Emit JSON output.")
    p.add_argument("--quiet", action="store_true", help="Suppress human logs.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    quiet = bool(args.quiet)

    endpoint = (args.endpoint.strip() or _default_endpoint()).strip()
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint

    timeout_s = float(args.timeout)
    interval_s = max(0.5, float(args.interval))
    max_wait_s = float(args.max_wait)

    # Bases: CLI > env BASES > env BASE
    raw_bases = args.bases.strip()
    if raw_bases:
        os.environ["PUMPFUN_API_BASES"] = raw_bases  # keep canonical client consistent too
    env_bases = _envs("PUMPFUN_API_BASES", "") or _envs(
        "PUMPFUN_API_BASE", "https://frontend-api.pump.fun"
    )
    bases = _parse_bases(raw_bases or env_bases)
    if not bases:
        bases = ["https://frontend-api.pump.fun"]

    if _pump_post is None and not _HAVE_AIOHTTP:
        _hlog(
            "ERROR: utils_pkg.pump_client.pump_post unavailable AND aiohttp not installed.",
            quiet=False,
        )
        if _pump_client_err:
            _hlog(f"pump_client import error: {_pump_client_err}", quiet=False)
        raise SystemExit(2)

    body = _default_body()

    _hlog(f"bases={','.join(bases)}", quiet=quiet)
    _hlog(
        f"endpoint={endpoint} timeout={timeout_s}s interval={interval_s}s max_wait={max_wait_s}s once={bool(args.once)}",
        quiet=quiet,
    )
    if _pump_post is None:
        _hlog("NOTE: using aiohttp fallback (canonical pump_post not available).", quiet=quiet)

    start = time.monotonic()
    attempt = 0

    while True:
        attempt += 1
        res = asyncio.run(probe_once(bases, endpoint, body, timeout_s=timeout_s, quiet=quiet))

        if res.ok:
            if args.json:
                print(
                    json.dumps(
                        {
                            "ok": True,
                            "attempt": attempt,
                            "base": res.base,
                            "endpoint": res.endpoint,
                            "elapsed_s": round(res.elapsed_s, 6),
                            "bytes": res.bytes,
                            "head": res.head,
                        },
                        indent=2,
                    )
                )
            else:
                print(
                    f"PF API OK ✅  attempt={attempt} base={res.base} elapsed={res.elapsed_s:.3f}s bytes={res.bytes}"
                )
                print(f"head: {res.head}")
            raise SystemExit(0)

        # failure
        if args.json:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "attempt": attempt,
                        "base": res.base,
                        "endpoint": res.endpoint,
                        "elapsed_s": round(res.elapsed_s, 6),
                        "error": res.error,
                    },
                    indent=2,
                )
            )
        else:
            _hlog(f"still down ❌ attempt={attempt} err={res.error}", quiet=quiet)

        if args.once:
            raise SystemExit(1)

        if max_wait_s > 0:
            elapsed = time.monotonic() - start
            if elapsed >= max_wait_s:
                _hlog(f"max_wait reached ({max_wait_s}s) — giving up.", quiet=False)
                raise SystemExit(1)

        time.sleep(interval_s)


if __name__ == "__main__":
    main()
