#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/pf_cookie_refresher.py

Pump.fun Browser Cookie Refresher (interactive, secret-safe)
===========================================================

Purpose
-------
Interactive local tool to open Pump.fun in a real browser and export the
resulting cookies into our canonical cookie store via:

    utils_pkg.pf_cookie_store.save_pf_cookies(cookie_jar)

This is meant for legitimate, user-driven sessions (you complete any login /
verification in the browser). This script does NOT “solve” challenges; it only
waits and then saves cookies that exist.

Why this exists (P0)
--------------------
- Keep bot runtime clean: scanners/traders just read cookies from the store.
- Keep secrets out of logs: prints masked summaries only.
- Deterministic exit codes for automation / troubleshooting.

Env
---
PF_COOKIE_START_URL      default: https://pump.fun/board
PF_COOKIE_UA             default: a modern desktop UA string
PF_COOKIE_HEADLESS       0|1 (default 0)  (headful recommended)
PF_COOKIE_WAIT_S         max wait seconds (default 120)
PF_COOKIE_POLL_S         poll interval seconds (default 2)

Cookie selection:
PF_COOKIE_NAMES          CSV allowlist of cookie names to save
                          - default: "auth_token"
                          - set to "*" to save ALL cookies
PF_COOKIE_REQUIRED       CSV list that MUST be present to succeed
                          - default: none (empty)

Profile / persistence:
PF_COOKIE_PROFILE_DIR    persistent profile directory (default: ./.pf_profile)

CLI
---
python -m scripts.pf_cookie_refresher
python -m scripts.pf_cookie_refresher --url https://pump.fun/board --wait 180
python -m scripts.pf_cookie_refresher --names "*" --required "auth_token" --json

Exit codes
----------
0 success (cookies saved)
2 required cookie(s) missing / no cookies selected
3 missing Playwright or browser not installed
4 runtime error / user aborted
"""

import argparse
import asyncio
import contextlib
import json
import os
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Repo root + dotenv (local dev convenience)
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # pragma: no cover
    from dotenv import find_dotenv, load_dotenv  # type: ignore

    load_dotenv(find_dotenv(), override=False)
except Exception:  # pragma: no cover
    pass

# ---------------------------------------------------------------------------
# Canonical cookie store
# ---------------------------------------------------------------------------
try:
    from utils_pkg.pf_cookie_store import save_pf_cookies  # type: ignore
except Exception as e:
    print(
        f"[pf_cookie_refresher] ERROR: cannot import save_pf_cookies: {type(e).__name__}: {e}",
        file=sys.stderr,
    )
    raise

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


def _envf(k: str, d: float) -> float:
    try:
        return float(_envs(k, str(d)) or str(d))
    except Exception:
        return d


def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in (s or "").replace(";", ",").split(",") if p.strip()]


def _mask_cookie_value(v: str) -> str:
    s = str(v or "")
    if len(s) <= 6:
        return "***"
    return s[:3] + "***" + s[-3:]


def _hlog(msg: str, *, quiet: bool) -> None:
    if quiet:
        return
    sys.stderr.write(f"[pf_cookie_refresher] {_ts()} {msg}\n")
    sys.stderr.flush()


def _normalize_url(u: str) -> str:
    u = (u or "").strip()
    if not u:
        return "https://pump.fun/board"
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "https://" + u.lstrip("/")
    return u


def _parse_names(raw: str, *, default: Sequence[str]) -> tuple[bool, set[str]]:
    """
    Returns (all_cookies, names_set).
    raw="*" => all cookies
    raw=""  => default
    """
    raw = (raw or "").strip()
    if raw == "*":
        return True, set()
    if not raw:
        return False, set(default)
    return False, set(_split_csv(raw))


def _select_cookies(
    cookies: list[dict[str, Any]], *, all_cookies: bool, allow: set[str]
) -> dict[str, str]:
    jar: dict[str, str] = {}
    for c in cookies or []:
        name = str(c.get("name") or "").strip()
        val = str(c.get("value") or "")
        if not name:
            continue
        if all_cookies or (name in allow):
            jar[name] = val
    return jar


def _is_missing_playwright_or_browser(err: BaseException) -> bool:
    """
    Treat both "playwright not installed" and "browser not installed" as exit code 3.
    """
    msg = str(err).lower()
    # Common Playwright "install browsers" errors
    needles = (
        "playwright install",
        "executable doesn't exist",
        "executable doesn’t exist",
        "browser has been closed",
        "failed to launch",
        "chromium",
        "webkit",
        "firefox",
        "missing dependency",
        "module named 'playwright'",
    )
    return any(n in msg for n in needles)


class _PlaywrightMissing(RuntimeError):
    pass


@dataclass(frozen=True)
class _Cfg:
    url: str
    ua: str
    headless: bool
    wait_s: float
    poll_s: float
    profile_dir: Path
    all_cookies: bool
    allow_names: set[str]
    required_names: set[str]
    quiet: bool


async def _run_refresh(cfg: _Cfg) -> dict[str, str]:
    """
    Launches a real browser context (persistent profile), navigates to URL,
    then waits until either:
      - required cookies are present (if required_names set), OR
      - at least one selected cookie exists, OR
      - timeout reached.
    """
    try:
        from playwright.async_api import async_playwright  # type: ignore
    except Exception as e:
        raise _PlaywrightMissing(f"missing_playwright: {type(e).__name__}: {e}") from e

    cfg.profile_dir.mkdir(parents=True, exist_ok=True)

    _hlog(
        f"launching browser headless={int(cfg.headless)} profile_dir={cfg.profile_dir}",
        quiet=cfg.quiet,
    )
    _hlog(f"opening url={cfg.url}", quiet=cfg.quiet)

    async with async_playwright() as pw:
        try:
            ctx = await pw.chromium.launch_persistent_context(
                user_data_dir=str(cfg.profile_dir),
                headless=cfg.headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
                viewport={"width": 1280, "height": 800},
                locale="en-US",
                user_agent=cfg.ua or None,
            )
        except Exception as e:
            # Often indicates "browser not installed" or launch failure
            raise _PlaywrightMissing(f"playwright_launch_failed: {type(e).__name__}: {e}") from e

        try:
            page = await ctx.new_page()
            await page.goto(
                cfg.url,
                wait_until="domcontentloaded",
                timeout=int(max(30.0, cfg.wait_s) * 1000),
            )

            deadline = time.monotonic() + max(5.0, cfg.wait_s)
            jar: dict[str, str] = {}

            def _has_required(j: dict[str, str]) -> bool:
                return (not cfg.required_names) or cfg.required_names.issubset(set(j.keys()))

            while time.monotonic() < deadline:
                cookies = await ctx.cookies()
                jar = _select_cookies(cookies, all_cookies=cfg.all_cookies, allow=cfg.allow_names)
                if jar and _has_required(jar):
                    break
                await page.wait_for_timeout(int(max(0.5, cfg.poll_s) * 1000))

            return jar
        finally:
            with contextlib.suppress(Exception):
                await ctx.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Interactive Pump.fun cookie refresher (Playwright).")
    p.add_argument(
        "--url",
        default=_envs("PF_COOKIE_START_URL", _envs("PF_START_URL", "https://pump.fun/board")),
    )
    p.add_argument(
        "--ua",
        default=_envs(
            "PF_COOKIE_UA",
            _envs(
                "PUMPFUN_UA",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ),
        ),
    )

    # Preserve existing semantics; add --headful to explicitly force false (new, non-breaking).
    p.add_argument("--headless", action="store_true", default=_envb("PF_COOKIE_HEADLESS", False))
    p.add_argument(
        "--headful", action="store_true", help="Force headful mode (overrides --headless/env)."
    )

    p.add_argument(
        "--wait",
        type=float,
        default=float(_envf("PF_COOKIE_WAIT_S", float(_envf("PF_CF_WAIT_MS", 120000) / 1000.0))),
    )
    p.add_argument("--poll", type=float, default=float(_envf("PF_COOKIE_POLL_S", 2.0)))
    p.add_argument(
        "--profile-dir",
        default=_envs("PF_COOKIE_PROFILE_DIR", str((ROOT / ".pf_profile").resolve())),
    )
    p.add_argument(
        "--names",
        default=_envs("PF_COOKIE_NAMES", "auth_token"),
        help='CSV cookie allowlist to save; "*" saves all cookies. Default: auth_token',
    )
    p.add_argument(
        "--required",
        default=_envs("PF_COOKIE_REQUIRED", ""),
        help="CSV cookies that must exist to exit successfully (default: none).",
    )
    p.add_argument("--json", action="store_true", help="Emit JSON output (no secret values).")
    p.add_argument("--quiet", action="store_true", help="Reduce logs.")
    return p.parse_args()


def _set_windows_event_loop_policy() -> None:
    with contextlib.suppress(Exception):
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]


def main() -> int:
    args = parse_args()

    url = _normalize_url(args.url)
    ua = str(args.ua or "").strip()
    headless = bool(args.headless)
    if bool(getattr(args, "headful", False)):
        headless = False

    wait_s = float(args.wait)
    poll_s = float(args.poll)
    quiet = bool(args.quiet)

    profile_dir = Path(str(args.profile_dir)).expanduser().resolve()

    all_cookies, allow_names = _parse_names(str(args.names or ""), default=("auth_token",))
    required_names = set(_split_csv(str(args.required or "")))

    _hlog(f"names={'*' if all_cookies else ','.join(sorted(allow_names))}", quiet=quiet)
    _hlog(
        f"required={','.join(sorted(required_names)) if required_names else '-'} wait_s={wait_s:.1f} poll_s={poll_s:.1f}",
        quiet=quiet,
    )

    _set_windows_event_loop_policy()

    cfg = _Cfg(
        url=url,
        ua=ua,
        headless=headless,
        wait_s=wait_s,
        poll_s=poll_s,
        profile_dir=profile_dir,
        all_cookies=all_cookies,
        allow_names=allow_names,
        required_names=required_names,
        quiet=quiet,
    )

    try:
        jar = asyncio.run(_run_refresh(cfg))
    except KeyboardInterrupt:
        print("[pf_cookie_refresher] cancelled", file=sys.stderr)
        return 4
    except _PlaywrightMissing as e:
        print(
            "[pf_cookie_refresher] ERROR: Playwright is missing or the browser is not installed.\n"
            "Install (local):  pip install playwright\n"
            "Then:            python -m playwright install chromium\n"
            f"Details: {e}",
            file=sys.stderr,
        )
        return 3
    except Exception as e:
        # If Playwright bubbles a launch/install error in a different shape, treat it as code 3.
        if _is_missing_playwright_or_browser(e):
            print(
                "[pf_cookie_refresher] ERROR: Playwright/browser not ready.\n"
                "Try: pip install playwright && python -m playwright install chromium\n"
                f"Details: {type(e).__name__}: {e}",
                file=sys.stderr,
            )
            return 3
        print(f"[pf_cookie_refresher] ERROR: {type(e).__name__}: {e}", file=sys.stderr)
        return 4

    if not jar:
        print(
            "[pf_cookie_refresher] No cookies selected/found before timeout.\n"
            "Tip: run headful (default) and complete any site prompts, then re-run with a longer --wait.\n"
            "You can also set --names '*' to save all cookies.",
            file=sys.stderr,
        )
        return 2

    missing_required = sorted(list(cfg.required_names.difference(set(jar.keys()))))
    if missing_required:
        print(
            f"[pf_cookie_refresher] Required cookies missing: {', '.join(missing_required)}",
            file=sys.stderr,
        )
        return 2

    try:
        save_pf_cookies(jar)
    except Exception as e:
        print(
            f"[pf_cookie_refresher] ERROR: failed to save cookies: {type(e).__name__}: {e}",
            file=sys.stderr,
        )
        return 4

    masked = {k: _mask_cookie_value(v) for k, v in jar.items()}
    if bool(args.json):
        print(json.dumps({"ok": True, "saved": True, "cookies": masked}, indent=2))
    else:
        print("[pf_cookie_refresher] ✅ cookies saved:", masked)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
