#!/usr/bin/env python3
"""
scripts/smoke_pw.py

Optional Playwright smoke test (dev tool).
- Graceful when Playwright isn't installed.
- Checks Pump.fun page load (title) and optional API health request.

Examples:
  py scripts/smoke_pw.py
  py scripts/smoke_pw.py --url https://pump.fun/board
  py scripts/smoke_pw.py --api-health https://frontend-api.pump.fun/health
  py scripts/smoke_pw.py --api-health https://frontend-api.pump.fun/health --proxy http://user:pass@host:port

Env:
  PLAYWRIGHT_PROXY   Optional proxy server URL (used if --proxy not provided)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys


def _eprint(msg: str) -> None:
    print(msg, file=sys.stderr)


def _try_import_playwright() -> bool:
    try:
        import playwright  # noqa: F401
        from playwright.async_api import async_playwright  # noqa: F401

        return True
    except Exception:
        return False


def _install_hint() -> str:
    return (
        "\nPlaywright is not installed in this environment.\n\n"
        "Local (Windows):\n"
        "  pip install playwright\n"
        "  py -m playwright install chromium\n\n"
        "Linux/CI/Render (during build):\n"
        "  pip install playwright\n"
        "  python -m playwright install chromium\n"
    )


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Playwright smoke test (optional dev tool).")
    ap.add_argument(
        "--url",
        default=os.getenv("PUMPFUN_SMOKE_URL", "https://pump.fun/board"),
        help="Page URL to open (default https://pump.fun/board)",
    )
    ap.add_argument(
        "--api-health",
        default=os.getenv("PUMPFUN_API_HEALTH_URL", ""),
        help="Optional API URL to request (e.g., https://frontend-api.pump.fun/health)",
    )
    ap.add_argument(
        "--proxy",
        default=os.getenv("PLAYWRIGHT_PROXY", ""),
        help="Proxy server (e.g., http://user:pass@host:port). Overrides PLAYWRIGHT_PROXY.",
    )
    ap.add_argument(
        "--timeout-ms",
        type=int,
        default=int(os.getenv("PUMPFUN_SMOKE_TIMEOUT_MS", "120000")),
        help="Navigation timeout (ms)",
    )
    ap.add_argument("--headful", action="store_true", help="Run browser headful (default headless)")
    ap.add_argument("--json", action="store_true", help="Print JSON result summary")
    return ap.parse_args()


async def _run_smoke(url: str, api_health: str, proxy: str, timeout_ms: int, headful: bool) -> dict:
    from playwright.async_api import async_playwright

    result = {
        "page_url": url,
        "page_ok": False,
        "page_title": None,
        "api_url": api_health or None,
        "api_ok": None if not api_health else False,
        "api_status": None,
        "proxy_used": proxy or None,
    }

    async with async_playwright() as p:
        launch_kwargs = {"headless": (not headful)}
        if proxy:
            launch_kwargs["proxy"] = {"server": proxy}

        browser = await p.chromium.launch(**launch_kwargs)
        try:
            ctx = await browser.new_context()
            page = await ctx.new_page()

            # Page smoke
            resp = await page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
            # resp can be None for some navigations; still try to read title
            title = await page.title()
            result["page_title"] = title
            result["page_ok"] = bool(title) and (resp is None or (200 <= resp.status < 500))

            # API smoke (optional)
            if api_health:
                r = await ctx.request.get(api_health, timeout=min(30000, timeout_ms))
                result["api_status"] = r.status
                result["api_ok"] = 200 <= r.status < 300

        finally:
            await browser.close()

    return result


def main() -> None:
    args = parse_args()

    if not _try_import_playwright():
        _eprint(_install_hint())
        # Exit code 2 so CI can treat as "not runnable" if desired.
        sys.exit(2)

    try:
        res = asyncio.run(
            _run_smoke(
                url=args.url,
                api_health=args.api_health,
                proxy=args.proxy,
                timeout_ms=args.timeout_ms,
                headful=args.headful,
            )
        )
    except Exception as e:
        _eprint(f"[smoke_pw] FAILED: {e}")
        sys.exit(1)

    ok = bool(res.get("page_ok")) and (res.get("api_ok") in (None, True))

    if args.json:
        print(json.dumps(res, indent=2))
    else:
        print(f"[smoke_pw] page_ok={res['page_ok']} title={res['page_title']!r}")
        if res["api_url"]:
            print(
                f"[smoke_pw] api_ok={res['api_ok']} status={res['api_status']} url={res['api_url']}"
            )
        if res["proxy_used"]:
            print(f"[smoke_pw] proxy_used={res['proxy_used']}")

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
