# utils_pkg/pump_browser_client.py
# Robust Pump.fun browser client for CF-protected tRPC POSTs.
# - Defaults to Playwright "chromium" (no system Chrome required)
# - Optional executable override via PUMP_BROWSER_EXECUTABLE_PATH
# - Proper warm-up (visit site + /cdn-cgi/trace) before API POST
# - Reuses persistent context to keep CF cookies
# - Retries with backoff on 530/5xx
# - Honors common Render envs: PLAYWRIGHT_* , CHROMIUM_FLAGS, PF_* etc.

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from typing import Iterable, Tuple

from playwright.async_api import async_playwright, BrowserContext, Playwright


# --------------------------
# Env helpers
# --------------------------

def _bool_env(name: str, default: bool = True) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() not in ("0", "false", "no", "")

def _str_env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return v if v is not None else default

def _int_env(name: str, default: int) -> int:
    v = os.environ.get(name)
    try:
        return int(v) if v is not None and str(v).strip() != "" else default
    except Exception:
        return default

def _list_env(name: str, default_csv: str) -> list[str]:
    raw = _str_env(name, default_csv)
    return [s.strip() for s in raw.split(",") if s.strip()]


# --------------------------
# Tunables / defaults
# --------------------------

# Browser selection
_DEFAULT_CHANNEL = "chromium"  # critical: NOT "chrome" (avoids system Chrome)
# where to persist cookies, keeps CF session alive across calls (works on Render)
_DEFAULT_USER_DATA = _str_env("PUMP_USER_DATA_DIR", "/opt/render/.cache/pumpctx")

# Default UA (will fall back to navigator.userAgent after warm-up)
_DEFAULT_UA = _str_env(
    "PUMPFUN_UA",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# API bases
_DEFAULT_API_BASES = "https://frontend-api.pump.fun"

# Warm-up timing
_WARM_API_SEC = float(_str_env("PF_WARM_API_SEC", "8") or "8")
_WARM_SITE_SEC = float(_str_env("PF_WARM_SITE_SEC", "2") or "2")

# Retry policy
_MAX_RETRIES = _int_env("PUMP_HTTP_RETRIES", 4)
_RETRY_BASE_SLEEP = float(_str_env("PUMP_HTTP_RETRY_BASE_SEC", "1.0"))


# --------------------------
# Browser/context creation
# --------------------------

async def _new_context(pw: Playwright) -> Tuple[BrowserContext, str]:
    """
    Create a persistent Chromium context and return (ctx, resolved_user_agent).
    Honors:
      - PUMP_BROWSER_CHANNEL (default chromium)
      - PUMP_BROWSER_EXECUTABLE_PATH (optional)
      - PF_HEADLESS / PLAYWRIGHT_HEADLESS
      - CHROMIUM_FLAGS
    Performs Cloudflare warm-up on both site and API.
    """
    profile = _DEFAULT_USER_DATA
    headless = _bool_env("PLAYWRIGHT_HEADLESS", _bool_env("PF_HEADLESS", True))
    channel = _str_env("PUMP_BROWSER_CHANNEL", _DEFAULT_CHANNEL) or _DEFAULT_CHANNEL
    executable = _str_env("PUMP_BROWSER_EXECUTABLE_PATH", "").strip() or None

    # Extra Chromium args (Render-safe flags)
    chromium_args = _str_env("CHROMIUM_FLAGS", "--no-sandbox --disable-dev-shm-usage").split()

    launch_kwargs = dict(
        user_data_dir=profile,
        headless=headless,
        args=chromium_args,
    )

    # Prefer explicit executable when provided (takes precedence over channel)
    # Note: Playwright disallows mixing both; we choose one.
    if executable:
        launch_kwargs["executable_path"] = executable
    else:
        # Use "chromium" channel by default; NEVER hardcode "chrome"
        # (system Chrome path may not exist on Render)
        launch_kwargs["channel"] = channel

    # Set an initial UA; we may refine after warm-up
    launch_kwargs["user_agent"] = _DEFAULT_UA

    ctx = await pw.chromium.launch_persistent_context(**launch_kwargs)
    page = await ctx.new_page()

    # --- Cloudflare warm-up ---
    # 1) Hit pump.fun to let CF set challenge cookies
    try:
        await page.goto("https://pump.fun/", wait_until="domcontentloaded", timeout=120_000)
        await page.wait_for_timeout(int(_WARM_SITE_SEC * 1000))
        try:
            await page.goto("https://pump.fun/cdn-cgi/trace", wait_until="domcontentloaded", timeout=60_000)
        except Exception:
            pass
    except Exception:
        # non-fatal; continue
        pass

    # 2) Hit API base (first base) to pre-establish cookies at the API origin
    bases = _list_env("PUMPFUN_API_BASES", _DEFAULT_API_BASES)
    api_origin = bases[0].rstrip("/")
    try:
        await page.goto(api_origin, wait_until="domcontentloaded", timeout=120_000)
        try:
            await page.goto(api_origin + "/cdn-cgi/trace", wait_until="domcontentloaded", timeout=60_000)
        except Exception:
            pass
    except Exception:
        pass

    await page.wait_for_timeout(int(_WARM_API_SEC * 1000))

    # Resolve final UA from the live page (more realistic)
    try:
        ua = await page.evaluate("() => navigator.userAgent")
    except Exception:
        ua = _DEFAULT_UA

    return ctx, ua


# --------------------------
# Low-level POST using context.request
# --------------------------

async def _post_via_request_ctx(
    ctx: BrowserContext,
    full_url: str,
    body: dict,
    timeout_ms: int,
    user_agent: str,
) -> str:
    """
    POST via BrowserContext's request client. Reuses cookies from the context,
    avoids CORS, and looks like it originated from the warm browser session.
    """
    resp = await ctx.request.post(
        full_url,
        data=json.dumps(body),
        headers={
            "content-type": "application/json",
            "origin": "https://pump.fun",
            "referer": "https://pump.fun/",
            "accept": "*/*",
            "user-agent": user_agent,
        },
        timeout=timeout_ms,  # milliseconds
    )
    text = await resp.text()
    if resp.status == 200:
        return text

    # Useful diagnostics for CF/edge issues
    ray = resp.headers.get("cf-ray", "")
    snippet = text[:400].replace("\n", " ")
    raise RuntimeError(f"{full_url} -> {resp.status} (cf-ray={ray}): {snippet}")


# --------------------------
# Public entry point
# --------------------------

async def pump_post_via_browser(path: str, json_body: dict, timeout_ms: int = 90_000) -> str:
    """
    Robust tRPC POST helper that:
      - Creates a Chromium persistent context
      - Warms up CF on site + API
      - Tries each API base with retries/backoff
      - Closes context on exit

    Env of interest:
      PUMPFUN_API_BASES                  CSV list of bases (default https://frontend-api.pump.fun)
      PUMP_BROWSER_CHANNEL               Default "chromium"
      PUMP_BROWSER_EXECUTABLE_PATH       Override executable path
      PLAYWRIGHT_HEADLESS / PF_HEADLESS  Headless toggle
      CHROMIUM_FLAGS                     Extra chromium flags
      PUMPFUN_UA                         Override UA
      PUMP_HTTP_RETRIES                  Retry count (default 4)
      PUMP_HTTP_RETRY_BASE_SEC           Backoff base seconds (default 1.0)
    """
    if not path.startswith("/"):
        path = "/" + path

    bases = [b.rstrip("/") for b in _list_env("PUMPFUN_API_BASES", _DEFAULT_API_BASES)]
    last_err: Exception | None = None

    async with async_playwright() as pw:
        ctx: BrowserContext | None = None
        ua_resolved = _DEFAULT_UA
        try:
            ctx, ua_resolved = await _new_context(pw)

            # Try each base with per-base retries
            for base in bases:
                full = f"{base}{path}"
                for attempt in range(_MAX_RETRIES + 1):
                    try:
                        return await _post_via_request_ctx(ctx, full, json_body, timeout_ms, ua_resolved)
                    except Exception as e:
                        last_err = e
                        # Backoff for CF hiccups (530/5xx). Random jitter helps.
                        sleep_s = _RETRY_BASE_SLEEP * (2 ** attempt) * (1.0 + random.random() * 0.25)
                        # On the first failure against a base, try a quick re-warm of that origin:
                        if attempt == 0:
                            try:
                                page = await ctx.new_page()
                                await page.goto(base, wait_until="domcontentloaded", timeout=60_000)
                                await page.wait_for_timeout(int(750))
                                await page.close()
                            except Exception:
                                pass
                        await asyncio.sleep(min(sleep_s, 8.0))
        finally:
            if ctx is not None:
                try:
                    await ctx.close()
                except Exception:
                    pass

    if last_err:
        raise last_err
    raise RuntimeError("browser tRPC post failed with unknown error")
