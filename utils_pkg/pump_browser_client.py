# utils_pkg/pump_browser_client.py
# Robust Pump.fun browser client for CF-protected tRPC POSTs.
# - Defaults to Playwright "chromium" (no system Chrome required)
# - Optional executable override via PUMP_BROWSER_EXECUTABLE_PATH
# - Proper warm-up (visit site + /cdn-cgi/trace) before API POST
# - Reuses persistent context to keep CF cookies
# - Retries with backoff + jitter
# - Circuit breaker on repeated 530s (CF Origin DNS error)
# - Honors common Render/env knobs: PLAYWRIGHT_*, CHROMIUM_FLAGS, PF_*, PUMPFUN_*

from __future__ import annotations

import asyncio
import json
import os
import random
from typing import Tuple

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

def _float_env(name: str, default: float) -> float:
    v = os.environ.get(name)
    try:
        return float(v) if v is not None and str(v).strip() != "" else default
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
# persistent profile keeps CF cookies/session alive across calls
_DEFAULT_USER_DATA = _str_env("PF_CHROME_PROFILE", _str_env("PUMP_USER_DATA_DIR", "/opt/render/project/src/.pw-profile"))

# Default UA (we’ll try to read navigator.userAgent after warm-up)
_DEFAULT_UA = _str_env(
    "PUMPFUN_UA",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# API bases
_DEFAULT_API_BASES = "https://frontend-api.pump.fun"

# Warm-up timing
_WARM_API_SEC  = _float_env("PF_WARM_API_SEC", 8.0)
_WARM_SITE_SEC = _float_env("PF_WARM_SITE_SEC", 2.0)

# Retry + circuit breaker policy (can be overridden by env)
_RETRY_MAX           = _int_env("PUMPFUN_RETRY_MAX", 4)
_RETRY_BASE_MS       = _int_env("PUMPFUN_RETRY_BASE_MS", 800)
_BREAKER_FAILS       = _int_env("PUMPFUN_BREAKER_FAILS", 6)          # trip after N consecutive 530s
_BREAKER_COOL_MS     = _int_env("PUMPFUN_BREAKER_COOL_MS", 120_000)  # 2 minutes cooldown (open state)

# Hard caps
_MAX_SLEEP_S = 8.0


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
      - PF_CHROME_PROFILE / PUMP_USER_DATA_DIR (profile dir)
    Performs Cloudflare warm-up on both site and API.
    """
    profile    = _DEFAULT_USER_DATA
    headless   = _bool_env("PLAYWRIGHT_HEADLESS", _bool_env("PF_HEADLESS", True))
    channel    = _str_env("PUMP_BROWSER_CHANNEL", _DEFAULT_CHANNEL) or _DEFAULT_CHANNEL
    executable = _str_env("PUMP_BROWSER_EXECUTABLE_PATH", "").strip() or None

    # Extra Chromium args (Render-safe flags)
    chromium_args = _str_env("CHROMIUM_FLAGS", "--no-sandbox --disable-dev-shm-usage").split()

    launch_kwargs = dict(
        user_data_dir=profile,
        headless=headless,
        args=chromium_args,
        user_agent=_DEFAULT_UA,  # initial UA; we’ll resolve to navigator.userAgent after warm-up
    )

    # Prefer explicit executable when provided; otherwise use channel (chromium)
    if executable:
        launch_kwargs["executable_path"] = executable
    else:
        launch_kwargs["channel"] = channel

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
        # non-fatal
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

    try:
        await page.close()
    except Exception:
        pass

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
    Raises RuntimeError with rich message on non-200 (includes cf-ray and status).
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
      - Tries each API base with retries/backoff (+ jitter)
      - Circuit-breaks on repeated 530s
      - Closes context on exit

    Relevant env:
      PUMPFUN_API_BASES            CSV list of bases (default https://frontend-api.pump.fun)
      PUMP_BROWSER_CHANNEL         Default "chromium"
      PUMP_BROWSER_EXECUTABLE_PATH Override executable path
      PLAYWRIGHT_HEADLESS/PF_HEADLESS
      CHROMIUM_FLAGS
      PUMPFUN_UA
      PUMPFUN_RETRY_MAX            (default 4)
      PUMPFUN_RETRY_BASE_MS        (default 800)
      PUMPFUN_BREAKER_FAILS        (default 6)
      PUMPFUN_BREAKER_COOL_MS      (default 120000)
      PF_WARM_API_SEC              (default 8)
      PF_WARM_SITE_SEC             (default 2)
    """
    if not path.startswith("/"):
        path = "/" + path

    bases = [b.rstrip("/") for b in _list_env("PUMPFUN_API_BASES", _DEFAULT_API_BASES)]
    if not bases:
        raise RuntimeError("No PUMPFUN_API_BASES configured")

    last_err: Exception | None = None
    consecutive_530 = 0

    async with async_playwright() as pw:
        ctx: BrowserContext | None = None
        ua_resolved = _DEFAULT_UA
        try:
            ctx, ua_resolved = await _new_context(pw)

            # Try bases with retries
            for attempt in range(_RETRY_MAX + 1):
                for base in bases:
                    full = f"{base}{path}"
                    try:
                        txt = await _post_via_request_ctx(ctx, full, json_body, timeout_ms, ua_resolved)
                        consecutive_530 = 0
                        return txt
                    except RuntimeError as e:
                        msg = str(e)
                        last_err = e

                        # Fast fail on 404 (wrong path/host)
                        if "-> 404" in msg:
                            raise

                        # Track 530s for breaker logic
                        if "-> 530" in msg or "Origin DNS error" in msg:
                            consecutive_530 += 1

                        # Quick re-warm this base after first failure
                        if attempt == 0:
                            try:
                                pg = await ctx.new_page()
                                await pg.goto(base, wait_until="domcontentloaded", timeout=60_000)
                                await pg.wait_for_timeout(600)
                                await pg.close()
                            except Exception:
                                pass

                # exponential backoff with jitter between attempts
                sleep_ms = int(_RETRY_BASE_MS * (2 ** attempt) * (1.0 + random.random() * 0.25))
                await asyncio.sleep(min(float(sleep_ms) / 1000.0, _MAX_SLEEP_S))

                # circuit breaker: too many 530s in a row -> cool down
                if consecutive_530 >= _BREAKER_FAILS:
                    cool_ms = max(_BREAKER_COOL_MS, _RETRY_BASE_MS * 4)
                    # simple log line (stderr captured by your logger)
                    print(f"[pump] circuit-breaker open after {consecutive_530}x 530; cooling {cool_ms}ms", flush=True)
                    await asyncio.sleep(float(cool_ms) / 1000.0)
                    consecutive_530 = 0  # half-open afterwards
        finally:
            if ctx is not None:
                try:
                    await ctx.close()
                except Exception:
                    pass

    if last_err:
        raise last_err
    raise RuntimeError("browser tRPC post failed with unknown error")
