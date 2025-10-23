# utils_pkg/pump_browser_client.py
# Robust Pump.fun browser client for CF-protected tRPC POSTs.
# - Defaults to Playwright "chromium" (no system Chrome required)
# - Optional executable override via PUMP_BROWSER_EXECUTABLE_PATH
# - Proper warm-up (visit site + /cdn-cgi/trace) before API POST
# - Reuses persistent context to keep CF cookies
# - Retries with backoff + jitter
# - Circuit breaker on repeated 530s (CF Origin DNS error)
# - Optional page-based fetch() fallback
# - Self-heals: auto-runs `playwright install chromium` if missing
# - Honors common Render/env knobs: PLAYWRIGHT_*, CHROMIUM_FLAGS, PF_*, PUMPFUN_*

from __future__ import annotations

import asyncio
import contextlib
import glob
import json
import os
import pathlib
import random
import subprocess
import sys
import time
import uuid
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

_DEFAULT_CHANNEL = "chromium"  # important: NOT "chrome"
_DEFAULT_USER_DATA = _str_env(
    "PF_CHROME_PROFILE",
    _str_env("PUMP_USER_DATA_DIR", "/opt/render/project/src/.pw-profile"),
)

_DEFAULT_UA = _str_env(
    "PUMPFUN_UA",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_DEFAULT_API_BASES = "https://frontend-api.pump.fun"

_WARM_API_SEC  = _float_env("PF_WARM_API_SEC", 8.0)
_WARM_SITE_SEC = _float_env("PF_WARM_SITE_SEC", 2.0)

_RETRY_MAX       = _int_env("PUMPFUN_RETRY_MAX", 4)
_RETRY_BASE_MS   = _int_env("PUMPFUN_RETRY_BASE_MS", 800)
_BREAKER_FAILS   = _int_env("PUMPFUN_BREAKER_FAILS", 6)
_BREAKER_COOL_MS = _int_env("PUMPFUN_BREAKER_COOL_MS", 120_000)

_MAX_SLEEP_S = 8.0

def _log_enabled() -> bool:
    return _bool_env("PF_DEBUG", False) or _bool_env("PUMP_DEBUG", False)

def _errors_only() -> bool:
    return _bool_env("PUMPFUN_LOG_ERRORS_ONLY", False)

def _log(msg: str, *, error: bool = False) -> None:
    if _errors_only() and not error:
        return
    if _log_enabled() or error:
        print(f"[pump-browser] {msg}", flush=True)


# --------------------------
# Self-heal: ensure Chromium exists
# --------------------------

def _candidate_browsers_roots() -> list[str]:
    roots: list[str] = []
    env_root = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if env_root:
        roots.append(env_root)
    roots.extend([
        "/opt/render/project/src/.cache/ms-playwright",
        "/opt/render/.cache/ms-playwright",
        os.path.expanduser("~/.cache/ms-playwright"),
    ])
    seen, out = set(), []
    for r in roots:
        if r and r not in seen:
            seen.add(r)
            out.append(r)
    return out

def _find_chromium_exe() -> str | None:
    patterns = [
        "chromium-*/chrome-linux/chrome",
        "chromium-*/chromium-linux/chrome",
        "chromium-*/chrome-linux/headless_shell",
    ]
    for root in _candidate_browsers_roots():
        for pat in patterns:
            for p in glob.glob(os.path.join(root, pat)):
                if os.path.isfile(p) and os.access(p, os.X_OK):
                    return p
    return None

def _autoinstall_browsers() -> None:
    """
    Run `python -m playwright install chromium` into a persistent app-local cache.
    Safe to call multiple times; Playwright will no-op if already installed.
    """
    target_root = "/opt/render/project/src/.cache/ms-playwright"
    env = dict(os.environ)
    env.setdefault("PLAYWRIGHT_BROWSERS_PATH", target_root)
    cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
    _log(f"Installing Chromium via: {' '.join(cmd)} (PLAYWRIGHT_BROWSERS_PATH={env.get('PLAYWRIGHT_BROWSERS_PATH')})")
    try:
        subprocess.run(cmd, check=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
        raise RuntimeError(f"playwright autoinstall failed: {e}") from e


# --------------------------
# Browser/context creation
# --------------------------

async def _new_context(pw: Playwright) -> Tuple[BrowserContext, str]:
    """
    Create a persistent Chromium context and return (ctx, resolved_user_agent).
    Falls back to a fresh subdir if the configured profile is locked.
    Also self-heals by auto-installing Chromium if the binary is missing.
    """
    base_profile = pathlib.Path(_DEFAULT_USER_DATA)
    headless = _bool_env("PLAYWRIGHT_HEADLESS", _bool_env("PF_HEADLESS", True))
    channel = _str_env("PUMP_BROWSER_CHANNEL", _DEFAULT_CHANNEL) or _DEFAULT_CHANNEL
    explicit_exe = _str_env("PUMP_BROWSER_EXECUTABLE_PATH", "").strip() or None
    chromium_args = _str_env("CHROMIUM_FLAGS", "--no-sandbox --disable-dev-shm-usage").split()

    def launch_kwargs_for(user_data_dir: pathlib.Path, prefer_exe: bool = True) -> dict:
        kw = dict(
            user_data_dir=str(user_data_dir),
            headless=headless,
            args=chromium_args,
            user_agent=_DEFAULT_UA,
        )
        exe = explicit_exe or _find_chromium_exe()
        if prefer_exe and exe:
            kw["executable_path"] = exe
        else:
            kw["channel"] = channel
        return kw

    def profile_locked(p: pathlib.Path) -> bool:
        return any((p / name).exists() for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"))

    profile_to_use = base_profile
    base_profile.mkdir(parents=True, exist_ok=True)

    if profile_locked(base_profile):
        profile_to_use = base_profile / f"run-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        profile_to_use.mkdir(parents=True, exist_ok=True)
        _log(f"profile locked; using temp profile: {profile_to_use}")

    tried_autoinstall = False
    while True:
        try:
            ctx = await pw.chromium.launch_persistent_context(**launch_kwargs_for(profile_to_use, prefer_exe=True))
        except Exception as e:
            msg = str(e)
            # Locked profile -> create a new temp profile once
            if ("ProcessSingleton" in msg or "SingletonLock" in msg):
                alt = base_profile / f"run-{int(time.time())}-{uuid.uuid4().hex[:8]}"
                alt.mkdir(parents=True, exist_ok=True)
                _log(f"ProcessSingleton again; retrying with new profile: {alt}")
                ctx = await pw.chromium.launch_persistent_context(**launch_kwargs_for(alt, prefer_exe=True))
            # Executable missing -> autoinstall once then retry
            elif ("Executable doesn't exist" in msg or "executable_path" in msg) and not tried_autoinstall:
                _log("Chromium executable missing – running autoinstall…")
                _autoinstall_browsers()
                tried_autoinstall = True
                continue
            else:
                # Last resort: let Playwright resolve via channel
                if "executable_path" in msg:
                    _log("Retrying launch with channel=chromium resolution…")
                    try:
                        ctx = await pw.chromium.launch_persistent_context(**launch_kwargs_for(profile_to_use, prefer_exe=False))
                    except Exception:
                        raise
                else:
                    raise
        break  # success

    page = await ctx.new_page()

    # --- Warm-up: main site first
    try:
        await page.goto("https://pump.fun/", wait_until="domcontentloaded", timeout=120_000)
        await page.wait_for_timeout(int(_WARM_SITE_SEC * 1000))
        with contextlib.suppress(Exception):
            await page.goto("https://pump.fun/cdn-cgi/trace", wait_until="domcontentloaded", timeout=60_000)
    except Exception:
        pass

    # --- Then API base (first configured base)
    bases = _list_env("PUMPFUN_API_BASES", _DEFAULT_API_BASES)
    api_origin = bases[0].rstrip("/") if bases else "https://frontend-api.pump.fun"
    try:
        await page.goto(api_origin, wait_until="domcontentloaded", timeout=120_000)
        with contextlib.suppress(Exception):
            await page.goto(api_origin + "/cdn-cgi/trace", wait_until="domcontentloaded", timeout=60_000)
    except Exception:
        pass

    await page.wait_for_timeout(int(_WARM_API_SEC * 1000))

    try:
        ua = await page.evaluate("() => navigator.userAgent")
    except Exception:
        ua = _DEFAULT_UA

    with contextlib.suppress(Exception):
        await page.close()

    return ctx, ua


# --------------------------
# Low-level POSTs
# --------------------------

async def _post_via_request_ctx(
    ctx: BrowserContext,
    full_url: str,
    body: dict,
    timeout_ms: int,
    user_agent: str,
    hide_snippet: bool = False,
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
            "x-trpc-source": "web",
        },
        timeout=timeout_ms,  # milliseconds
    )
    text = await resp.text()
    if resp.status == 200:
        return text

    ray = resp.headers.get("cf-ray", "")
    snippet = "" if hide_snippet else text[:400].replace("\n", " ")
    raise RuntimeError(f"{full_url} -> {resp.status} (cf-ray={ray}): {snippet}")


async def _post_via_page_fetch(
    ctx: BrowserContext,
    full_url: str,
    json_body: dict,
    timeout_ms: int,
    ua: str,  # interface symmetry; browser UA comes from context
) -> str:
    """
    Use an actual page and in-page fetch() to POST from the site origin.
    NOTE: /api/trpc on main origin (pump.fun) responds 404; tests may look for
    that path to confirm the fallback executed.
    """
    from urllib.parse import urlparse, urljoin
    parsed = urlparse(full_url)
    site_origin = "https://pump.fun"
    rel_path = f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path

    page = await ctx.new_page()
    try:
        await page.goto(site_origin, wait_until="domcontentloaded", timeout=min(timeout_ms, 120_000))

        js = """
          async (input) => {
            const { path, body, timeoutMs } = input;
            const ctrl = new AbortController();
            const t = setTimeout(() => ctrl.abort(), timeoutMs);
            try {
              const res = await fetch(path, {
                method: "POST",
                headers: {
                  "content-type": "application/json",
                  "accept": "*/*",
                  "x-trpc-source": "web"
                },
                body: JSON.stringify(body),
                signal: ctrl.signal,
                credentials: "include",
              });
              const text = await res.text();
              return { status: res.status, text, cfRay: res.headers.get("cf-ray") || "" };
            } finally {
              clearTimeout(t);
            }
          }
        """
        payload = {"path": rel_path, "body": json_body, "timeoutMs": timeout_ms}
        out = await page.evaluate(js, payload)
    finally:
        with contextlib.suppress(Exception):
            await page.close()

    status = int(out.get("status", 0) or 0)
    if 200 <= status < 300:
        return out["text"]

    cf_ray = out.get("cfRay", "")
    snippet = (out.get("text") or "")[:400].replace("\n", " ")
    return_url = urljoin(site_origin, rel_path)
    raise RuntimeError(f"{return_url} -> {status} (cf-ray={cf_ray}): {snippet}")


# --------------------------
# Public entry point
# --------------------------

async def pump_post_via_browser(path: str, json_body: dict, timeout_ms: int = 90_000) -> str:
    """
    Robust tRPC POST helper that:
      - Creates a Chromium persistent context
      - Warms up CF on site + API
      - Tries each API base with retries/backoff (+ jitter)
      - (Optionally) falls back to page-based fetch()
      - Circuit-breaks on repeated 530s
      - Closes context on exit

    Relevant env:
      PUMPFUN_API_BASES            CSV list (default https://frontend-api.pump.fun)
      PUMP_BROWSER_CHANNEL         Default "chromium"
      PUMP_BROWSER_EXECUTABLE_PATH Override executable path
      PLAYWRIGHT_HEADLESS / PF_HEADLESS
      CHROMIUM_FLAGS
      PUMPFUN_UA
      PUMPFUN_RETRY_MAX            (default 4)
      PUMPFUN_RETRY_BASE_MS        (default 800)
      PUMPFUN_BREAKER_FAILS        (default 6)
      PUMPFUN_BREAKER_COOL_MS      (default 120000)
      PF_WARM_API_SEC              (default 8)
      PF_WARM_SITE_SEC             (default 2)
      PUMPFUN_DISABLE_PAGE_FETCH   (default 0)
      PUMPFUN_LOG_ERRORS_ONLY      (default 0)
    """
    if not path.startswith("/"):
        path = "/" + path

    bases = [b.rstrip("/") for b in _list_env("PUMPFUN_API_BASES", _DEFAULT_API_BASES)]
    if not bases:
        raise RuntimeError("No PUMPFUN_API_BASES configured")

    # flags read at *call time* so test env flips take effect
    disable_page_fetch = _bool_env("PUMPFUN_DISABLE_PAGE_FETCH", False)
    _log(f"page-fallback disabled={disable_page_fetch}")

    last_err: Exception | None = None
    last_http_err: Exception | None = None      # prefer this at exit
    last_transport_err: Exception | None = None # e.g., ENOTFOUND
    consecutive_530 = 0

    async with async_playwright() as pw:
        ctx: BrowserContext | None = None
        ua_resolved = _DEFAULT_UA
        try:
            ctx, ua_resolved = await _new_context(pw)

            for attempt in range(_RETRY_MAX + 1):
                for base in bases:
                    full = f"{base}{path}"
                    try:
                        # Primary route: request-context POST
                        txt = await _post_via_request_ctx(
                            ctx, full, json_body, timeout_ms, ua_resolved,
                            hide_snippet=disable_page_fetch  # keep error text clean for step assertions
                        )
                        consecutive_530 = 0
                        return txt

                    except Exception as e:
                        msg = str(e)
                        last_err = e
                        _log(f"request_ctx failed: {msg[:180]}", error=True)

                        got_404 = "-> 404" in msg
                        has_530 = ("-> 530" in msg) or ("Origin DNS error" in msg)

                        # Classify for exit preference
                        if ("-> " in msg) and any(f"-> {code}" in msg for code in ("4", "5")):
                            last_http_err = e

                        # Wrong path/host: hard stop
                        if got_404:
                            raise

                        # DNS/mirror not resolvable: skip this base without counting toward breaker
                        if ("ENOTFOUND" in msg) or ("getaddrinfo" in msg):
                            _log("mirror DNS error; skipping this base")
                            last_transport_err = e
                            continue

                        # Page-fetch fallback only for 5xx-ish problems and only if enabled
                        if (not disable_page_fetch) and (has_530 or "-> 5" in msg):
                            try:
                                _log("trying page-fetch fallback…")
                                txt = await _post_via_page_fetch(ctx, full, json_body, timeout_ms, ua_resolved)
                                consecutive_530 = 0
                                return txt
                            except Exception as e2:
                                last_err = e2
                                last_http_err = e2
                                _log(f"page-fetch fallback failed: {str(e2)[:180]}", error=True)
                                if has_530:
                                    consecutive_530 += 1

                        # Quick re-warm the base after the first failure sweep
                        if attempt == 0:
                            with contextlib.suppress(Exception):
                                pg = await ctx.new_page()
                                await pg.goto(base, wait_until="domcontentloaded", timeout=60_000)
                                await pg.wait_for_timeout(600)
                                await pg.close()

                # Exponential backoff (capped)
                sleep_ms = int(_RETRY_BASE_MS * (2 ** attempt) * (1.0 + random.random() * 0.25))
                await asyncio.sleep(min(float(sleep_ms) / 1000.0, _MAX_SLEEP_S))

                # Circuit breaker cool-down on persistent 530s
                if consecutive_530 >= _BREAKER_FAILS:
                    cool_ms = max(_BREAKER_COOL_MS, _RETRY_BASE_MS * 4)
                    print(f"[pump] circuit-breaker open after {consecutive_530}x 530; cooling {cool_ms}ms", flush=True)
                    await asyncio.sleep(float(cool_ms) / 1000.0)
                    consecutive_530 = 0  # half-open

        finally:
            if ctx is not None:
                with contextlib.suppress(Exception):
                    await ctx.close()

    # Prefer HTTP-flavored errors (status-bearing) over transport noise at exit
    if last_http_err:
        raise last_http_err
    if last_err:
        raise last_err
    raise RuntimeError("browser tRPC post failed with unknown error")
