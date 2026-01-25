from __future__ import annotations

"""
utils_pkg/pump_browser_client.py

Pump.fun browser fallback client (Cloudflare / tRPC resilient) — V3+++
====================================================================

Purpose
-------
When direct HTTP calls to Pump.fun’s frontend API are blocked/ratelimited by CF,
this module provides a *single, persistent Playwright Chromium context* that
can execute POSTs using cookies/session state.

This is a *fallback* path (your utils_pkg/pump_client.py should try direct HTTP first).

Hard requirements
-----------------
- No network work at import time
- Safe to call concurrently (single shared browser context + semaphore)
- Persistent profile so CF cookies survive between calls
- Render-friendly defaults (headless, no-sandbox flags)
- Bounded retries + exponential backoff + jitter
- Circuit breaker on repeated 530-like failures
- Optional self-heal installer for Playwright Chromium (disabled by default)

Public API (LOCK-IN)
--------------------
- async pump_post_via_browser(path_or_url: str, json_body: dict, timeout_ms: int = 90_000) -> str
- async close_browser_client() -> None

Env contract (compatible + extended)
------------------------------------
Core:
- PUMPFUN_API_BASES             CSV list of API bases (default: https://frontend-api.pump.fun)
- PF_CHROME_PROFILE / PUMP_USER_DATA_DIR
                               persistent profile dir (default: /opt/render/project/src/.pw-profile)
- PLAYWRIGHT_HEADLESS / PF_HEADLESS
                               (default: 1 / True)
- CHROMIUM_FLAGS                (default: "--no-sandbox --disable-dev-shm-usage")
- PUMPFUN_UA                     (default: reasonable Linux Chrome UA)

Retries / breaker:
- PUMPFUN_RETRY_MAX              (default: 4)
- PUMPFUN_RETRY_BASE_MS          (default: 800)
- PUMPFUN_RETRY_JITTER           (default: 0.25)
- PUMPFUN_BREAKER_FAILS          (default: 6)
- PUMPFUN_BREAKER_COOL_MS        (default: 120000)

Warm-up:
- PF_WARM_SITE_SEC               (default: 2.0)
- PF_WARM_API_SEC                (default: 6.0)

Fallback control:
- PUMPFUN_DISABLE_PAGE_FETCH     (default: 0)  # if 1, never use page-evaluate fetch()

Autoinstall (OFF by default; prefer build-time install):
- PUMP_BROWSER_AUTOINSTALL       (default: 0)  # if 1, attempt `python -m playwright install chromium`
- PLAYWRIGHT_BROWSERS_PATH       (optional)    # where to install browsers
- PUMP_BROWSER_AUTOINSTALL_COOLDOWN_S (default: 3600)

Logging:
- PF_DEBUG / PUMP_DEBUG          (default: 0)
- PUMPFUN_LOG_ERRORS_ONLY        (default: 0)

Notes
-----
- Uses BrowserContext.request.post (cookie-aware).
- Optional last-ditch page.evaluate(fetch) path (can be disabled).
"""

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
from dataclasses import dataclass
from typing import Any

from playwright.async_api import BrowserContext, Playwright, async_playwright

# -----------------------------------------------------------------------------
# Logging (prefer canonical logging_setup)
# -----------------------------------------------------------------------------
try:
    from utils_pkg.logging_setup import get_logger as _get_logger  # preferred
except Exception:  # pragma: no cover
    try:
        from logging_setup import get_logger as _get_logger  # root shim
    except Exception:  # pragma: no cover
        _get_logger = None  # type: ignore

if _get_logger:
    try:
        LOG = _get_logger("utils.pump_browser")  # type: ignore[misc]
    except TypeError:  # pragma: no cover
        LOG = _get_logger()  # type: ignore[misc]
else:
    import logging

    LOG = logging.getLogger("utils.pump_browser")
    if not LOG.handlers:
        logging.basicConfig(level=logging.INFO)


# -----------------------------------------------------------------------------
# Env helpers
# -----------------------------------------------------------------------------
_TRUE = {"1", "true", "t", "yes", "y", "on"}
_FALSE = {"0", "false", "f", "no", "n", "off"}


def _envs(k: str, d: str = "") -> str:
    return (os.getenv(k) or d).strip()


def _envi(k: str, d: int) -> int:
    v = os.getenv(k)
    try:
        return int(float(v)) if v not in (None, "") else d
    except Exception:
        return d


def _envf(k: str, d: float) -> float:
    v = os.getenv(k)
    try:
        return float(v) if v not in (None, "") else d
    except Exception:
        return d


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


def _csv(k: str, d: str) -> list[str]:
    raw = _envs(k, d)
    return [x.strip() for x in raw.split(",") if x.strip()]


def _debug_enabled() -> bool:
    return _envb("PF_DEBUG", False) or _envb("PUMP_DEBUG", False)


def _errors_only() -> bool:
    return _envb("PUMPFUN_LOG_ERRORS_ONLY", False)


def _log_info(msg: str) -> None:
    if _errors_only():
        return
    if _debug_enabled():
        LOG.info(msg)


def _log_warn(msg: str) -> None:
    LOG.warning(msg)


def _log_err(msg: str) -> None:
    LOG.error(msg)


# -----------------------------------------------------------------------------
# Defaults / knobs
# -----------------------------------------------------------------------------
_DEFAULT_API_BASES = "https://frontend-api.pump.fun"
_DEFAULT_CHANNEL = "chromium"  # IMPORTANT: not "chrome" (requires system chrome)
_DEFAULT_PROFILE_DIR = _envs(
    "PF_CHROME_PROFILE", _envs("PUMP_USER_DATA_DIR", "/opt/render/project/src/.pw-profile")
)
_DEFAULT_UA = _envs(
    "PUMPFUN_UA",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)

# warm-up
_WARM_SITE_SEC = _envf("PF_WARM_SITE_SEC", 2.0)
_WARM_API_SEC = _envf("PF_WARM_API_SEC", 6.0)

# retries
_RETRY_MAX = max(0, _envi("PUMPFUN_RETRY_MAX", 4))
_RETRY_BASE_MS = max(50, _envi("PUMPFUN_RETRY_BASE_MS", 800))
_RETRY_JITTER = max(0.0, _envf("PUMPFUN_RETRY_JITTER", 0.25))
_MAX_SLEEP_S = 8.0

# breaker
_BREAKER_FAILS = max(1, _envi("PUMPFUN_BREAKER_FAILS", 6))
_BREAKER_COOL_MS = max(5_000, _envi("PUMPFUN_BREAKER_COOL_MS", 120_000))

# connect/launch
_LAUNCH_TIMEOUT_MS = max(10_000, _envi("PUMP_BROWSER_LAUNCH_TIMEOUT_MS", 90_000))
_NAV_TIMEOUT_MS = max(10_000, _envi("PUMP_BROWSER_NAV_TIMEOUT_MS", 120_000))

# concurrency cap (shared browser is heavy)
_BROWSER_SEMA = max(1, _envi("PUMP_BROWSER_MAX_PARALLEL", 2))

# page fallback
_DISABLE_PAGE_FETCH = _envb("PUMPFUN_DISABLE_PAGE_FETCH", False)

# autoinstall
_AUTOINSTALL = _envb("PUMP_BROWSER_AUTOINSTALL", False)
_AUTOINSTALL_COOLDOWN_S = max(60, _envi("PUMP_BROWSER_AUTOINSTALL_COOLDOWN_S", 3600))


# -----------------------------------------------------------------------------
# Self-heal helpers (Chromium install discovery)
# -----------------------------------------------------------------------------
def _candidate_browser_roots() -> list[str]:
    roots: list[str] = []
    env_root = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    if env_root:
        roots.append(env_root)

    roots.extend(
        [
            "/opt/render/project/src/.cache/ms-playwright",
            "/opt/render/.cache/ms-playwright",
            os.path.expanduser("~/.cache/ms-playwright"),
        ]
    )
    # de-dup
    seen = set()
    out: list[str] = []
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
    for root in _candidate_browser_roots():
        for pat in patterns:
            for p in glob.glob(os.path.join(root, pat)):
                if os.path.isfile(p) and os.access(p, os.X_OK):
                    return p
    return None


_LAST_AUTOINSTALL_TS: float = 0.0


def _maybe_autoinstall() -> None:
    """
    Optional: run `python -m playwright install chromium`.

    Default is OFF because best practice is to do this in build.
    If enabled, we cooldown to avoid repeated installs on crash loops.
    """
    global _LAST_AUTOINSTALL_TS
    if not _AUTOINSTALL:
        return

    now = time.time()
    if _LAST_AUTOINSTALL_TS and (now - _LAST_AUTOINSTALL_TS) < _AUTOINSTALL_COOLDOWN_S:
        return
    _LAST_AUTOINSTALL_TS = now

    target_root = (
        os.environ.get("PLAYWRIGHT_BROWSERS_PATH") or "/opt/render/project/src/.cache/ms-playwright"
    )
    env = dict(os.environ)
    env["PLAYWRIGHT_BROWSERS_PATH"] = target_root

    cmd = [sys.executable, "-m", "playwright", "install", "chromium"]
    _log_warn(
        f"pump-browser autoinstall enabled; running: {' '.join(cmd)} (PLAYWRIGHT_BROWSERS_PATH={target_root})"
    )
    subprocess.run(cmd, check=True, env=env, capture_output=True)


# -----------------------------------------------------------------------------
# Manager (singleton browser + context)
# -----------------------------------------------------------------------------
@dataclass
class _BreakerState:
    fails_530: int = 0
    open_until_ts: float = 0.0

    def allow(self) -> bool:
        return time.time() >= self.open_until_ts

    def note_530(self) -> None:
        self.fails_530 += 1
        if self.fails_530 >= _BREAKER_FAILS:
            cool_s = float(_BREAKER_COOL_MS) / 1000.0
            self.open_until_ts = time.time() + cool_s
            self.fails_530 = 0  # half-open after cooldown

    def reset(self) -> None:
        self.fails_530 = 0
        self.open_until_ts = 0.0


class _BrowserManager:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._sema = asyncio.Semaphore(_BROWSER_SEMA)
        self._pw: Playwright | None = None
        self._ctx: BrowserContext | None = None
        self._ua: str = _DEFAULT_UA
        self._breaker = _BreakerState()
        self._created_profile_dir: str | None = None

    async def close(self) -> None:
        async with self._lock:
            ctx = self._ctx
            pw = self._pw
            self._ctx = None
            self._pw = None
            self._ua = _DEFAULT_UA
            self._created_profile_dir = None
            self._breaker.reset()

        if ctx is not None:
            with contextlib.suppress(Exception):
                await ctx.close()
        if pw is not None:
            with contextlib.suppress(Exception):
                await pw.stop()

    def _compute_profile_dir(self) -> str:
        """
        Avoid ProcessSingleton profile locks by using a per-process/run subdir.
        Keeps persistence *within* a single long-running process.
        """
        base = pathlib.Path(_DEFAULT_PROFILE_DIR)
        base.mkdir(parents=True, exist_ok=True)

        # If caller provided an explicit stable dir and it's not locked, use it.
        # Otherwise create a per-process subdir under base.
        stable = base
        lock_files = ["SingletonLock", "SingletonCookie", "SingletonSocket"]
        if any((stable / lf).exists() for lf in lock_files):
            # locked => create unique run dir
            run_dir = base / f"run-{os.getpid()}-{uuid.uuid4().hex[:8]}"
            run_dir.mkdir(parents=True, exist_ok=True)
            return str(run_dir)
        return str(stable)

    async def ensure(self) -> tuple[BrowserContext, str]:
        """
        Ensure Playwright + persistent context is running.
        Returns (context, user_agent).
        """
        async with self._lock:
            if self._ctx is not None and self._pw is not None:
                return self._ctx, self._ua

            # Start Playwright
            self._pw = await async_playwright().start()

            # Resolve launch config
            headless = _envb("PLAYWRIGHT_HEADLESS", _envb("PF_HEADLESS", True))
            channel = _envs("PUMP_BROWSER_CHANNEL", _DEFAULT_CHANNEL) or _DEFAULT_CHANNEL
            explicit_exe = _envs("PUMP_BROWSER_EXECUTABLE_PATH", "").strip() or None
            chromium_args = _envs("CHROMIUM_FLAGS", "--no-sandbox --disable-dev-shm-usage").split()

            exe = explicit_exe or _find_chromium_exe()
            if exe is None:
                # optional self-heal
                try:
                    _maybe_autoinstall()
                except Exception as e:
                    _log_err(f"pump-browser autoinstall failed: {e}")
                exe = explicit_exe or _find_chromium_exe()

            profile_dir = self._compute_profile_dir()
            self._created_profile_dir = profile_dir

            launch_kwargs: dict[str, Any] = dict(
                user_data_dir=profile_dir,
                headless=headless,
                args=chromium_args,
                user_agent=_DEFAULT_UA,
                ignore_https_errors=True,
                locale="en-US",
            )
            if exe:
                launch_kwargs["executable_path"] = exe
            else:
                # let Playwright resolve via channel
                launch_kwargs["channel"] = channel

            _log_info(
                f"pump-browser launching persistent context (headless={headless}, profile={profile_dir})"
            )

            try:
                self._ctx = await self._pw.chromium.launch_persistent_context(  # type: ignore[attr-defined]
                    **launch_kwargs,
                    timeout=_LAUNCH_TIMEOUT_MS,
                )
            except Exception as e:
                # If profile locked, create a new run dir and retry once
                msg = str(e)
                if "ProcessSingleton" in msg or "SingletonLock" in msg:
                    alt = (
                        pathlib.Path(_DEFAULT_PROFILE_DIR)
                        / f"run-{os.getpid()}-{uuid.uuid4().hex[:8]}"
                    )
                    alt.mkdir(parents=True, exist_ok=True)
                    launch_kwargs["user_data_dir"] = str(alt)
                    _log_warn(f"pump-browser profile locked; retrying with {alt}")
                    self._ctx = await self._pw.chromium.launch_persistent_context(  # type: ignore[attr-defined]
                        **launch_kwargs,
                        timeout=_LAUNCH_TIMEOUT_MS,
                    )
                    self._created_profile_dir = str(alt)
                else:
                    raise

            # Warm-up
            await self._warm(self._ctx)

            # Resolve UA from actual runtime
            self._ua = await self._resolve_ua(self._ctx) or _DEFAULT_UA
            return self._ctx, self._ua

    async def _resolve_ua(self, ctx: BrowserContext) -> str | None:
        page = None
        try:
            page = await ctx.new_page()
            await page.goto(
                "https://pump.fun/", wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS
            )
            return await page.evaluate("() => navigator.userAgent")
        except Exception:
            return None
        finally:
            if page is not None:
                with contextlib.suppress(Exception):
                    await page.close()

    async def _warm(self, ctx: BrowserContext) -> None:
        """
        Warm CF cookies by visiting pump.fun and one API base.
        Best-effort only.
        """
        bases = _csv("PUMPFUN_API_BASES", _DEFAULT_API_BASES)
        api_origin = (bases[0] if bases else "https://frontend-api.pump.fun").rstrip("/")

        page = None
        try:
            page = await ctx.new_page()
            with contextlib.suppress(Exception):
                await page.goto(
                    "https://pump.fun/", wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS
                )
                await page.wait_for_timeout(int(_WARM_SITE_SEC * 1000))
            with contextlib.suppress(Exception):
                await page.goto(
                    "https://pump.fun/cdn-cgi/trace", wait_until="domcontentloaded", timeout=60_000
                )
            with contextlib.suppress(Exception):
                await page.goto(api_origin, wait_until="domcontentloaded", timeout=_NAV_TIMEOUT_MS)
                await page.wait_for_timeout(int(_WARM_API_SEC * 1000))
            with contextlib.suppress(Exception):
                await page.goto(
                    api_origin + "/cdn-cgi/trace", wait_until="domcontentloaded", timeout=60_000
                )
        finally:
            if page is not None:
                with contextlib.suppress(Exception):
                    await page.close()

    # ---- breaker helpers ----
    def breaker_allow(self) -> bool:
        return self._breaker.allow()

    def breaker_note_530(self) -> None:
        self._breaker.note_530()

    def breaker_reset(self) -> None:
        self._breaker.reset()

    # ---- concurrency guard ----
    async def __aenter_sema(self):
        await self._sema.acquire()

    def __exit_sema(self):
        with contextlib.suppress(Exception):
            self._sema.release()


_MGR = _BrowserManager()


# -----------------------------------------------------------------------------
# Low-level POST implementations
# -----------------------------------------------------------------------------
async def _post_via_context_request(
    ctx: BrowserContext,
    full_url: str,
    json_body: dict,
    timeout_ms: int,
    user_agent: str,
) -> tuple[int, str, dict[str, str]]:
    """
    POST via BrowserContext.request (cookie-aware).
    Returns (status, text, headers_lower_dict).
    """
    # BrowserContext.request exists in modern Playwright; if missing, raise clearly.
    req = getattr(ctx, "request", None)
    if req is None:
        raise RuntimeError(
            "Playwright BrowserContext.request is unavailable in this Playwright version"
        )

    resp = await req.post(
        full_url,
        data=json.dumps(json_body),
        headers={
            "content-type": "application/json",
            "accept": "*/*",
            "origin": "https://pump.fun",
            "referer": "https://pump.fun/",
            "user-agent": user_agent,
            "x-trpc-source": "web",
        },
        timeout=timeout_ms,
    )
    text = await resp.text()
    headers = {k.lower(): v for k, v in (resp.headers or {}).items()}
    return int(resp.status), text, headers


async def _post_via_page_fetch(
    ctx: BrowserContext,
    full_url: str,
    json_body: dict,
    timeout_ms: int,
) -> tuple[int, str, str]:
    """
    Last-ditch: real page + window.fetch(full_url, ...).
    Returns (status, text, cf_ray).
    """
    page = await ctx.new_page()
    try:
        # Make sure we have a first-party origin loaded
        with contextlib.suppress(Exception):
            await page.goto(
                "https://pump.fun/",
                wait_until="domcontentloaded",
                timeout=min(timeout_ms, _NAV_TIMEOUT_MS),
            )

        js = """
          async ({ url, body, timeoutMs }) => {
            const ctrl = new AbortController();
            const t = setTimeout(() => ctrl.abort(), timeoutMs);
            try {
              const res = await fetch(url, {
                method: "POST",
                headers: {
                  "content-type": "application/json",
                  "accept": "*/*",
                  "x-trpc-source": "web"
                },
                body: JSON.stringify(body),
                signal: ctrl.signal,
                credentials: "include"
              });
              const text = await res.text();
              return { status: res.status, text, cfRay: res.headers.get("cf-ray") || "" };
            } finally {
              clearTimeout(t);
            }
          }
        """
        out = await page.evaluate(js, {"url": full_url, "body": json_body, "timeoutMs": timeout_ms})
        status = int(out.get("status", 0) or 0)
        text = str(out.get("text") or "")
        cf_ray = str(out.get("cfRay") or "")
        return status, text, cf_ray
    finally:
        with contextlib.suppress(Exception):
            await page.close()


def _join(base: str, path_or_url: str) -> str:
    s = (path_or_url or "").strip()
    if s.startswith("http://") or s.startswith("https://"):
        return s
    if not s.startswith("/"):
        s = "/" + s
    return base.rstrip("/") + s


def _is_530ish(status: int, text: str) -> bool:
    if status == 530:
        return True
    t = (text or "").lower()
    return "origin dns error" in t or ("cloudflare" in t and "530" in t)


def _sleep_backoff(attempt: int) -> float:
    # attempt: 0..N
    base = _RETRY_BASE_MS * (2 ** max(0, attempt))
    jitter = 1.0 + (random.random() * _RETRY_JITTER)
    return min(_MAX_SLEEP_S, (base * jitter) / 1000.0)


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------
async def pump_post_via_browser(path_or_url: str, json_body: dict, timeout_ms: int = 90_000) -> str:
    """
    Robust CF-aware POST. Tries each base in PUMPFUN_API_BASES with retries/backoff.

    Returns: response text (str)
    Raises: RuntimeError on final failure
    """
    if not isinstance(json_body, dict):
        raise TypeError("json_body must be a dict")

    bases = _csv("PUMPFUN_API_BASES", _DEFAULT_API_BASES)
    if not bases:
        raise RuntimeError("PUMPFUN_API_BASES is empty")

    if not _MGR.breaker_allow():
        # breaker open
        raise RuntimeError("pump-browser circuit breaker open (cooldown active)")

    # serialize heavy browser work with semaphore
    await _MGR.__aenter_sema()
    try:
        ctx, ua = await _MGR.ensure()

        last_err: Exception | None = None
        last_http: tuple[int, str, str] | None = None  # status, cf-ray, snippet

        for attempt in range(_RETRY_MAX + 1):
            for base in bases:
                full_url = _join(base, path_or_url)
                try:
                    status, text, headers = await _post_via_context_request(
                        ctx, full_url, json_body, timeout_ms, ua
                    )
                    if status == 200:
                        _MGR.breaker_reset()
                        return text

                    cf_ray = headers.get("cf-ray", "")
                    snippet = (text or "")[:350].replace("\n", " ")
                    last_http = (status, cf_ray, snippet)

                    # 404 is usually "wrong path" => do not waste retries across bases
                    if status == 404:
                        raise RuntimeError(f"{full_url} -> 404 (cf-ray={cf_ray}): {snippet}")

                    # breaker accounting
                    if _is_530ish(status, text):
                        _MGR.breaker_note_530()

                    # optional page fallback for 5xx-ish
                    if (not _DISABLE_PAGE_FETCH) and (500 <= status <= 599):
                        try:
                            st2, tx2, ray2 = await _post_via_page_fetch(
                                ctx, full_url, json_body, timeout_ms
                            )
                            if st2 == 200:
                                _MGR.breaker_reset()
                                return tx2
                            snippet2 = (tx2 or "")[:350].replace("\n", " ")
                            last_http = (st2, ray2, snippet2)
                            if _is_530ish(st2, tx2):
                                _MGR.breaker_note_530()
                        except Exception as e2:
                            last_err = e2

                except Exception as e:
                    last_err = e

            # backoff between attempts
            if attempt < _RETRY_MAX:
                await asyncio.sleep(_sleep_backoff(attempt))

        # final failure
        if last_http is not None:
            st, ray, snip = last_http
            raise RuntimeError(
                f"pump-browser POST failed: last_status={st} cf-ray={ray} body={snip}"
            )
        raise RuntimeError(f"pump-browser POST failed: {last_err or 'unknown error'}")

    finally:
        _MGR.__exit_sema()


async def close_browser_client() -> None:
    """Gracefully close the shared Playwright context/browser (best-effort)."""
    await _MGR.close()


__all__ = ["close_browser_client", "pump_post_via_browser"]
