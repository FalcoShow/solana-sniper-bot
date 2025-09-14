import os, json
from playwright.async_api import async_playwright

def _bool_env(name: str, default: bool = True) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() not in ("0", "false", "no", "")

def _str_env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return v if v is not None else default

async def _new_context(pw):
    profile  = _str_env("PF_CHROME_PROFILE", "")
    headless = _bool_env("PF_HEADLESS", True)
    warm_sec = float(_str_env("PF_WARM_API_SEC", "8") or "8")
    ua       = _str_env("PUMPFUN_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    ctx = await pw.chromium.launch_persistent_context(
        profile,
        channel="chrome",
        headless=headless,
        user_agent=ua,
    )
    page = await ctx.new_page()

    # Warm CF on API first, then site
    bases = (_str_env("PUMPFUN_API_BASES", "https://frontend-api.pump.fun")).split(",")
    api_origin = bases[0].strip().rstrip("/")
    try:
        await page.goto(api_origin, wait_until="domcontentloaded", timeout=120000)
        try:
            await page.goto(api_origin + "/cdn-cgi/trace", wait_until="domcontentloaded", timeout=60000)
        except Exception:
            pass
    except Exception:
        pass
    await page.wait_for_timeout(int(warm_sec * 1000))
    try:
        await page.goto("https://pump.fun/board", wait_until="domcontentloaded", timeout=120000)
        await page.wait_for_timeout(500)
    except Exception:
        pass
    return ctx, page

async def _post_via_request_ctx(ctx, full_url: str, body: dict, timeout_ms: int) -> str:
    # Use the BrowserContext's request client -> carries cookies; no CORS.
    resp = await ctx.request.post(
        full_url,
        data=json.dumps(body),
        headers={
            "content-type": "application/json",
            "origin": "https://pump.fun",
            "referer": "https://pump.fun/board",
            "accept": "*/*",
        },
        timeout=timeout_ms,   # milliseconds
    )
    txt = await resp.text()
    if resp.status == 200:
        return txt
    ray = resp.headers.get("cf-ray", "")
    raise RuntimeError(f"{full_url} -> {resp.status} (cf-ray={ray}): {txt[:300]}")

async def pump_post_via_browser(path: str, json_body: dict, timeout_ms: int = 90000) -> str:
    if not path.startswith("/"):
        path = "/" + path
    bases = [b.strip().rstrip("/") for b in _str_env("PUMPFUN_API_BASES", "https://frontend-api.pump.fun").split(",")]

    last_err = None
    async with async_playwright() as pw:
        ctx, _page = await _new_context(pw)
        try:
            for base in bases:
                full = f"{base}{path}"
                try:
                    return await _post_via_request_ctx(ctx, full, json_body, timeout_ms)
                except Exception as e:
                    last_err = e
        finally:
            await ctx.close()

    if last_err:
        raise last_err
    raise RuntimeError("browser tRPC post failed with unknown error")
