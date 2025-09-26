# utils_pkg/pump_client.py
import os, json, asyncio
from utils_pkg.pumpfun_headers import pumpfun_headers
from utils_pkg.proxy_client import make_session, http_post
from utils_pkg.pump_browser_client import pump_post_via_browser

API_BASES = [b.strip() for b in os.environ.get("PUMPFUN_API_BASES", "https://frontend-api.pump.fun").split(",") if b.strip()]

def _api_bases():
    bases = (os.getenv("PUMPFUN_API_BASES") or "https://frontend-api.pump.fun").split(",")
    return [b.strip() for b in bases if b.strip()]

def _join(base: str, path: str) -> str:
    if path.startswith("http"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return base.rstrip("/") + path

async def pump_get(path: str, *, timeout: int = 25) -> str:
    headers = pumpfun_headers()
    for base in _api_bases():
        url = _join(base, path)
        try:
            async with make_session() as s:
                status, body = await http_get(s, url, headers=headers, timeout=timeout)
            if status == 200:
                return body
            last = (status, body)
        except Exception as e:
            last = (-1, f"exception:{e}")
    raise RuntimeError(f"GET failed across bases. last={last[0]}:{last[1][:300]}")

async def pump_post(path: str, json_body: dict, timeout: int = 30) -> str:
    headers = pumpfun_headers()
    last = (-1, "exception:none")

    # Try direct HTTP first
    try:
        async with make_session() as s:
            for base in API_BASES:
                url = base.rstrip("/") + path
                try:
                    status, body = await http_post(s, url, headers=headers, json=json_body, timeout=timeout)
                    if status == 200:
                        return body
                    last = (status, body)
                except Exception as e:
                    last = (-1, f"exception:{type(e).__name__}:{e}")
    except Exception as e:
        last = (-1, f"exception:{type(e).__name__}:{e}")

    # Then browser capture (handles CF)
    try:
        return await pump_post_via_browser(path, json_body)
    except Exception as e:
        raise RuntimeError(f"POST failed across bases and browser fallback: {e}") from e