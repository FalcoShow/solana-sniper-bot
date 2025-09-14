# utils_pkg/proxy_client.py
import os
import aiohttp

def _proxy_url():
    if os.getenv("PF_NO_PROXY", "") == "1":
        return None
    hp = os.getenv("PROXY_HOSTPORT")
    if not hp:
        return None
    user, pw = os.getenv("PROXY_USER"), os.getenv("PROXY_PASS")
    if user and pw:
        return f"https://{user}:{pw}@{hp}"
    return f"https://{hp}"

def make_session():
    # Return a ClientSession (used with "async with make_session() as s")
    return aiohttp.ClientSession()

async def http_get(session: aiohttp.ClientSession, url: str, *, headers=None, timeout=25):
    kw = {}
    proxy = _proxy_url()
    if proxy:
        kw["proxy"] = proxy
    async with session.get(url, headers=headers, timeout=timeout, **kw) as r:
        return r.status, await r.text()

async def http_post(session: aiohttp.ClientSession, url: str, *, headers=None, json=None, timeout=25):
    kw = {}
    proxy = _proxy_url()
    if proxy:
        kw["proxy"] = proxy
    async with session.post(url, headers=headers, json=json, timeout=timeout, **kw) as r:
        return r.status, await r.text()
