# utils_pkg/pumpfun_headers.py
import os
from utils_pkg.pf_cookie_store import cookie_string

_DEFAULT_UA = (
    os.getenv("PUMPFUN_UA")
    or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

def pumpfun_headers() -> dict:
    ua = os.environ.get("PUMPFUN_UA", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    hdrs = {
        "accept": "*/*",
        "content-type": "application/json",
        "origin": "https://pump.fun",
        "referer": "https://pump.fun/board",
        "user-agent": ua,
    }
    ck = cookie_string() or os.environ.get("PUMPFUN_COOKIE", "")
    if ck:
        hdrs["cookie"] = ck
    return hdrs