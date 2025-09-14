# utils_pkg/pf_cookie_store.py
from __future__ import annotations
from pathlib import Path
import os
from typing import Dict
import os, pathlib, json

# Where to save/read the PF cookie string. Env var overrides.
DEFAULT_COOKIE_PATH = Path(os.environ.get("PF_COOKIE_PATH") or Path.home() / ".pf_cookies.txt")

def save_pf_cookies(cookies: Dict[str, str], path: str | None = None) -> str:
    """
    Save just the Cloudflare cookies we care about as a single Cookie header string.
    """
    p = Path(path) if path else DEFAULT_COOKIE_PATH
    wanted = ("__cf_bm", "_cfuvid", "cf_clearance")
    s = "; ".join(f"{k}={v}" for k, v in cookies.items() if k in wanted and v)
    p.write_text(s, encoding="utf-8")
    return str(p)

def load_pf_cookie_string(path: str | None = None) -> str:
    """
    Load the cookie header string ('k=v; k=v; ...'). Returns '' if missing.
    """
    p = Path(path) if path else DEFAULT_COOKIE_PATH
    return p.read_text(encoding="utf-8") if p.exists() else ""

def cookie_path() -> str:
    return str(DEFAULT_COOKIE_PATH)

def cookie_path() -> str:
    """Where we persist the Pump.fun Cloudflare cookies."""
    return os.path.join(os.path.expanduser("~"), ".pf_cookies.txt")

def save_pf_cookies(jar: Dict[str, str], path: str | None = None) -> str:
    """Write name=value; name2=value2 to disk so curl/aiohttp can reuse."""
    p = path or cookie_path()
    pathlib.Path(p).write_text("; ".join(f"{k}={v}" for k,v in jar.items()))
    return p

def load_pf_cookies(path: str | None = None) -> Dict[str, str]:
    """Return cookies as a dict (best-effort; supports file with 'name=value; ...')."""
    p = path or cookie_path()
    if not os.path.exists(p):
        return {}
    raw = pathlib.Path(p).read_text().strip()
    out: Dict[str,str] = {}
    for part in [x.strip() for x in raw.split(";") if x.strip()]:
        if "=" in part:
            k,v = part.split("=",1)
            out[k.strip()] = v
    return out

def cookie_string() -> str:
    """Return the exact Cookie header string from disk ('' if missing)."""
    p = cookie_path()
    return pathlib.Path(p).read_text().strip() if os.path.exists(p) else ""