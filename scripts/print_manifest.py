#!/usr/bin/env python3
# scripts/print_manifest.py

import os
import sys
import json
import argparse
import hashlib
import pathlib
import platform
import datetime
import re
from typing import List, Dict, Any, Iterable

ROOT = pathlib.Path(__file__).resolve().parent.parent

DEFAULT_INCLUDE_EXTS = {".py", ".toml", ".txt", ".yaml", ".yml", ".env", ".md"}
DEFAULT_SHOW = [
    "requirements.txt", "pyproject.toml", "Procfile", "render.yaml", "config.py", ".env"
]
DEFAULT_KEY_DIRS = ["utils_pkg", "daytrader", "sniper", "scanner", "scripts"]

# Project-relevant env keys (presence-only; we never print values)
ENV_KEYS = [
    # Core infra
    "WALLET_PUBKEY", "WALLET_PRIVATE_KEY", "RPC_URL", "HELIUS_RPC_URL",
    "REDIS_URL", "BIRDEYE_API_KEY",
    # Postgres
    "PG_ENABLED", "PG_DSN", "PG_DSN_INTERNAL", "PG_DSN_EXTERNAL",
    # Notion
    "NOTION_API_KEY", "NOTION_DB_TRADES",
    "NOTION_TITLE_PROP", "NOTION_PROP_TOKEN", "NOTION_PROP_ENTRY",
    "NOTION_PROP_CURRENT", "NOTION_PROP_TIME",
    # OpenAI
    "OPENAI_API_KEY", "OPENAI_MODEL", "OPENAI_BASE_URL",
    # Telegram/Discord
    "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
    "DISCORD_BOT_TOKEN", "DISCORD_SEARCH_CHANNEL_ID",
    # Social
    "ENABLE_SOCIAL_SCRAPING", "X_BEARER_TOKEN",
    # Whale logic
    "ENABLE_WHALE_FILTER", "DYNAMIC_WHALE_DISCOVERY", "CURATED_WHALE_WALLETS",
]

def sha_short(path: pathlib.Path, n: int = 12) -> str:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()[:n]
    except Exception:
        return "?"

def list_tree(root: pathlib.Path, depth: int, include_exts: set[str]) -> List[Dict[str, Any]]:
    items = []
    for dirpath, dirnames, filenames in os.walk(root):
        rel = pathlib.Path(dirpath).relative_to(root)
        if len(rel.parts) > depth:
            continue
        # Sort for stable output
        dirnames.sort()
        filenames.sort()
        files = []
        for fn in filenames:
            ext = pathlib.Path(fn).suffix.lower()
            if ext in include_exts:
                files.append(fn)
        items.append({
            "path": "." if str(rel) == "." else str(rel),
            "dirs": dirnames[:],          # shallow list (names only)
            "files": files,
        })
    return items

def key_files_hashes(root: pathlib.Path, key_dirs: Iterable[str]) -> List[Dict[str, Any]]:
    out = []
    for d in key_dirs:
        p = root / d
        if not p.exists():
            out.append({"dir": d, "status": "missing"})
            continue
        for f in sorted(p.rglob("*.py")):
            rel = f.relative_to(root)
            # skip hidden paths
            if any(part.startswith(".") for part in rel.parts):
                continue
            out.append({
                "file": str(rel),
                "sha256_12": sha_short(f),
            })
    return out

def top_packages(limit: int) -> List[Dict[str, str]]:
    rows = []
    try:
        import importlib.metadata as m  # stdlib in py3.8+
        dists = sorted(m.distributions(), key=lambda d: d.metadata["Name"].lower())
        for d in dists[:limit]:
            rows.append({"name": d.metadata["Name"], "version": d.version})
    except Exception as e:
        rows.append({"name": "unavailable", "version": f"reason: {e}"})
    return rows

def read_env_file_keys(env_path: pathlib.Path) -> List[str]:
    if not env_path.exists():
        return []
    keys = []
    try:
        for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Match KEY=VALUE, allow quoted values, ignore export prefix
            m = re.match(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=", line)
            if m:
                keys.append(m.group(1))
    except Exception:
        pass
    return keys

def env_presence_summary(keys: List[str]) -> Dict[str, Any]:
    present = []
    missing = []
    for k in keys:
        if os.getenv(k) is not None:
            present.append(k)
        else:
            missing.append(k)
    return {"present": present, "missing": missing}

def summarize_show_files(files: List[str]) -> List[Dict[str, Any]]:
    out = []
    for f in files:
        p = ROOT / f
        out.append({
            "file": f,
            "exists": p.exists(),
            "sha256_12": sha_short(p) if p.exists() and p.is_file() else None
        })
    return out

def build_report(args) -> Dict[str, Any]:
    env_keys_in_file = read_env_file_keys(ROOT / ".env")
    env_presence = env_presence_summary(ENV_KEYS)

    report = {
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "system": {
            "platform": platform.platform(),
            "python": sys.version,
            "cwd": str(ROOT),
        },
        "config": {
            "include_exts": sorted(list(args.include_exts)),
            "depth": args.depth,
            "key_dirs": args.key_dirs,
            "show": args.show,
            "top_packages": args.top
        },
        "top_level_files": summarize_show_files(args.show),
        "tree": list_tree(ROOT, args.depth, args.include_exts),
        "key_module_hashes": key_files_hashes(ROOT, args.key_dirs),
        "env": {
            "env_file_exists": (ROOT / ".env").exists(),
            "env_file_keys": env_keys_in_file,
            "project_env_presence": env_presence,  # names only, no values
        },
        "pip_freeze_top": top_packages(args.top),
    }
    return report

def print_text(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    w = lines.append
    w("=== SYSTEM ===")
    w(report["system"]["platform"])
    w(report["system"]["python"])
    w("")
    w("=== TOP-LEVEL FILES ===")
    for row in report["top_level_files"]:
        w(f"{row['file']}: {'OK' if row['exists'] else 'missing'}"
          + (f"  [{row['sha256_12']}]" if row['sha256_12'] else ""))
    w("")
    w(f"=== TREE (first {report['config']['depth']} levels) ===")
    for node in report["tree"]:
        p = node["path"]
        w(p if p != "." else ".")
        for d in node["dirs"]:
            w(f"  [D] {d}")
        for f in node["files"]:
            w(f"  [F] {f}")
    w("")
    w("=== KEY MODULE FILES (hash) ===")
    for row in report["key_module_hashes"]:
        if row.get("status") == "missing":
            w(f"{row['dir']} missing")
        else:
            w(f"{row['file']} {row['sha256_12']}")
    w("")
    w(f"=== PIP FREEZE (first {report['config']['top_packages']}) ===")
    for pkg in report["pip_freeze_top"]:
        w(f"{pkg['name']} {pkg['version']}")
    w("")
    w("=== ENV SUMMARY (names only; no values) ===")
    env = report["env"]
    w(f".env exists: {env['env_file_exists']}")
    if env["env_file_keys"]:
        w(f".env keys ({len(env['env_file_keys'])}): " + ", ".join(env["env_file_keys"]))
    pres = env["project_env_presence"]
    w(f"project keys present ({len(pres['present'])}): " + ", ".join(pres["present"]))
    w(f"project keys missing ({len(pres['missing'])}): " + ", ".join(pres["missing"]))
    return "\n".join(lines)

def ensure_parent(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)

def main():
    ap = argparse.ArgumentParser(description="Print repo manifest / health snapshot.")
    ap.add_argument("--depth", type=int, default=3, help="Directory depth to list (default: 3)")
    ap.add_argument("--include-exts", nargs="*", default=None,
                    help="File extensions to include (default: common project exts)")
    ap.add_argument("--show", nargs="*", default=None,
                    help="Top-level files to explicitly check (default: common set)")
    ap.add_argument("--key-dirs", nargs="*", default=None,
                    help="Directories whose *.py files to hash (default: utils_pkg daytrader sniper scanner scripts)")
    ap.add_argument("--top", type=int, default=20, help="How many packages to show (default: 20)")
    ap.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    ap.add_argument("--out", type=str, default=None, help="Optional output file path")
    args = ap.parse_args()

    args.include_exts = set(DEFAULT_INCLUDE_EXTS if args.include_exts is None
                            else {e if e.startswith(".") else "."+e for e in args.include_exts})
    args.show = DEFAULT_SHOW if args.show is None else args.show
    args.key_dirs = DEFAULT_KEY_DIRS if args.key_dirs is None else args.key_dirs

    report = build_report(args)

    # Prepare output
    if args.json:
        payload = json.dumps(report, indent=2)
    else:
        payload = print_text(report)

    if args.out:
        out_path = (ROOT / args.out).resolve()
        ensure_parent(out_path)
        out_path.write_text(payload, encoding="utf-8")
        print(f"✅ Wrote {out_path}")
    else:
        print(payload)

if __name__ == "__main__":
    main()
