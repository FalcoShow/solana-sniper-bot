#!/usr/bin/env python3
from __future__ import annotations

"""
scripts/print_manifest.py — Repo manifest / deploy-health snapshot (P0-ready)

Goals
-----
- Deterministic, stable output (great for comparing between local vs Render)
- Zero secret leakage (ENV values never printed; only presence + key names)
- Useful “why is prod different?” signals:
    • python/runtime/platform
    • git commit + dirty flag (if available)
    • key top-level files existence + short sha
    • directory tree (bounded depth, bounded file counts)
    • key module hashes (bounded + configurable)
    • .env key list (names only)
    • env presence for project keys (names only)
    • installed packages summary (names + versions)

Usage
-----
  python scripts/print_manifest.py
  python scripts/print_manifest.py --json
  python scripts/print_manifest.py --depth 4 --max-files-per-dir 100
  python scripts/print_manifest.py --out exports/manifest.json --json
  python scripts/print_manifest.py --root . --hash-max-bytes 2000000
"""

import argparse
import contextlib
import datetime as _dt
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ──────────────────────────────────────────────────────────────
# Repo root bootstrap
# ──────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Optional .env load (local convenience only; Render uses env vars)
try:  # pragma: no cover
    from dotenv import find_dotenv, load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore
    find_dotenv = None  # type: ignore

if load_dotenv and find_dotenv:
    with contextlib.suppress(Exception):
        load_dotenv(find_dotenv(), override=False)


# ──────────────────────────────────────────────────────────────
# Defaults
# ──────────────────────────────────────────────────────────────

DEFAULT_INCLUDE_EXTS: set[str] = {
    ".py",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
    ".env",
    ".md",
    ".sh",
    ".json",
}

DEFAULT_SHOW_FILES: list[str] = [
    "requirements.txt",
    "pyproject.toml",
    "Procfile",
    "render.yaml",
    "runtime.txt",
    "src/runtime.txt",
    "config.py",
    ".env.example",
    ".env",
    "start.sh",
]

DEFAULT_KEY_DIRS: list[str] = ["utils_pkg", "scanner", "sniper", "scripts", "tests", "daytrader"]

# Exclude noisy / large / irrelevant dirs by default
DEFAULT_EXCLUDE_DIRS: set[str] = {
    ".git",
    ".github",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "venv",
    "env",
    "node_modules",
    "dist",
    "build",
    ".next",
    ".idea",
    ".vscode",
    "exports",
    "logs",
}

# Project-relevant env keys (presence-only; never print values)
ENV_KEYS: list[str] = [
    # Core infra
    "WALLET_PUBKEY",
    "WALLET_PRIVATE_KEY",
    "KEYPAIR_PATH",
    "RPC_URL",
    "HELIUS_RPC_URL",
    "REDIS_URL",
    "BIRDEYE_API_KEY",
    # Postgres
    "PG_ENABLED",
    "POSTGRES_DSN",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    # Notion
    "NOTION_TOKEN",
    "NOTION_API_KEY",
    "NOTION_DATABASE_ID",
    "NOTION_DB_TRADES",
    "NOTION_TITLE_PROP",
    "NOTION_PROP_TOKEN",
    "NOTION_PROP_ENTRY",
    "NOTION_PROP_CURRENT",
    "NOTION_PROP_TIME",
    # OpenAI
    "OPENAI_API_KEY",
    "OPENAI_MODEL",
    "OPENAI_BASE_URL",
    # Telegram/Discord
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "DISCORD_BOT_TOKEN",
    "DISCORD_SEARCH_CHANNEL_ID",
    # Social / scraping
    "ENABLE_SOCIAL_SCRAPING",
    "X_BEARER_TOKEN",
    # Whale logic
    "ENABLE_WHALE_FILTER",
    "DYNAMIC_WHALE_DISCOVERY",
    "CURATED_WHALE_WALLETS",
]


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────


def utc_now_iso() -> str:
    return _dt.datetime.now(tz=_dt.UTC).isoformat(timespec="seconds")


def safe_relpath(p: Path, root: Path) -> str:
    try:
        return str(p.resolve().relative_to(root.resolve()))
    except Exception:
        # Best effort
        try:
            return str(p.relative_to(root))
        except Exception:
            return str(p)


def sha256_short(path: Path, n: int = 12, *, max_bytes: int = 2_000_000) -> str | None:
    """
    Return sha256 hex prefix or None if unreadable.
    If file > max_bytes, returns a sha of first max_bytes + file_size suffix marker.
    """
    try:
        if not path.is_file():
            return None
        size = path.stat().st_size
        h = hashlib.sha256()
        with path.open("rb") as f:
            if max_bytes and size > max_bytes:
                h.update(f.read(max_bytes))
                # include size to reduce collision risk for truncated hashing
                h.update(str(size).encode("utf-8"))
                return h.hexdigest()[:n] + f":partial({max_bytes}/{size})"
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()[:n]
    except Exception:
        return None


def read_text_safe(path: Path, limit_bytes: int = 512_000) -> str:
    try:
        if not path.exists() or not path.is_file():
            return ""
        b = path.read_bytes()
        if limit_bytes and len(b) > limit_bytes:
            b = b[:limit_bytes]
        return b.decode("utf-8", errors="ignore")
    except Exception:
        return ""


ENV_KEY_RE = re.compile(r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=")


def read_env_file_keys(env_path: Path) -> list[str]:
    """
    Parse .env style file and return keys (names only).
    """
    if not env_path.exists() or not env_path.is_file():
        return []
    keys: list[str] = []
    try:
        for line in read_text_safe(env_path).splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            m = ENV_KEY_RE.match(s)
            if m:
                keys.append(m.group(1))
    except Exception:
        pass
    # Stable unique
    return sorted(set(keys), key=str.lower)


def env_presence(keys: Sequence[str]) -> dict[str, Any]:
    present: list[str] = []
    missing: list[str] = []
    for k in keys:
        if os.getenv(k) is not None:
            present.append(k)
        else:
            missing.append(k)
    return {"present": sorted(present), "missing": sorted(missing)}


def git_info(root: Path) -> dict[str, Any]:
    """
    Best-effort git commit + dirty. No failure if git not available.
    """

    def run(cmd: list[str]) -> tuple[int, str]:
        try:
            p = subprocess.run(
                cmd,
                cwd=str(root),
                capture_output=True,
                text=True,
                timeout=2.0,
            )
            out = (p.stdout or "").strip()
            if not out:
                out = (p.stderr or "").strip()
            return p.returncode, out
        except Exception:
            return 1, ""

    # Must be a git repo
    if not (root / ".git").exists():
        return {"is_git_repo": False}

    rc, head = run(["git", "rev-parse", "HEAD"])
    if rc != 0 or not head:
        return {"is_git_repo": True, "commit": None, "dirty": None}

    rc2, status = run(["git", "status", "--porcelain"])
    dirty = bool(status.strip()) if rc2 == 0 else None

    rc3, branch = run(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    branch = branch if rc3 == 0 and branch else None

    return {"is_git_repo": True, "commit": head, "branch": branch, "dirty": dirty}


def installed_packages(limit: int = 50) -> list[dict[str, str]]:
    """
    List installed distributions (name + version). Stable sort by name.
    """
    rows: list[dict[str, str]] = []
    try:
        import importlib.metadata as m  # py3.8+

        dists = list(m.distributions())
        items: list[tuple[str, str]] = []
        for d in dists:
            try:
                name = (d.metadata.get("Name") or "").strip()
                if not name:
                    continue
                items.append((name, d.version))
            except Exception:
                continue
        items.sort(key=lambda x: x[0].lower())
        if limit > 0:
            items = items[:limit]
        for name, ver in items:
            rows.append({"name": name, "version": ver})
    except Exception as e:
        rows.append({"name": "unavailable", "version": f"reason: {e}"})
    return rows


@dataclass(frozen=True)
class WalkConfig:
    depth: int
    include_exts: set[str]
    exclude_dirs: set[str]
    max_files_per_dir: int
    include_hidden: bool


def _should_skip_dir(dirname: str, cfg: WalkConfig) -> bool:
    if dirname in cfg.exclude_dirs:
        return True
    return bool(not cfg.include_hidden and dirname.startswith("."))


def list_tree(root: Path, cfg: WalkConfig) -> list[dict[str, Any]]:
    """
    Walk directory tree with bounded depth.
    Returns a stable, deterministic structure for diffing.
    """
    out: list[dict[str, Any]] = []
    root = root.resolve()

    for dirpath, dirnames, filenames in os.walk(root):
        p = Path(dirpath)

        # Compute depth
        try:
            rel = p.relative_to(root)
            depth_now = len(rel.parts)
        except Exception:
            rel = Path(".")
            depth_now = 0

        if depth_now > cfg.depth:
            # Prevent descending further by clearing in-place
            dirnames[:] = []
            continue

        # Filter directories in-place so os.walk won't traverse excluded dirs
        dirnames[:] = sorted([d for d in dirnames if not _should_skip_dir(d, cfg)])

        # Filter files
        files: list[str] = []
        for fn in sorted(filenames):
            if not cfg.include_hidden and fn.startswith("."):
                continue
            ext = Path(fn).suffix.lower()
            if cfg.include_exts and ext not in cfg.include_exts:
                continue
            files.append(fn)

        truncated = False
        if cfg.max_files_per_dir and len(files) > cfg.max_files_per_dir:
            files = files[: cfg.max_files_per_dir]
            truncated = True

        out.append(
            {
                "path": "." if str(rel) == "." else str(rel),
                "dirs": dirnames[:],
                "files": files,
                "files_truncated": truncated,
            }
        )

    # Stable ordering: sort nodes by path
    out.sort(key=lambda x: x["path"])
    return out


def key_module_hashes(
    root: Path,
    key_dirs: Iterable[str],
    *,
    max_bytes: int,
    include_hidden: bool,
) -> list[dict[str, Any]]:
    """
    Hash *.py files under key directories for “what changed?” comparisons.
    """
    root = root.resolve()
    out: list[dict[str, Any]] = []

    for d in key_dirs:
        base = (root / d).resolve()
        if not base.exists() or not base.is_dir():
            out.append({"dir": d, "status": "missing"})
            continue

        for f in sorted(base.rglob("*.py")):
            try:
                rel = f.relative_to(root)
            except Exception:
                rel = f

            # Skip hidden path segments unless requested
            if not include_hidden and any(part.startswith(".") for part in rel.parts):
                continue

            out.append(
                {
                    "file": str(rel),
                    "sha256_12": sha256_short(f, n=12, max_bytes=max_bytes),
                    "size_bytes": (f.stat().st_size if f.exists() else None),
                }
            )

    return out


def summarize_show_files(
    root: Path, files: Iterable[str], *, max_bytes: int
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for f in files:
        p = (root / f).resolve()
        exists = p.exists() and p.is_file()
        out.append(
            {
                "file": f,
                "exists": bool(exists),
                "sha256_12": sha256_short(p, n=12, max_bytes=max_bytes) if exists else None,
                "size_bytes": (p.stat().st_size if exists else None),
            }
        )
    return out


def manifest_fingerprint(key_hashes: list[dict[str, Any]]) -> str:
    """
    Create a single fingerprint from key file hashes for quick comparisons.
    """
    h = hashlib.sha256()
    for row in sorted(key_hashes, key=lambda r: r.get("file", r.get("dir", ""))):
        f = row.get("file") or row.get("dir") or ""
        s = row.get("sha256_12") or row.get("status") or ""
        h.update(f.encode("utf-8"))
        h.update(b"\0")
        h.update(str(s).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()[:16]


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.root).resolve()

    env_file = root / args.env_file
    env_keys_in_file = read_env_file_keys(env_file)
    env_pres = env_presence(args.env_keys)

    walk_cfg = WalkConfig(
        depth=args.depth,
        include_exts=set(args.include_exts),
        exclude_dirs=set(args.exclude_dirs),
        max_files_per_dir=args.max_files_per_dir,
        include_hidden=args.include_hidden,
    )

    top_files = summarize_show_files(root, args.show, max_bytes=args.hash_max_bytes)
    tree = list_tree(root, walk_cfg)
    key_hashes = key_module_hashes(
        root,
        args.key_dirs,
        max_bytes=args.hash_max_bytes,
        include_hidden=args.include_hidden,
    )

    report: dict[str, Any] = {
        "generated_at": utc_now_iso(),
        "root": str(root),
        "system": {
            "platform": platform.platform(),
            "python_version": sys.version.replace("\n", " "),
            "python_executable": sys.executable,
        },
        "git": git_info(root) if args.git else {"enabled": False},
        "config": {
            "depth": args.depth,
            "include_exts": sorted(set(args.include_exts)),
            "exclude_dirs": sorted(set(args.exclude_dirs)),
            "max_files_per_dir": args.max_files_per_dir,
            "include_hidden": bool(args.include_hidden),
            "show": list(args.show),
            "key_dirs": list(args.key_dirs),
            "hash_max_bytes": args.hash_max_bytes,
            "packages_limit": args.packages,
        },
        "top_level_files": top_files,
        "tree": tree,
        "key_module_hashes": key_hashes,
        "fingerprint": manifest_fingerprint(key_hashes),
        "env": {
            "env_file": safe_relpath(env_file, root),
            "env_file_exists": env_file.exists(),
            "env_file_keys": env_keys_in_file,  # names only
            "project_env_presence": env_pres,  # names only
        },
        "packages": installed_packages(args.packages),
    }

    return report


def render_text(report: dict[str, Any]) -> str:
    lines: list[str] = []
    w = lines.append

    w("=== MANIFEST ===")
    w(f"generated_at: {report.get('generated_at')}")
    w(f"root        : {report.get('root')}")
    w("")

    sysinfo = report.get("system", {}) or {}
    w("=== SYSTEM ===")
    w(f"platform    : {sysinfo.get('platform')}")
    w(f"python      : {sysinfo.get('python_version')}")
    w(f"executable  : {sysinfo.get('python_executable')}")
    w("")

    git = report.get("git", {}) or {}
    w("=== GIT ===")
    if not git or git.get("enabled") is False:
        w("git enabled : no")
    else:
        w(f"is_repo     : {git.get('is_git_repo')}")
        w(f"branch      : {git.get('branch')}")
        w(f"commit      : {git.get('commit')}")
        w(f"dirty       : {git.get('dirty')}")
    w("")

    w("=== TOP-LEVEL FILES (exist + sha) ===")
    for row in report.get("top_level_files", []) or []:
        status = "OK" if row.get("exists") else "missing"
        sha = row.get("sha256_12")
        extra = f" [{sha}]" if sha else ""
        w(f"{row.get('file')}: {status}{extra}")
    w("")

    cfg = report.get("config", {}) or {}
    w(f"=== TREE (depth={cfg.get('depth')}, max_files_per_dir={cfg.get('max_files_per_dir')}) ===")
    for node in report.get("tree", []) or []:
        p = node.get("path", ".")
        w(p if p != "." else ".")
        for d in node.get("dirs", []) or []:
            w(f"  [D] {d}")
        for f in node.get("files", []) or []:
            w(f"  [F] {f}")
        if node.get("files_truncated"):
            w("  … (files truncated)")
    w("")

    w("=== KEY MODULE HASHES ===")
    for row in report.get("key_module_hashes", []) or []:
        if row.get("status") == "missing":
            w(f"{row.get('dir')} : missing")
        else:
            w(f"{row.get('file')} {row.get('sha256_12')}")
    w("")
    w(f"fingerprint: {report.get('fingerprint')}")
    w("")

    env = report.get("env", {}) or {}
    pres = env.get("project_env_presence", {}) or {}
    w("=== ENV SUMMARY (names only; no values) ===")
    w(f"{env.get('env_file')} exists: {env.get('env_file_exists')}")
    keys = env.get("env_file_keys", []) or []
    if keys:
        w(f".env keys ({len(keys)}): " + ", ".join(keys))
    w(
        f"project keys present ({len(pres.get('present', []) or [])}): "
        + ", ".join(pres.get("present", []) or [])
    )
    w(
        f"project keys missing ({len(pres.get('missing', []) or [])}): "
        + ", ".join(pres.get("missing", []) or [])
    )
    w("")

    pkgs = report.get("packages", []) or []
    w(f"=== PACKAGES (first {cfg.get('packages_limit')}) ===")
    for p in pkgs:
        w(f"{p.get('name')} {p.get('version')}")
    w("")

    return "\n".join(lines)


def atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    tmp.replace(path)


def main() -> None:
    ap = argparse.ArgumentParser(description="Print repo manifest / deploy-health snapshot (P0).")

    ap.add_argument(
        "--root", type=str, default=str(REPO_ROOT), help="Repo root (default: auto-detected)."
    )
    ap.add_argument("--depth", type=int, default=3, help="Directory depth to list (default: 3).")
    ap.add_argument(
        "--max-files-per-dir",
        type=int,
        default=200,
        help="Limit file names per dir (default: 200).",
    )
    ap.add_argument(
        "--include-hidden", action="store_true", help="Include hidden files/dirs (default: no)."
    )

    ap.add_argument(
        "--include-exts",
        nargs="*",
        default=None,
        help="File extensions to include in tree (default: common project exts).",
    )
    ap.add_argument(
        "--exclude-dirs",
        nargs="*",
        default=None,
        help="Directory names to exclude (default: common noise dirs).",
    )

    ap.add_argument(
        "--show",
        nargs="*",
        default=None,
        help="Top-level files to explicitly check (default: common set).",
    )
    ap.add_argument(
        "--key-dirs",
        nargs="*",
        default=None,
        help="Dirs whose *.py files to hash (default: utils_pkg scanner sniper scripts tests daytrader).",
    )
    ap.add_argument(
        "--hash-max-bytes",
        type=int,
        default=2_000_000,
        help="If file larger than this, hash partial + size marker (default: 2,000,000).",
    )

    ap.add_argument(
        "--env-file", type=str, default=".env", help="Env file to parse keys from (default: .env)."
    )
    ap.add_argument(
        "--env-keys",
        nargs="*",
        default=None,
        help="Env keys to check presence for (default: built-in project list).",
    )

    ap.add_argument(
        "--packages", type=int, default=30, help="How many packages to list (default: 30)."
    )
    ap.add_argument(
        "--git", action="store_true", help="Include git commit/dirty info (default: off)."
    )

    ap.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    ap.add_argument(
        "--out",
        type=str,
        default=None,
        help="Optional output file path (relative to root unless absolute).",
    )

    args = ap.parse_args()

    # Normalize include exts
    if args.include_exts is None:
        args.include_exts = sorted(DEFAULT_INCLUDE_EXTS)
    else:
        exts: set[str] = set()
        for e in args.include_exts:
            if not e:
                continue
            e = e.strip()
            if not e:
                continue
            exts.add(e if e.startswith(".") else "." + e)
        args.include_exts = sorted(exts)

    # Normalize excluded dirs
    if args.exclude_dirs is None:
        args.exclude_dirs = sorted(DEFAULT_EXCLUDE_DIRS)
    else:
        args.exclude_dirs = sorted({d.strip() for d in args.exclude_dirs if d and d.strip()})

    # Normalize show files and key dirs
    args.show = DEFAULT_SHOW_FILES if args.show is None else args.show
    args.key_dirs = DEFAULT_KEY_DIRS if args.key_dirs is None else args.key_dirs

    # Env keys to check presence
    args.env_keys = ENV_KEYS if args.env_keys is None else args.env_keys

    report = build_report(args)

    payload = json.dumps(report, indent=2, default=str) if args.json else render_text(report)

    if args.out:
        root = Path(args.root).resolve()
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = (root / out_path).resolve()
        atomic_write(out_path, payload)
        print(f"✅ Wrote {out_path}")
    else:
        print(payload)


if __name__ == "__main__":
    main()
