# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/sync_env_example.py

One-time helper to keep .env.example complete:
- Scans repo .py files for os.getenv / os.environ.* usage
- Ensures every referenced ENV KEY exists in .env.example
- Appends any missing keys to an auto-added section at the end
- Creates a backup .env.example.bak before writing

Usage:
  py scripts\\sync_env_example.py --write
  py scripts\\sync_env_example.py          (dry run)
"""

import argparse
import re
from pathlib import Path
from typing import Set, Tuple

ENV_EXAMPLE_NAME = ".env.example"
BACKUP_NAME = ".env.example.bak"

ENV_KEY_RE = re.compile(r"^\s*([A-Z][A-Z0-9_]*)\s*=", re.M)

# Patterns for env usage
PATTERNS = [
    re.compile(r'os\.getenv\(\s*["\']([A-Z][A-Z0-9_]*)["\']'),
    re.compile(r'os\.environ\.get\(\s*["\']([A-Z][A-Z0-9_]*)["\']'),
    re.compile(r'os\.environ\.setdefault\(\s*["\']([A-Z][A-Z0-9_]*)["\']'),
    re.compile(r'os\.environ\.pop\(\s*["\']([A-Z][A-Z0-9_]*)["\']'),
    re.compile(r'os\.environ\[\s*["\']([A-Z][A-Z0-9_]*)["\']\s*\]'),
]

# Files/dirs to skip
SKIP_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    "node_modules",
    "dist",
    "build",
}

# Platform/external env keys that we do NOT force into .env.example
EXTERNAL_KEYS = {
    "PATH",
    "PYTHONPATH",
    "HOME",
    "USER",
    "USERNAME",
    "SHELL",
    "TERM",
    "PORT",
    "HOSTNAME",  # optional; if you want it in .env.example, remove this
}
EXTERNAL_PREFIXES: Tuple[str, ...] = ("RENDER_", "GITHUB_", "RUNNER_", "CI")


def repo_root() -> Path:
    # scripts/ is under repo root
    return Path(__file__).resolve().parent.parent


def is_external(key: str) -> bool:
    if key in EXTERNAL_KEYS:
        return True
    return any(key.startswith(p) for p in EXTERNAL_PREFIXES)


def parse_env_example_keys(text: str) -> Set[str]:
    return set(ENV_KEY_RE.findall(text))


def scan_env_keys_in_py(root: Path) -> Set[str]:
    keys: Set[str] = set()
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        try:
            src = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        for rx in PATTERNS:
            for m in rx.finditer(src):
                k = m.group(1)
                if k and not is_external(k):
                    keys.add(k)
    return keys


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="Write changes to .env.example")
    args = ap.parse_args()

    root = repo_root()
    env_path = root / ENV_EXAMPLE_NAME
    if not env_path.exists():
        raise SystemExit(f"Missing {ENV_EXAMPLE_NAME} at {env_path}")

    text = env_path.read_text(encoding="utf-8", errors="replace")
    existing = parse_env_example_keys(text)
    used = scan_env_keys_in_py(root)

    missing = sorted(k for k in (used - existing) if not is_external(k))

    print(f"[sync] repo: {root}")
    print(f"[sync] existing keys in .env.example: {len(existing)}")
    print(f"[sync] env keys referenced in code:   {len(used)}")
    print(f"[sync] missing keys to add:          {len(missing)}")

    if not missing:
        print("[sync] OK: .env.example already covers code env usage.")
        return 0

    # Dry run
    if not args.write:
        print("[sync] DRY RUN: would append these keys:")
        for k in missing[:200]:
            print(f"  - {k}")
        if len(missing) > 200:
            print("  ... (truncated)")
        print("[sync] Re-run with --write to apply.")
        return 2

    # Backup
    backup_path = root / BACKUP_NAME
    backup_path.write_text(text, encoding="utf-8")
    print(f"[sync] backup written: {backup_path}")

    # Append section
    section = []
    section.append("")
    section.append(
        "# -----------------------------------------------------------------------------"
    )
    section.append("# AUTO-ADDED BY scripts/sync_env_example.py")
    section.append("# These keys were found in code via os.getenv/os.environ usage.")
    section.append(
        "# You may move them into the appropriate section above later, but keep the keys."
    )
    section.append(
        "# -----------------------------------------------------------------------------"
    )
    for k in missing:
        section.append(f"{k}=")
    section.append("")

    new_text = text.rstrip("\n") + "\n" + "\n".join(section)
    env_path.write_text(new_text, encoding="utf-8")
    print(f"[sync] wrote updates to: {env_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
