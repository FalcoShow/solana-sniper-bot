# tools/patch_sniper_tail.py
from __future__ import annotations

"""
DEV-ONLY (ARCHIVE) — patch_sniper_tail.py
========================================

Why this exists
---------------
This script is a *one-off migration helper* used to patch an old/experimental
sniper/sniper.py by replacing everything after a marker with a provided "tail".

In the God-Tier Overhaul plan, this belongs in tools/ and MUST NOT be used
in production. It is intentionally guarded.

Safety contract
---------------
- Refuses to run unless you pass BOTH:
    (1) --yes-i-am-sure
    (2) environment variable ALLOW_SNIPER_TAIL_PATCH=1
- Refuses to run on Render-like environments by default.
- Creates a timestamped backup before writing.
- Atomic write (temp file -> replace).
- Idempotent: if the resulting content is identical, it does nothing.

Usage
-----
1) Create a tail file that contains the replacement section, e.g.:
   tools/sniper_tail_replacement.py

2) Run locally:
   set ALLOW_SNIPER_TAIL_PATCH=1
   python tools/patch_sniper_tail.py --target sniper/sniper.py --tail-file tools/sniper_tail_replacement.py --yes-i-am-sure --show-diff

Dry run:
   python tools/patch_sniper_tail.py ... --dry-run --show-diff

Marker behavior
---------------
We split the target file at the marker (default shown below). Everything after
the marker is replaced by the tail file content (normalized to include marker).

Default marker:
    "# ==============================================================================\n# Sniper"

If your file uses a different marker, pass --marker "..."
"""

import argparse
import difflib
import os
import sys
import time
from pathlib import Path

DEFAULT_MARKER = (
    "# ==============================================================================\n# Sniper"
)
DEFAULT_TARGET = "sniper/sniper.py"


# ------------------------------ guards ----------------------------------------


def _env_true(v: str | None) -> bool:
    if v is None:
        return False
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _looks_like_render_env() -> bool:
    """
    Best-effort detection of Render/hosted envs.
    We still require explicit opt-in; this just adds another safety fence.
    """
    hosted_keys = (
        "RENDER",
        "RENDER_SERVICE_ID",
        "RENDER_SERVICE_NAME",
        "DYNO",  # heroku-ish
        "K_SERVICE",  # gcp cloud run
        "AWS_EXECUTION_ENV",
    )
    for k in hosted_keys:
        if os.getenv(k):
            return True
    return os.getenv("ENV", "").strip().lower() in ("prod", "production")


def _require_opt_in(args: argparse.Namespace) -> None:
    if not args.yes_i_am_sure:
        raise SystemExit("Refusing to run: pass --yes-i-am-sure (this is dev-only).")
    if not _env_true(os.getenv("ALLOW_SNIPER_TAIL_PATCH")):
        raise SystemExit("Refusing to run: set ALLOW_SNIPER_TAIL_PATCH=1 (dev-only safety gate).")

    if _looks_like_render_env() and not args.force_on_hosted:
        raise SystemExit(
            "Refusing to run in a hosted/prod-like environment. "
            "If you truly intend to run here, pass --force-on-hosted (NOT recommended)."
        )


# ------------------------------ IO helpers ------------------------------------


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as err:
        raise SystemExit(f"File not found: {path}") from err
    except Exception as err:
        raise SystemExit(f"Failed reading {path}: {err}") from err


def _atomic_write(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def _backup_file(path: Path, backup_dir: Path | None = None) -> Path:
    ts = time.strftime("%Y%m%d-%H%M%S")
    bdir = backup_dir if backup_dir is not None else path.parent
    bdir.mkdir(parents=True, exist_ok=True)
    backup = bdir / f"{path.name}.bak.{ts}"
    backup.write_text(_read_text(path), encoding="utf-8")
    return backup


# ------------------------------ patch logic -----------------------------------


def _split_at_marker(src: str, marker: str) -> tuple[str, str]:
    parts = src.split(marker, 1)
    if len(parts) != 2:
        raise ValueError("marker_not_found")
    head, tail = parts[0], parts[1]
    return head, tail


def _normalize_tail(tail_text: str, marker: str) -> str:
    """
    Ensure tail begins with marker and ends with a trailing newline.
    """
    t = tail_text.replace("\r\n", "\n").replace("\r", "\n")
    t = t.strip("\n")

    if marker in t:
        idx = t.find(marker)
        t = t[idx:]
    else:
        t = marker + "\n" + t

    return t + "\n"


def _apply_patch(original: str, marker: str, tail_replacement: str) -> str:
    head, _old_tail = _split_at_marker(original, marker)
    return head + _normalize_tail(tail_replacement, marker)


def _unified_diff(a: str, b: str, fromfile: str, tofile: str) -> str:
    al = a.splitlines(True)
    bl = b.splitlines(True)
    diff = difflib.unified_diff(al, bl, fromfile=fromfile, tofile=tofile, n=3)
    return "".join(diff)


# ------------------------------ CLI -------------------------------------------


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="patch_sniper_tail",
        description="DEV-ONLY: replace everything after a marker in sniper/sniper.py with a provided tail file.",
    )
    p.add_argument(
        "--target", default=DEFAULT_TARGET, help="Path to target file (default: sniper/sniper.py)"
    )
    p.add_argument("--tail-file", required=True, help="Path to replacement tail text (required).")
    p.add_argument("--marker", default=DEFAULT_MARKER, help="Marker string used to split file.")
    p.add_argument(
        "--backup-dir",
        default="",
        help="Optional directory to store backups (default: alongside target).",
    )
    p.add_argument("--dry-run", action="store_true", help="Do not write; just report.")
    p.add_argument("--show-diff", action="store_true", help="Print unified diff to stdout.")
    p.add_argument(
        "--yes-i-am-sure", dest="yes_i_am_sure", action="store_true", help="Required opt-in."
    )
    p.add_argument(
        "--force-on-hosted",
        action="store_true",
        help="Override hosted/prod fence (NOT recommended).",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    _require_opt_in(args)

    target = Path(args.target)
    tail_file = Path(args.tail_file)

    marker = args.marker.replace("\\n", "\n")  # allow passing escaped newlines
    backup_dir = Path(args.backup_dir) if args.backup_dir else None

    original = _read_text(target)
    replacement_tail = _read_text(tail_file)

    try:
        patched = _apply_patch(original, marker, replacement_tail)
    except ValueError as e:
        if str(e) == "marker_not_found":
            print(f"ERROR: marker not found in {target}")
            print("Hint: pass --marker with the exact header string used in your file.")
            return 2
        raise

    if patched == original:
        print("No changes needed (already matches requested tail).")
        return 0

    if args.show_diff:
        print(
            _unified_diff(
                original, patched, fromfile=str(target), tofile=str(target) + " (patched)"
            )
        )

    if args.dry_run:
        print("DRY-RUN: patch computed; no files written.")
        return 0

    backup_path = _backup_file(target, backup_dir=backup_dir)
    _atomic_write(target, patched)

    print(f"Patched OK: {target}")
    print(f"Backup saved: {backup_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
