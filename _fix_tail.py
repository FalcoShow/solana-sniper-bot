#!/usr/bin/env python3
"""
_fix_tail.py  (ARCHIVE / dev-only)

Purpose
-------
Safely fix "stray indentation" / tab damage near the bottom ("tail") of a python file
(typically sniper/sniper.py) without injecting unknown template code.

What it does
------------
- Locates a tail anchor (prefers an "Entrypoint" marker, else `if __name__ == ...`,
  else the last `def main(` / `async def main(`)
- Converts tabs to spaces
- Detects global indent offset applied to the tail (e.g., everything shifted by 4 spaces)
- Dedents the tail by the detected offset while preserving relative indentation
- Writes a timestamped backup before applying

Usage
-----
  py _fix_tail.py --file sniper/sniper.py --dry-run
  py _fix_tail.py --file sniper/sniper.py --apply

Notes
-----
- This is intentionally conservative. It will NOT inject new runtime logic.
- If no anchor is found, it exits with a clear error.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
from collections.abc import Iterable
from pathlib import Path

ANCHOR_HINTS = (
    "Entrypoint",
    "ENTRYPOINT",
    "# Entrypoint",
    "# ==============================================================================",
)

TOPLEVEL_PAT = re.compile(
    r"""^(?P<indent>[ \t]*)(@|from\s+|import\s+|class\s+|def\s+|async\s+def\s+|if\s+__name__\s*==)""",
    re.VERBOSE,
)


def _ts() -> str:
    return dt.datetime.now(dt.UTC).strftime("%Y%m%d_%H%M%S")


def _read_text(p: Path) -> str:
    # Use utf-8 with fallback; keep it simple for a dev script
    try:
        return p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return p.read_text(encoding="utf-8", errors="replace")


def _write_text(p: Path, s: str) -> None:
    p.write_text(s, encoding="utf-8")


def _make_backup(p: Path, content: str) -> Path:
    b = p.with_suffix(p.suffix + f".bak.{_ts()}")
    _write_text(b, content)
    return b


def _find_anchor_line(lines: list[str]) -> int | None:
    """
    Return line index (0-based) where tail starts.
    Priority:
      1) last line containing an Entrypoint marker (string search)
      2) last `if __name__ == "__main__":`
      3) last `async def main(` or `def main(`
    """
    best = None

    # 1) Entrypoint-ish markers
    for i, ln in enumerate(lines):
        if any(h in ln for h in ANCHOR_HINTS):
            best = i
    if best is not None:
        return best

    # 2) __main__
    for i, ln in enumerate(lines):
        if "__name__" in ln and "__main__" in ln and ln.lstrip().startswith("if "):
            best = i
    if best is not None:
        return best

    # 3) last main()
    for i, ln in enumerate(lines):
        s = ln.lstrip()
        if s.startswith("async def main(") or s.startswith("def main("):
            best = i
    return best


def _count_leading_spaces(s: str) -> int:
    n = 0
    for ch in s:
        if ch == " ":
            n += 1
        else:
            break
    return n


def _compute_tail_shift(tail_lines: Iterable[str]) -> int:
    """
    Detect a constant left-shift that appears to be applied to the entire tail.
    We estimate this by looking at likely top-level lines and taking the minimum indent.
    If the minimum indent is >= 2, we treat it as a global shift candidate.
    """
    indents = []
    for ln in tail_lines:
        if not ln.strip():
            continue
        m = TOPLEVEL_PAT.match(ln.replace("\t", "    "))
        if not m:
            continue
        indent = m.group("indent").replace("\t", "    ")
        indents.append(_count_leading_spaces(indent))

    if not indents:
        return 0

    shift = min(indents)
    # only shift if it looks like a real global indent (common is 2/4/8)
    if shift in (2, 4, 8, 12, 16):
        return shift
    if shift > 0 and shift <= 16:
        # tolerate odd-but-small shifts, but don't do anything crazy
        return shift
    return 0


def _dedent_lines(lines: list[str], shift: int) -> list[str]:
    if shift <= 0:
        return lines
    out: list[str] = []
    prefix = " " * shift
    for ln in lines:
        # only remove if line actually has the prefix
        if ln.startswith(prefix):
            out.append(ln[shift:])
        else:
            out.append(ln)
    return out


def _ensure_trailing_newline(s: str) -> str:
    return s if s.endswith("\n") else s + "\n"


def process_file(path: Path, apply: bool, dry_run: bool) -> tuple[bool, str]:
    src = _read_text(path)
    src = src.replace("\r\n", "\n").replace("\r", "\n")

    lines = src.splitlines(keepends=True)
    anchor = _find_anchor_line([ln.rstrip("\n") for ln in lines])
    if anchor is None:
        return False, "No anchor found (no Entrypoint marker, no __main__, no main())."

    head = lines[:anchor]
    tail = lines[anchor:]

    # normalize tabs → spaces in tail (common source of weird indentation)
    tail_norm = [ln.replace("\t", "    ") for ln in tail]
    shift = _compute_tail_shift(tail_norm)

    # apply dedent
    tail_fixed = _dedent_lines(tail_norm, shift)

    new = "".join(head + tail_fixed)
    new = _ensure_trailing_newline(new)

    changed = new != src

    msg = f"anchor_line={anchor + 1} shift={shift} changed={changed} file={path.as_posix()}"

    if dry_run and not apply:
        return True, "[DRY-RUN] " + msg

    if apply:
        backup = _make_backup(path, src)
        _write_text(path, new)
        return True, msg + f" backup={backup.name}"

    # default: no apply, no dry_run
    return True, msg + " (no changes written; pass --apply)"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Fix stray indentation in file tail safely (ARCHIVE/dev-only)."
    )
    ap.add_argument(
        "--file", default="sniper/sniper.py", help="Target python file (default: sniper/sniper.py)"
    )
    ap.add_argument("--apply", action="store_true", help="Write changes to disk (creates backup)")
    ap.add_argument("--dry-run", action="store_true", help="Print what would happen; do not write")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    p = Path(args.file)
    if not p.exists():
        raise SystemExit(f"File not found: {p}")

    ok, msg = process_file(p, apply=bool(args.apply), dry_run=bool(args.dry_run))
    print(msg)
    if not ok:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
