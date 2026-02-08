from __future__ import annotations

"""
tools/ruff_focus_report.py

Ruff Focus Report (LOCK-IN)
==========================

Purpose
-------
Small CLI helper to pretty-print Ruff JSON output grouped by filename.

Key requirements
----------------
- No heavy imports, no network
- Robust JSON decoding:
    - PowerShell redirection often yields UTF-16 LE
    - Handles UTF-8 BOM and common encodings
- Tolerates small variations in Ruff JSON schema (missing keys, alternate top-level shapes)
- Deterministic output ordering

Default behavior (compat)
-------------------------
- Reads `ruff_focus.json` (unless a path is provided)
- Groups by filename
- For each file, prints:
      <filename>
        <CODE>:<row>:<col> <message>
"""

import argparse
import codecs
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


# ──────────────────────────────────────────────────────────────────────────────
# Decoding / loading
# ──────────────────────────────────────────────────────────────────────────────
def _decode_bytes(data: bytes) -> str:
    """
    Decode bytes that may have BOMs and/or come from PowerShell redirection.

    Preference order:
      1) UTF-16 if BOM present
      2) UTF-8-sig if UTF-8 BOM present
      3) Try UTF-8
      4) Fallback to UTF-16
    """
    if data.startswith(codecs.BOM_UTF16_LE) or data.startswith(codecs.BOM_UTF16_BE):
        return data.decode("utf-16")
    if data.startswith(codecs.BOM_UTF8):
        return data.decode("utf-8-sig")

    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("utf-16")


def _read_all_bytes(path: str) -> bytes:
    """
    Read file bytes or stdin bytes.
    Path "-" means stdin.
    """
    if path == "-":
        return sys.stdin.buffer.read()
    return Path(path).read_bytes()


def _load_json_any(path: str) -> Any:
    """
    Load JSON from path (or stdin if path is "-") with robust decoding.
    """
    raw = _read_all_bytes(path)
    text = _decode_bytes(raw)
    return json.loads(text)


# ──────────────────────────────────────────────────────────────────────────────
# Normalization helpers
# ──────────────────────────────────────────────────────────────────────────────
def _as_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _as_str(x: Any, default: str = "") -> str:
    if x is None:
        return default
    try:
        s = str(x)
    except Exception:
        return default
    return s


def _iter_diagnostics(payload: Any) -> Iterable[dict[str, Any]]:
    """
    Ruff JSON output is commonly a list[dict], but other tools wrap it.
    We accept:
      - list of dicts
      - dict with "diagnostics" list
      - dict with "results" list
    """
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return

    if isinstance(payload, dict):
        for key in ("diagnostics", "results", "items"):
            v = payload.get(key)
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        yield item
                return

    # Unknown shape => empty
    return


def _diag_filename(d: dict[str, Any]) -> str:
    return _as_str(d.get("filename") or d.get("path") or d.get("file") or "<unknown>")


def _diag_code(d: dict[str, Any]) -> str:
    return _as_str(d.get("code") or d.get("rule") or "???").strip() or "???"


def _diag_message(d: dict[str, Any]) -> str:
    msg = d.get("message") or d.get("msg") or d.get("text") or ""
    return _as_str(msg).strip()


def _diag_location(d: dict[str, Any]) -> tuple[int, int]:
    """
    Ruff JSON typically uses:
      "location": {"row": <int>, "column": <int>}
    We also tolerate:
      "location": {"line": <int>, "column": <int>}
      "line": <int>, "column": <int>
    """
    loc = d.get("location")
    if isinstance(loc, dict):
        row = _as_int(loc.get("row") or loc.get("line"), 0)
        col = _as_int(loc.get("column") or loc.get("col"), 0)
        return row, col

    row = _as_int(d.get("row") or d.get("line"), 0)
    col = _as_int(d.get("column") or d.get("col"), 0)
    return row, col


# ──────────────────────────────────────────────────────────────────────────────
# Rendering
# ──────────────────────────────────────────────────────────────────────────────
def _render_grouped(diags: list[dict[str, Any]]) -> str:
    """
    Deterministic, human-readable output:
      <filename>
        CODE:row:col message
    """
    by_file: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for d in diags:
        by_file[_diag_filename(d)].append(d)

    lines: list[str] = []

    for fn in sorted(by_file.keys()):
        lines.append("")
        lines.append(fn)

        def sort_key(z: dict[str, Any]) -> tuple[str, int, int, str]:
            r, c = _diag_location(z)
            return (_diag_code(z), r, c, _diag_message(z))

        for d in sorted(by_file[fn], key=sort_key):
            row, col = _diag_location(d)
            code = _diag_code(d)
            msg = _diag_message(d)
            lines.append(f"  {code}:{row}:{col} {msg}")

    # Preserve original "leading newline before first filename" behavior
    # by joining as-is.
    return "\n".join(lines).lstrip("\n")  # print() adds newline anyway


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ruff_focus_report",
        description="Pretty-print Ruff JSON diagnostics grouped by filename.",
    )
    p.add_argument(
        "path",
        nargs="?",
        default="ruff_focus.json",
        help='Path to Ruff JSON output (use "-" for stdin).',
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        payload = _load_json_any(args.path)
    except FileNotFoundError:
        print(f"error: file not found: {args.path}", file=sys.stderr)
        return 2
    except json.JSONDecodeError as e:
        print(f"error: invalid json in {args.path}: {e}", file=sys.stderr)
        return 3
    except Exception as e:
        print(f"error: failed to read {args.path}: {type(e).__name__}: {e}", file=sys.stderr)
        return 4

    diags = list(_iter_diagnostics(payload))
    out = _render_grouped(diags)
    if out:
        print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
