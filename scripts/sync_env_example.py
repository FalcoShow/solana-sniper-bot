# -*- coding: utf-8 -*-
from __future__ import annotations

"""
scripts/sync_env_example.py

Contract-safe helper to keep .env.example aligned with *code-referenced* env keys,
WITHOUT introducing "env example vs contract" drift.

What it does (P0-safe):
- Scans Python files using AST for literal env key usage:
    os.getenv("KEY")
    os.environ.get("KEY")
    os.environ["KEY"]
    os.environ.setdefault("KEY", ...)
    os.environ.pop("KEY", ...)
- Loads canonical env keys from utils_pkg.contracts (best-effort)
- Compares:
    - code-used keys vs contract keys (STRICT, with a narrow exception for proxy vars; see below)
    - contract keys vs .env.example keys (presence is CASE-INSENSITIVE to avoid NO_PROXY/no_proxy churn)
- If code uses keys not in contract: prints them and exits non-zero (you must add to contracts.py)
- If .env.example is missing keys that are in contract: can append them in an idempotent block.

Proxy canonicalization (the critical fix):
- Some ecosystems (requests/urllib) honor lowercase proxy env vars like "no_proxy".
- The canonical contract should keep ONLY uppercase proxy keys (NO_PROXY, HTTP_PROXY, HTTPS_PROXY, ALL_PROXY),
  but code may still set lowercase for compatibility.
- Therefore, for the code-vs-contract subset check, we canonicalize ONLY those four proxy keys to uppercase.

Usage:
  py scripts\\sync_env_example.py              (dry run)
  py scripts\\sync_env_example.py --write      (apply)
  py scripts\\sync_env_example.py --json       (emit JSON summary)
  py scripts\\sync_env_example.py --json-only  (JSON only)
  py scripts\\sync_env_example.py --debug      (extra diagnostics)

Exit codes:
  0 = OK (no drift) OR wrote fixes successfully
  2 = Drift detected but not written (dry run; re-run with --write)
  3 = ERROR: code uses env keys missing from contract (fix contracts.py first)
  4 = ERROR: failed to load contract keys (script still runs, but will not write)
"""

import argparse
import ast
import importlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

ENV_EXAMPLE_NAME = ".env.example"

# Marker block to make writes idempotent
AUTO_BLOCK_START = "# >>> AUTO-ADDED BY scripts/sync_env_example.py (DO NOT EDIT BELOW) >>>"
AUTO_BLOCK_END = "# <<< AUTO-ADDED BY scripts/sync_env_example.py (DO NOT EDIT ABOVE) <<<"

# Accept both uppercase and lowercase assignments in .env.example (optional `export`)
ENV_ASSIGN_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z][A-Za-z0-9_]*)\s*=", re.M)

# Valid env key tokens (conservative)
ENV_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

# Directories to skip when scanning repo
SKIP_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    "node_modules",
    "dist",
    "build",
    ".eggs",
}

# Platform/external env keys we do NOT force into contract/env.example
# (This should roughly track the allowlist in your contract tests.)
EXTERNAL_KEYS = {
    "PATH",
    "PYTHONPATH",
    "HOME",
    "USER",
    "USERNAME",
    "SHELL",
    "TERM",
    "PORT",
}
EXTERNAL_PREFIXES = ("RENDER_", "GITHUB_", "RUNNER_", "CI", "PYTEST_")

# Proxy vars that are case-equivalent for code-vs-contract membership checks.
# Canonical contract should prefer uppercase; code may still use lowercase for compatibility.
PROXY_EQUIV = {"NO_PROXY", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"}


def _normalize_for_contract_membership(key: str) -> str:
    """
    Normalize ONLY proxy keys to their uppercase canonical form so code can use
    'no_proxy' without forcing it into the canonical contract.

    Everything else remains strict (no global uppercasing).
    """
    k = (key or "").strip()
    if not k:
        return ""
    ku = k.upper()
    if ku in PROXY_EQUIV:
        return ku
    return k


# When we add to .env.example, prefer the uppercase variant if it exists in contract
def _preferred_env_example_key(contract_keys: set[str], key: str) -> str:
    up = key.upper()
    if up in contract_keys:
        return up
    return key


def _repo_root() -> Path:
    # scripts/ is under repo root
    return Path(__file__).resolve().parent.parent


def _is_external(key: str) -> bool:
    # treat external checks case-insensitively
    k = key.upper()
    if k in EXTERNAL_KEYS:
        return True
    return any(k.startswith(p) for p in EXTERNAL_PREFIXES)


def _iter_py_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        yield p


def _parse_env_example_keys(text: str) -> set[str]:
    keys = set(ENV_ASSIGN_RE.findall(text))
    return {k for k in keys if ENV_KEY_RE.fullmatch(k or "")}


def _env_example_has_key(env_keys: set[str], key: str) -> bool:
    """
    Presence check that is case-insensitive for typical env tokens.

    This prevents churn like:
      .env.example has "NO_PROXY"
      code/contracts mention "no_proxy"
      -> treated as already covered.
    """
    if key in env_keys:
        return True
    up = key.upper()
    lo = key.lower()
    return (up in env_keys) or (lo in env_keys)


def _remove_existing_auto_block(text: str) -> str:
    """
    Remove the existing auto-added block if present (idempotent behavior).
    """
    if AUTO_BLOCK_START not in text:
        return text

    start_idx = text.find(AUTO_BLOCK_START)
    end_idx = text.find(AUTO_BLOCK_END, start_idx)
    if end_idx == -1:
        # If somehow missing end marker, drop from start marker to EOF
        return text[:start_idx].rstrip() + "\n"

    end_idx = end_idx + len(AUTO_BLOCK_END)
    before = text[:start_idx].rstrip()
    after = text[end_idx:].lstrip("\n")
    if after:
        return before + "\n\n" + after
    return before + "\n"


def _make_backup_path(env_path: Path) -> Path:
    """
    Create a non-clobbering backup path like:
      .env.example.bak
      .env.example.bak.1
      .env.example.bak.2
    """
    base = env_path.with_name(env_path.name + ".bak")
    if not base.exists():
        return base
    for i in range(1, 1000):
        candidate = env_path.with_name(env_path.name + f".bak.{i}")
        if not candidate.exists():
            return candidate
    return env_path.with_name(env_path.name + ".bak.9999")


@dataclass(frozen=True)
class ScanResult:
    ok: bool
    exit_code: int
    repo_root: str
    env_example_path: str
    contract_loaded: bool
    contract_source: str
    counts: dict[str, int]
    missing_in_env_example: list[str]
    used_missing_in_env_example: list[str]
    used_missing_in_contract: list[str]
    wrote: bool
    notes: list[str]


class _EnvUsageVisitor(ast.NodeVisitor):
    """
    Extract literal env key usage for these patterns:

      os.getenv("KEY")
      os.environ.get("KEY")
      os.environ.setdefault("KEY", ...)
      os.environ.pop("KEY", ...)
      os.environ["KEY"]

    Also supports (best-effort):
      getenv("KEY")            (if imported directly)
      environ.get("KEY")       (if imported directly)
      environ["KEY"]           (if imported directly)
    """

    def __init__(self) -> None:
        self.keys: set[str] = set()

    @staticmethod
    def _const_str(node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    @staticmethod
    def _is_name(node: ast.AST, name: str) -> bool:
        return isinstance(node, ast.Name) and node.id == name

    @staticmethod
    def _is_os_name(node: ast.AST) -> bool:
        return _EnvUsageVisitor._is_name(node, "os")

    @staticmethod
    def _is_os_environ(node: ast.AST) -> bool:
        # os.environ
        return (
            isinstance(node, ast.Attribute)
            and node.attr == "environ"
            and _EnvUsageVisitor._is_os_name(node.value)
        )

    def visit_Call(self, node: ast.Call) -> Any:
        # os.getenv("KEY") or getenv("KEY")
        if isinstance(node.func, ast.Attribute):
            if self._is_os_name(node.func.value) and node.func.attr == "getenv":
                if node.args:
                    k = self._const_str(node.args[0])
                    if k:
                        self.keys.add(k)

            # os.environ.get|setdefault|pop("KEY")
            if self._is_os_environ(node.func.value):
                if node.func.attr in {"get", "setdefault", "pop"} and node.args:
                    k = self._const_str(node.args[0])
                    if k:
                        self.keys.add(k)

        elif isinstance(node.func, ast.Name):
            # getenv("KEY") if imported directly
            if node.func.id == "getenv" and node.args:
                k = self._const_str(node.args[0])
                if k:
                    self.keys.add(k)

        # environ.get("KEY") if environ imported directly
        if isinstance(node.func, ast.Attribute) and self._is_name(node.func.value, "environ"):
            if node.func.attr in {"get", "setdefault", "pop"} and node.args:
                k = self._const_str(node.args[0])
                if k:
                    self.keys.add(k)

        self.generic_visit(node)
        return None

    def visit_Subscript(self, node: ast.Subscript) -> Any:
        # os.environ["KEY"]
        if self._is_os_environ(node.value):
            k = self._const_str(node.slice)  # py3.9+: slice is an expr
            if k:
                self.keys.add(k)

        # environ["KEY"] if environ imported directly
        if self._is_name(node.value, "environ"):
            k = self._const_str(node.slice)
            if k:
                self.keys.add(k)

        self.generic_visit(node)
        return None


def _scan_env_keys_in_repo(root: Path) -> tuple[set[str], list[str]]:
    """
    Return (keys, parse_errors)
    parse_errors contains strings "relpath: error"
    """
    keys: set[str] = set()
    errors: list[str] = []

    for p in _iter_py_files(root):
        try:
            src = p.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue

        try:
            tree = ast.parse(src, filename=str(p))
        except Exception as e:
            rel = str(p.relative_to(root))
            errors.append(f"{rel}: {type(e).__name__}: {e}")
            continue

        v = _EnvUsageVisitor()
        v.visit(tree)

        for k in v.keys:
            if not k:
                continue
            if not ENV_KEY_RE.fullmatch(k):
                continue
            if _is_external(k):
                continue
            keys.add(k)

    return keys, errors


def _bootstrap_sys_path_for_contract(repo_root: Path) -> None:
    root_str = str(repo_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    src = repo_root / "src"
    if src.is_dir():
        src_str = str(src)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)


def _extract_keys_from_obj(obj: Any) -> set[str]:
    keys: set[str] = set()

    if isinstance(obj, dict):
        keys = {str(k) for k in obj.keys()}
    elif isinstance(obj, (list, tuple, set, frozenset)):
        keys = {str(k) for k in obj}
    elif hasattr(obj, "keys"):
        try:
            keys = {str(k) for k in obj.keys()}  # type: ignore[attr-defined]
        except Exception:
            keys = set()

    # Validate tokens and drop junk
    keys = {k for k in keys if isinstance(k, str) and ENV_KEY_RE.fullmatch(k or "")}
    return keys


def _load_contract_keys(repo_root: Path) -> tuple[set[str], list[str], bool, str]:
    """
    Best-effort load of canonical keys from utils_pkg.contracts.

    Prefers ENV_KEYS (your repo already uses this).
    Also supports common containers used across earlier rewrites.
    """
    notes: list[str] = []
    _bootstrap_sys_path_for_contract(repo_root)

    try:
        mod = importlib.import_module("utils_pkg.contracts")
    except Exception as e:
        notes.append(f"Failed to import utils_pkg.contracts: {type(e).__name__}: {e}")
        return set(), notes, False, ""

    # Prefer these in order (ENV_KEYS first!)
    candidate_names = (
        "ENV_KEYS",
        "CANONICAL_ENV_KEYS",
        "CANONICAL_ENV_VARS",
        "ENV_SPEC",
        "ENV_CONTRACT",
        "CONTRACT",
        "SPEC",
    )

    for name in candidate_names:
        if not hasattr(mod, name):
            continue
        obj = getattr(mod, name)
        keys = _extract_keys_from_obj(obj)
        if not keys:
            continue

        # Drop external keys (case-insensitively) so we don't force them into env.example
        keys = {k for k in keys if not _is_external(k)}

        notes.append(f"Loaded contract keys from utils_pkg.contracts.{name} ({len(keys)} keys)")
        return keys, notes, True, name

    notes.append(
        "Could not find a canonical key container in utils_pkg.contracts (no known names)."
    )
    return set(), notes, False, ""


def _write_env_example(env_path: Path, original_text: str, keys_to_append: list[str]) -> None:
    cleaned = _remove_existing_auto_block(original_text).rstrip() + "\n"

    block_lines: list[str] = []
    block_lines.append(AUTO_BLOCK_START)
    block_lines.append(
        "# These keys are in the canonical contract but were missing from .env.example."
    )
    block_lines.append("# Keep keys; values should be filled in your real .env (not committed).")
    for k in keys_to_append:
        block_lines.append(f"{k}=")
    block_lines.append(AUTO_BLOCK_END)

    new_text = cleaned + "\n" + "\n".join(block_lines) + "\n"

    backup_path = _make_backup_path(env_path)
    backup_path.write_text(original_text, encoding="utf-8")

    tmp_path = env_path.with_suffix(env_path.suffix + ".tmp")
    tmp_path.write_text(new_text, encoding="utf-8")
    tmp_path.replace(env_path)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="Write changes to .env.example")
    ap.add_argument("--json", action="store_true", help="Emit JSON summary to stdout")
    ap.add_argument("--json-only", action="store_true", help="Emit JSON only (no human logs)")
    ap.add_argument("--debug", action="store_true", help="Extra diagnostics")
    ns, _rest = ap.parse_known_args(argv)

    def log(msg: str) -> None:
        if ns.json_only:
            return
        print(msg)

    root = _repo_root()
    env_path = root / ENV_EXAMPLE_NAME

    if not env_path.exists():
        payload = ScanResult(
            ok=False,
            exit_code=4,
            repo_root=str(root),
            env_example_path=str(env_path),
            contract_loaded=False,
            contract_source="",
            counts={},
            missing_in_env_example=[],
            used_missing_in_env_example=[],
            used_missing_in_contract=[],
            wrote=False,
            notes=[f"Missing {ENV_EXAMPLE_NAME} at repo root: {env_path}"],
        )
        if ns.json or ns.json_only:
            print(json.dumps(payload.__dict__, ensure_ascii=False))
        else:
            log(payload.notes[0])
        return payload.exit_code

    env_text = env_path.read_text(encoding="utf-8", errors="replace")
    env_keys = _parse_env_example_keys(env_text)

    used_keys_raw, parse_errors = _scan_env_keys_in_repo(root)
    contract_keys_raw, contract_notes, contract_loaded, contract_source = _load_contract_keys(root)

    # Normalize only proxy keys for the strict "code-used keys must exist in contract" check.
    used_keys = {_normalize_for_contract_membership(k) for k in used_keys_raw}
    contract_keys = {_normalize_for_contract_membership(k) for k in contract_keys_raw}

    notes: list[str] = []
    notes.extend(contract_notes)
    if parse_errors:
        notes.append(f"AST parse errors in repo files: {len(parse_errors)}")

    # STRICT (with proxy canonicalization): code-used keys must exist in contract
    used_missing_in_contract: list[str] = []
    if contract_loaded:
        used_missing_in_contract = sorted(used_keys - contract_keys)

    # CASE-INSENSITIVE presence for env.example coverage (prevents NO_PROXY/no_proxy churn)
    def env_has(k: str) -> bool:
        return _env_example_has_key(env_keys, k)

    missing_in_env_example: list[str] = []
    if contract_loaded:
        missing_in_env_example = sorted([k for k in contract_keys if not env_has(k)])

    used_missing_in_env_example = sorted([k for k in used_keys if not env_has(k)])

    # Keys we are allowed to write:
    # Only append keys that are in contract (so we never break lock-in tests).
    keys_to_append_set: set[str] = set()
    if contract_loaded:
        for k in missing_in_env_example:
            keys_to_append_set.add(_preferred_env_example_key(contract_keys, k))
        for k in used_missing_in_env_example:
            if k in contract_keys:
                keys_to_append_set.add(_preferred_env_example_key(contract_keys, k))

    # Ensure we don't add duplicates that already exist (case-insensitive)
    keys_to_append: list[str] = []
    for k in sorted(keys_to_append_set):
        if not env_has(k):
            keys_to_append.append(k)

    log(f"[sync] repo: {root}")
    log(f"[sync] existing keys in .env.example: {len(env_keys)}")
    log(f"[sync] env keys referenced in code:   {len(used_keys)}")
    if contract_loaded:
        log(f"[sync] keys in utils_pkg.contracts:  {len(contract_keys)} (source={contract_source})")
    else:
        log("[sync] WARN: could not load contract keys from utils_pkg.contracts")

    if ns.debug and contract_notes and not ns.json_only:
        log("[sync] contract load notes:")
        for n in contract_notes:
            log(f"  - {n}")

    if ns.debug and not ns.json_only:
        # Show proxy keys encountered in code (helps confirm the fix).
        proxy_seen = sorted([k for k in used_keys_raw if (k or "").strip().upper() in PROXY_EQUIV])
        if proxy_seen:
            log("[sync] debug: proxy-like keys seen in code (raw):")
            for k in proxy_seen[:50]:
                log(f"  - {k}")

    if parse_errors and not ns.json_only:
        log("[sync] WARN: Some .py files failed to parse (syntax errors). First few:")
        for err in parse_errors[:20]:
            log(f"  - {err}")
        if len(parse_errors) > 20:
            log("  ... (truncated)")

    if contract_loaded and used_missing_in_contract:
        log(
            "[sync] ERROR: Code references env keys that are NOT in utils_pkg.contracts (must fix contracts.py):"
        )
        for k in used_missing_in_contract[:200]:
            log(f"  - {k}")
        if len(used_missing_in_contract) > 200:
            log("  ... (truncated)")

        payload = ScanResult(
            ok=False,
            exit_code=3,
            repo_root=str(root),
            env_example_path=str(env_path),
            contract_loaded=True,
            contract_source=contract_source,
            counts={
                "env_example": len(env_keys),
                "code_used": len(used_keys),
                "contract": len(contract_keys),
                "parse_errors": len(parse_errors),
            },
            missing_in_env_example=missing_in_env_example,
            used_missing_in_env_example=used_missing_in_env_example,
            used_missing_in_contract=used_missing_in_contract,
            wrote=False,
            notes=notes,
        )
        if ns.json or ns.json_only:
            print(json.dumps(payload.__dict__, ensure_ascii=False))
        return 3

    if not contract_loaded:
        payload = ScanResult(
            ok=False,
            exit_code=4,
            repo_root=str(root),
            env_example_path=str(env_path),
            contract_loaded=False,
            contract_source="",
            counts={
                "env_example": len(env_keys),
                "code_used": len(used_keys),
                "parse_errors": len(parse_errors),
            },
            missing_in_env_example=[],
            used_missing_in_env_example=used_missing_in_env_example,
            used_missing_in_contract=[],
            wrote=False,
            notes=notes,
        )
        if ns.json or ns.json_only:
            print(json.dumps(payload.__dict__, ensure_ascii=False))
        else:
            log(
                "[sync] ERROR: Cannot load contract keys; refusing to write to avoid breaking lock-in tests."
            )
        return 4

    # OK if everything covered and nothing to add
    if not missing_in_env_example and not used_missing_in_env_example:
        log("[sync] OK: .env.example and contracts cover code env usage.")
        payload = ScanResult(
            ok=True,
            exit_code=0,
            repo_root=str(root),
            env_example_path=str(env_path),
            contract_loaded=True,
            contract_source=contract_source,
            counts={
                "env_example": len(env_keys),
                "code_used": len(used_keys),
                "contract": len(contract_keys),
                "parse_errors": len(parse_errors),
            },
            missing_in_env_example=[],
            used_missing_in_env_example=[],
            used_missing_in_contract=[],
            wrote=False,
            notes=notes,
        )
        if ns.json or ns.json_only:
            print(json.dumps(payload.__dict__, ensure_ascii=False))
        return 0

    # Drift exists; show what we'd add
    log(f"[sync] missing keys to add to .env.example (contract-safe): {len(keys_to_append)}")
    if keys_to_append and not ns.json_only:
        for k in keys_to_append[:200]:
            log(f"  - {k}")
        if len(keys_to_append) > 200:
            log("  ... (truncated)")

    if not ns.write:
        payload = ScanResult(
            ok=False,
            exit_code=2,
            repo_root=str(root),
            env_example_path=str(env_path),
            contract_loaded=True,
            contract_source=contract_source,
            counts={
                "env_example": len(env_keys),
                "code_used": len(used_keys),
                "contract": len(contract_keys),
                "parse_errors": len(parse_errors),
            },
            missing_in_env_example=missing_in_env_example,
            used_missing_in_env_example=used_missing_in_env_example,
            used_missing_in_contract=[],
            wrote=False,
            notes=notes,
        )
        if ns.json or ns.json_only:
            print(json.dumps(payload.__dict__, ensure_ascii=False))
        else:
            log("[sync] DRY RUN: re-run with --write to apply.")
        return 2

    # Write changes (idempotent block)
    wrote = False
    if keys_to_append:
        _write_env_example(env_path, env_text, keys_to_append)
        log(f"[sync] wrote updates to: {env_path}")
        wrote = True
    else:
        log("[sync] Nothing contract-safe to write (but drift exists outside contract coverage).")

    payload = ScanResult(
        ok=True,
        exit_code=0 if wrote else 2,
        repo_root=str(root),
        env_example_path=str(env_path),
        contract_loaded=True,
        contract_source=contract_source,
        counts={
            "env_example": len(env_keys),
            "code_used": len(used_keys),
            "contract": len(contract_keys),
            "parse_errors": len(parse_errors),
        },
        missing_in_env_example=missing_in_env_example,
        used_missing_in_env_example=used_missing_in_env_example,
        used_missing_in_contract=[],
        wrote=wrote,
        notes=notes,
    )
    if ns.json or ns.json_only:
        print(json.dumps(payload.__dict__, ensure_ascii=False))

    return 0 if wrote else 2


if __name__ == "__main__":
    raise SystemExit(main())
