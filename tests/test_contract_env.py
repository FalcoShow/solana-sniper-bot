from __future__ import annotations

"""
tests/test_contract_env.py — Env Contract Tests (God-Tier LOCK-IN, Phase 0–7)
============================================================================

Single source of truth for env vars:
  1) utils_pkg/contracts.py   (canonical contract definitions)
  2) .env.example             (human-editable deployment contract)
  3) code usage               (os.getenv / os.environ.get / os.environ[...] / setdefault usage)

Hard goals:
- ZERO network / ZERO external services needed.
- Fail FAST when contract drifts.
- Actionable diffs: what keys are missing/unknown and where keys are referenced.
- Windows-safe (pytest-asyncio friendly, no event-loop abuse).

Enforced invariants:
- .env.example exists, is parseable, has no duplicate keys, and uses UPPER_SNAKE_CASE keys
- utils_pkg.contracts exports the canonical env contract (ENV_KEYS/ENV_VARS + REQUIRED/SAFE sets + external allowlists)
- .env.example matches the canonical contract exactly (no missing/unknown keys)
- code does not reference env vars NOT in the canonical contract (except approved external env keys)
- "safe mode" import path does not crash (no secrets required; no I/O at import-time)

NOTE:
- Socket-level network blocking is handled globally by tests/conftest.py (loopback-only).
  This file still enforces SAFE_MODE/NO_NETWORK toggles defensively.
"""

import ast
import contextlib
import importlib
import os
import re
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.contract


# -----------------------------------------------------------------------------
# Repo root detection + .env.example parsing
# -----------------------------------------------------------------------------
_ENV_LINE_RE = re.compile(
    r"""^\s*(?:export\s+)?(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<val>.*)\s*$"""
)
_UPPER_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")

_REQUIRED_HINT_RE = re.compile(r"\brequired\b", re.IGNORECASE)

_SAFE_TOGGLE_KEY_RE = re.compile(
    r"(SAFE|DRY|PAPER|SIM|NO_NETWORK|DISABLE_NETWORK|READ_ONLY|CI|TEST|PYTEST|BACKTEST|DISABLE_)_?",
    re.IGNORECASE,
)


def _repo_root() -> Path:
    """
    Prefer REPO_ROOT (set by conftest), else discover from filesystem.
    """
    rr = (os.environ.get("REPO_ROOT") or "").strip()
    if rr:
        try:
            p = Path(rr).resolve()
            if p.exists():
                return p
        except Exception:
            pass
    return _find_repo_root(Path(__file__).resolve().parent)


def _find_repo_root(start: Path) -> Path:
    """
    Robust repo root discovery:
      - Prefer directory containing .env.example
      - Else fall back to project markers
      - Supports flat and src/ layouts
    """
    start = start.resolve()
    markers = (
        ".env.example",
        "pyproject.toml",
        "pytest.ini",
        ".git",
        "requirements.txt",
        "setup.cfg",
        "render.yaml",
        ".renderignore",
    )

    best: Path | None = None
    for p in (start, *start.parents):
        if (p / ".env.example").exists():
            return p

        if best is None and any((p / m).exists() for m in markers):
            best = p

        src = p / "src"
        if src.is_dir() and any(
            (src / x).exists() for x in ("utils_pkg", "scanner", "scripts", "tests")
        ):
            best = best or p

    return best or (start.parents[0] if start.parents else start)


def _ensure_import_paths(root: Path) -> None:
    """
    Ensure repo root (and optional src/) are importable for `import utils_pkg...`.
    Hardened for Windows/Render path quirks.
    """
    root = root.resolve()
    candidates = [root]
    src = root / "src"
    if src.is_dir():
        candidates.append(src)
    for p in candidates:
        ps = str(p)
        if ps and ps not in sys.path:
            sys.path.insert(0, ps)


def _strip_inline_comment(raw: str) -> tuple[str, str]:
    """
    Split raw value into (value_part, comment_without_hash),
    treating '#' as a comment delimiter ONLY if it's not within quotes.
    """
    s = (raw or "").rstrip()
    if not s:
        return "", ""

    in_sq = False
    in_dq = False
    for i, ch in enumerate(s):
        if ch == "'" and not in_dq:
            in_sq = not in_sq
        elif ch == '"' and not in_sq:
            in_dq = not in_dq
        elif ch == "#" and not in_sq and not in_dq:
            return s[:i].rstrip(), s[i + 1 :].strip()
    return s, ""


def _unquote(val: str) -> str:
    v = (val or "").strip()
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        return v[1:-1]
    return v


@dataclass(frozen=True, slots=True)
class EnvExampleEntry:
    key: str
    raw_value: str
    value: str
    comment: str
    line_no: int

    @property
    def is_blank(self) -> bool:
        return self.value.strip() == ""


def _parse_env_example(path: Path) -> dict[str, EnvExampleEntry]:
    """
    Parse .env.example as KEY=VALUE lines (with optional `export`).

    - Fails on duplicate keys.
    - Fails on lines that look like assignments but are malformed.
    - Allows comment-only lines and section headers (no '=').
    """
    entries: dict[str, EnvExampleEntry] = {}
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception as e:
        raise AssertionError(
            f"Failed to read .env.example: {path} ({type(e).__name__}: {e})"
        ) from e

    for idx, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        m = _ENV_LINE_RE.match(line)
        if not m:
            if "=" in stripped:
                raise AssertionError(
                    f"Unparseable assignment in .env.example at line {idx}:\n"
                    f"  {line}\n"
                    "Expected format: KEY=VALUE (optional: export KEY=VALUE)."
                )
            continue

        key = (m.group("key") or "").strip()
        raw_val = (m.group("val") or "").strip()

        if not key:
            raise AssertionError(f"Empty key parsed at line {idx} in .env.example")

        if key in entries:
            prev = entries[key].line_no
            raise AssertionError(
                f"Duplicate key in .env.example: {key!r} (line {prev} and line {idx})"
            )

        val_part, cmt = _strip_inline_comment(raw_val)
        val_clean = _unquote(val_part)

        entries[key] = EnvExampleEntry(
            key=key,
            raw_value=raw_val,
            value=val_clean,
            comment=cmt,
            line_no=idx,
        )

    return entries


def _required_keys_from_comments(entries: dict[str, EnvExampleEntry]) -> set[str]:
    """
    Conservative: only keys with inline comments containing the word 'required' count.
    """
    req: set[str] = set()
    for k, e in entries.items():
        if _REQUIRED_HINT_RE.search(e.comment or ""):
            req.add(k)
    return req


# -----------------------------------------------------------------------------
# Canonical contract import (utils_pkg/contracts.py)
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class ContractSpec:
    env_keys: set[str]
    required_keys: set[str]
    safe_mode_keys: set[str]
    external_allowlist: set[str]
    external_allow_patterns: tuple[re.Pattern[str], ...]


def _iter_env_vars_items(raw: Any) -> Iterable[Any]:
    if raw is None:
        return []
    if isinstance(raw, list | tuple | set):
        return raw
    if hasattr(raw, "__iter__"):
        return raw
    return []


def _load_contract(root: Path) -> ContractSpec:
    """
    Contracts are canonical and REQUIRED for LOCK-IN.
    Fails with clear messages if contracts.py is missing or incomplete.
    """
    _ensure_import_paths(root)

    try:
        mod = importlib.import_module("utils_pkg.contracts")
    except Exception as e:
        raise AssertionError(
            "Missing canonical env contract module: utils_pkg/contracts.py\n"
            "It must export:\n"
            "  - ENV_VARS (iterable of items with .key OR plain strings)\n"
            "  - ENV_KEYS (set[str]) OR env_keys() -> set[str]\n"
            "  - REQUIRED_KEYS (set[str]) OR required_env_keys() -> set[str]\n"
            "  - SAFE_MODE_KEYS (set[str]) OR safe_mode_keys() -> set[str]\n"
            "  - EXTERNAL_ENV_ALLOWLIST (set[str])\n"
            "  - EXTERNAL_ENV_ALLOW_PATTERNS (tuple[Pattern[str], ...])\n"
            f"Import error: {type(e).__name__}: {e}"
        ) from e

    env_keys: set[str] = set()
    if hasattr(mod, "ENV_KEYS"):
        env_keys = set(mod.ENV_KEYS)
    elif hasattr(mod, "env_keys") and callable(mod.env_keys):
        env_keys = set(mod.env_keys())  # type: ignore[attr-defined]
    elif hasattr(mod, "ENV_VARS"):
        for item in _iter_env_vars_items(mod.ENV_VARS):
            if isinstance(item, str) and item.strip():
                env_keys.add(item.strip())
            else:
                k = getattr(item, "key", None)
                if isinstance(k, str) and k.strip():
                    env_keys.add(k.strip())

    required: set[str] = set()
    if hasattr(mod, "REQUIRED_KEYS"):
        required = set(mod.REQUIRED_KEYS)
    elif hasattr(mod, "required_env_keys") and callable(mod.required_env_keys):
        required = set(mod.required_env_keys())  # type: ignore[attr-defined]

    safe_keys: set[str] = set()
    if hasattr(mod, "SAFE_MODE_KEYS"):
        safe_keys = set(mod.SAFE_MODE_KEYS)
    elif hasattr(mod, "safe_mode_keys") and callable(mod.safe_mode_keys):
        safe_keys = set(mod.safe_mode_keys())  # type: ignore[attr-defined]

    allowlist_raw = getattr(mod, "EXTERNAL_ENV_ALLOWLIST", set())
    allowlist = set(allowlist_raw) if isinstance(allowlist_raw, set | list | tuple) else set()

    patterns_raw = getattr(mod, "EXTERNAL_ENV_ALLOW_PATTERNS", tuple())
    if patterns_raw and not isinstance(patterns_raw, tuple):
        patterns_raw = tuple(patterns_raw)

    if not env_keys:
        raise AssertionError(
            "utils_pkg.contracts loaded, but no env keys were discovered.\n"
            "Fix: export ENV_KEYS (recommended) or ENV_VARS with items containing `.key`."
        )

    bad_req = sorted(k for k in required if k not in env_keys)
    bad_safe = sorted(k for k in safe_keys if k not in env_keys)
    assert not bad_req, f"contracts REQUIRED_KEYS contains keys not in ENV_KEYS: {bad_req[:50]}"
    assert not bad_safe, f"contracts SAFE_MODE_KEYS contains keys not in ENV_KEYS: {bad_safe[:50]}"

    compiled: list[re.Pattern[str]] = []
    for p in patterns_raw:
        if isinstance(p, re.Pattern):
            compiled.append(p)
        else:
            raise AssertionError(
                "contracts EXTERNAL_ENV_ALLOW_PATTERNS must be a tuple of compiled regex Patterns."
            )

    return ContractSpec(
        env_keys=set(env_keys),
        required_keys=set(required),
        safe_mode_keys=set(safe_keys),
        external_allowlist=set(allowlist),
        external_allow_patterns=tuple(compiled),
    )


def _is_external_env_key(k: str, spec: ContractSpec) -> bool:
    if k in spec.external_allowlist:
        return True
    return any(p.search(k) for p in spec.external_allow_patterns)


# -----------------------------------------------------------------------------
# Code scan for env usage (actionable: show where keys are used)
# -----------------------------------------------------------------------------
def _iter_code_paths(root: Path) -> list[Path]:
    """
    Scan project code, not tests/venv/build artifacts.
    Includes:
      - root-level .py files
      - known source directories (flat + src/)
    """
    out: list[Path] = []
    for f in root.glob("*.py"):
        if f.is_file():
            out.append(f)

    for name in ("utils_pkg", "scanner", "sniper", "scripts"):
        p = root / name
        if p.is_dir():
            out.append(p)

    src = root / "src"
    if src.is_dir():
        for name in ("utils_pkg", "scanner", "sniper", "scripts"):
            p = src / name
            if p.is_dir():
                out.append(p)

    return out


def _is_ignored_path(p: Path) -> bool:
    parts = set(p.parts)
    if "tests" in parts:
        return True
    ignored = {
        ".venv",
        "venv",
        "env",
        "build",
        "dist",
        ".pytest_cache",
        ".mypy_cache",
        "__pycache__",
        ".git",
        "node_modules",
    }
    return any(x in parts for x in ignored)


class _EnvUseVisitor(ast.NodeVisitor):
    """
    Captures literal-string env key usage for:
      - os.getenv("KEY", ...)
      - os.environ.get("KEY", ...)
      - os.environ.setdefault("KEY", ...)
      - os.environ.pop("KEY", ...)
      - os.environ["KEY"] (read/write)
    """

    def __init__(self, rel_path: str, src_lines: list[str]) -> None:
        self.rel_path = rel_path
        self._lines = src_lines
        self.refs: dict[str, list[str]] = {}

    @staticmethod
    def _const_str(node: ast.AST | None) -> str | None:
        if node is None:
            return None
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    def _line_snippet(self, node: ast.AST) -> str:
        ln = int(getattr(node, "lineno", 0) or 0)
        if ln <= 0 or ln > len(self._lines):
            return ""
        s = self._lines[ln - 1].strip()
        if len(s) > 240:
            return s[:240] + "…"
        return s

    def _add(self, key: str, node: ast.AST) -> None:
        k = (key or "").strip()
        if not k:
            return
        ln = int(getattr(node, "lineno", 0) or 0)
        where = f"{self.rel_path}:{ln}: {self._line_snippet(node)}"
        self.refs.setdefault(k, []).append(where)

    def _is_os_environ(self, node: ast.AST) -> bool:
        # os.environ
        return (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "os"
            and node.attr == "environ"
        )

    def visit_Call(self, node: ast.Call) -> Any:
        # os.getenv("KEY", ...)
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            if node.func.value.id == "os" and node.func.attr == "getenv" and node.args:
                k = self._const_str(node.args[0])
                if k:
                    self._add(k, node)

        # os.environ.get/setdefault/pop("KEY", ...)
        if isinstance(node.func, ast.Attribute) and self._is_os_environ(node.func.value):
            if node.func.attr in {"get", "setdefault", "pop"} and node.args:
                k = self._const_str(node.args[0])
                if k:
                    self._add(k, node)

        return self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> Any:
        # os.environ["KEY"]
        if self._is_os_environ(node.value):
            sl = node.slice
            k: str | None = None
            if isinstance(sl, ast.Constant) and isinstance(sl.value, str):
                k = sl.value
            elif hasattr(ast, "Index") and isinstance(sl, ast.Index):  # pragma: no cover (py<3.9)
                k = self._const_str(sl.value)
            if k:
                self._add(k, node)
        return self.generic_visit(node)


def _scan_env_usage(root: Path) -> dict[str, list[str]]:
    """
    Return: key -> list of "path:line:source_line" references
    Only captures literal-string env key usage.
    """
    refs: dict[str, list[str]] = {}

    for base in _iter_code_paths(root):
        files = [base] if base.is_file() else list(base.rglob("*.py"))
        for f in files:
            if not f.is_file() or _is_ignored_path(f):
                continue

            try:
                text = f.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue

            try:
                rel = str(f.relative_to(root))
            except Exception:
                rel = str(f)

            lines = text.splitlines()

            try:
                tree = ast.parse(text, filename=rel)
            except Exception as e:
                raise AssertionError(
                    f"Python file failed to parse: {rel}\n{type(e).__name__}: {e}"
                ) from e

            vis = _EnvUseVisitor(rel, lines)
            vis.visit(tree)
            for k, items in vis.refs.items():
                refs.setdefault(k, []).extend(items)

    return refs


# -----------------------------------------------------------------------------
# Safe-mode helpers
# -----------------------------------------------------------------------------
def _unset_all_contract_keys(
    monkeypatch: pytest.MonkeyPatch, env_entries: dict[str, EnvExampleEntry]
) -> None:
    for k in env_entries:
        monkeypatch.delenv(k, raising=False)


def _force_safe_no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Force multiple toggles; safe even if some aren't used.
    """
    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")
    monkeypatch.setenv("SAFE_MODE", "1")
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("CI", "1")
    monkeypatch.setenv("TEST", "1")
    monkeypatch.setenv("PYTEST_MODE", "1")
    monkeypatch.setenv("BACKTEST_MODE", "1")

    monkeypatch.setenv("LIVE_TRADING", "0")
    monkeypatch.setenv("TRADING_ENABLED", "0")
    monkeypatch.setenv("ALLOW_BUYS", "0")
    monkeypatch.setenv("ALLOW_SELLS", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "1")

    # Avoid dotenv autoload at import-time if supported.
    monkeypatch.setenv("CORE_LOAD_DOTENV", "0")


def _enable_one_safe_toggle(
    monkeypatch: pytest.MonkeyPatch,
    spec: ContractSpec,
    env_entries: dict[str, EnvExampleEntry],
) -> str:
    """
    Prefer safe keys from canonical spec, but fall back to keys present in .env.example.
    """
    preferred = [
        "SAFE_MODE",
        "NO_NETWORK",
        "DISABLE_NETWORK",
        "DRYRUN",
        "DRY_RUN",
        "PAPER_TRADING",
        "SIM_MODE",
        "PYTEST_MODE",
        "BACKTEST_MODE",
        "CI",
        "TEST",
    ]

    for k in preferred:
        if k in env_entries:
            monkeypatch.setenv(k, "1")
            return k

    for k in sorted(spec.safe_mode_keys):
        if k in env_entries:
            monkeypatch.setenv(k, "1")
            return k

    for k in env_entries:
        if _SAFE_TOGGLE_KEY_RE.search(k):
            monkeypatch.setenv(k, "1")
            return k

    raise AssertionError(
        "No safe-mode toggle key found.\n"
        "Add one to .env.example (and keep it in utils_pkg.contracts), e.g. SAFE_MODE=1."
    )


@contextlib.contextmanager
def _fresh_import(module_name: str):
    """
    Ensure we import modules under the current monkeypatched env.
    """
    for name in list(sys.modules.keys()):
        if name == module_name or name.startswith(module_name + "."):
            sys.modules.pop(name, None)
    yield importlib.import_module(module_name)


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
def test_env_example_exists_and_is_parseable() -> None:
    root = _repo_root()
    env_path = root / ".env.example"
    assert env_path.exists(), f"Missing .env.example at repo root: expected {env_path}"

    entries = _parse_env_example(env_path)
    assert entries, ".env.example parsed zero keys — add your env contract keys."

    bad = [k for k in entries if not _UPPER_KEY_RE.match(k)]
    assert not bad, (
        f"Non-standard env keys detected. Use UPPER_SNAKE_CASE ONLY.\nBad keys (sample): {bad[:50]}"
    )


def test_contract_module_exists_and_exports_keys() -> None:
    root = _repo_root()
    spec = _load_contract(root)

    bad = [k for k in spec.env_keys if not _UPPER_KEY_RE.match(k)]
    assert not bad, f"contracts ENV keys must be UPPER_SNAKE_CASE. Bad (sample): {bad[:50]}"

    assert spec.safe_mode_keys, (
        "Canonical contract must define SAFE_MODE_KEYS (at least one).\n"
        "Add SAFE_MODE / NO_NETWORK / DISABLE_NETWORK / DRYRUN etc. to .env.example and contracts."
    )

    assert isinstance(spec.external_allowlist, set)
    assert isinstance(spec.external_allow_patterns, tuple)
    for p in spec.external_allow_patterns:
        assert isinstance(p, re.Pattern)


def test_env_example_matches_canonical_contract() -> None:
    """
    .env.example must match utils_pkg.contracts exactly (LOCK-IN).
    """
    root = _repo_root()
    entries = _parse_env_example(root / ".env.example")
    env_keys = set(entries.keys())

    spec = _load_contract(root)
    contract_keys = set(spec.env_keys)

    missing_in_env_example = sorted(contract_keys - env_keys)
    unknown_in_env_example = sorted(env_keys - contract_keys)

    assert not missing_in_env_example, (
        "Your .env.example is missing canonical contract keys.\n"
        "Add these keys (blank/defaults are fine):\n"
        + "\n".join(f" - {k}" for k in missing_in_env_example[:200])
        + ("\n... (truncated)" if len(missing_in_env_example) > 200 else "")
    )

    assert not unknown_in_env_example, (
        "Your .env.example contains keys not present in the canonical contract.\n"
        "Either add them to utils_pkg.contracts, or remove them from .env.example.\n"
        + "\n".join(f" - {k}" for k in unknown_in_env_example[:200])
        + ("\n... (truncated)" if len(unknown_in_env_example) > 200 else "")
    )


def test_code_env_usage_is_subset_of_contract() -> None:
    """
    If code references ENV_KEY via os.getenv/os.environ.get/os.environ[...]/setdefault/pop,
    that key MUST be in utils_pkg.contracts (and therefore in .env.example),
    unless it is explicitly allowlisted as an external env key.
    """
    root = _repo_root()
    spec = _load_contract(root)

    refs = _scan_env_usage(root)
    used_keys = set(refs.keys())

    non_upper = sorted(k for k in used_keys if not _UPPER_KEY_RE.match(k))
    assert not non_upper, (
        "Your code uses non-UPPER env keys. Standardize to UPPER_SNAKE_CASE.\n"
        f"Bad keys (sample): {non_upper[:50]}"
    )

    missing = sorted(k for k in (used_keys - spec.env_keys) if not _is_external_env_key(k, spec))
    assert not missing, (
        "Your code references env vars NOT in the canonical contract.\n"
        "Fix: add them to utils_pkg.contracts (and .env.example), or remove the usage.\n\n"
        + "\n\n".join(
            f"{k}\n" + "\n".join(f"  - {w}" for w in refs.get(k, [])[:10]) for k in missing[:80]
        )
        + ("\n\n... (truncated)" if len(missing) > 80 else "")
    )


def test_import_core_utils_in_safe_mode_is_lazy_and_offline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    In safe mode, importing core_utils should not crash and should not eagerly create
    network clients/sessions or background tasks. (No I/O at import-time.)
    """
    root = _repo_root()
    env_entries = _parse_env_example(root / ".env.example")
    spec = _load_contract(root)

    _unset_all_contract_keys(monkeypatch, env_entries)
    safe_key = _enable_one_safe_toggle(monkeypatch, spec, env_entries)
    _force_safe_no_network(monkeypatch)

    _ensure_import_paths(root)

    # Fresh import so env toggles apply
    with _fresh_import("utils_pkg.core_utils") as mod:
        assert mod is not None

    # If these are part of our lock-in contract, enforce them strictly.
    assert hasattr(mod, "_aiohttp_session"), (
        "LOCK-IN: core_utils must define _aiohttp_session (lazy session handle)."
    )
    assert getattr(mod, "_aiohttp_session", None) is None, (
        "core_utils created aiohttp session at import time. This must be lazy.\n"
        f"Safe toggle set: {safe_key}=1"
    )

    assert hasattr(mod, "_BACKGROUND_TASKS"), "LOCK-IN: core_utils must define _BACKGROUND_TASKS"
    bg = mod._BACKGROUND_TASKS
    assert isinstance(bg, set), "core_utils._BACKGROUND_TASKS must be a set"
    assert len(bg) == 0, "core_utils started background tasks at import time (must be explicit)"


def test_strict_required_hints_are_consistent(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    If you mark keys as 'required' in .env.example comments,
    they SHOULD exist in contracts REQUIRED_KEYS (LOCK-IN).

    Also, safe-mode import should still succeed with all keys unset.
    """
    root = _repo_root()
    env_entries = _parse_env_example(root / ".env.example")
    hinted_required = _required_keys_from_comments(env_entries)

    spec = _load_contract(root)
    if not hinted_required:
        pytest.skip(
            "No inline 'required' hints in .env.example; skipping strict hint consistency test."
        )

    missing_in_contract = sorted(hinted_required - spec.required_keys)
    assert not missing_in_contract, (
        "Keys are marked 'required' in .env.example comments but are not in contracts REQUIRED_KEYS.\n"
        "Update utils_pkg.contracts REQUIRED_KEYS to match.\n"
        + "\n".join(f" - {k}" for k in missing_in_contract)
    )

    # Even with all keys unset, safe-mode import must not crash.
    _unset_all_contract_keys(monkeypatch, env_entries)
    _force_safe_no_network(monkeypatch)
    _ensure_import_paths(root)

    with _fresh_import("utils_pkg.core_utils") as _mod:
        assert _mod is not None
