#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
deploy_tools/watchdog.py — V3+++ Watchdog (LOCK-IN FINAL, no-flap)
==================================================================

Purpose
-------
Watch a live logfile for "stuck loops" and repeated failure patterns, then take
safe, rate-limited actions:

  - restart (default)
  - rollback + restart (optional, if a backup is available)

Hard requirements (met)
-----------------------
- stdlib-only (NO external deps)
- Safe process control via PIDFILE (no pkill -f)
- Log tailing is rotation/truncation-safe
- Actions are rate-limited + protected by an exclusive lockfile
- Optional Telegram notifications (never required; NO hardcoded secrets)
- Render + local friendly paths
- "Never flaps": cooldown + action-window throttle + lock + dedupe + no-replay reopen

Environment variables (all optional)
------------------------------------

Paths:
  RUN_DIR=<repo>/.run
  WATCHDOG_LOGFILE=/tmp/sniper_live.out               (or .run/sniper_live.out on Windows)
  WATCHDOG_PIDFILE=<repo>/.run/sniper.pid
  WATCHDOG_OUTFILE=<repo>/.run/sniper_live.out

Start:
  WATCHDOG_START_SH=<repo>/start.sh                   (default runner)
  WATCHDOG_START_CMD="bash -lc './start.sh'"          (optional override; string command)
  WATCHDOG_START_CMD_MODE=auto|argv|shell             (default: auto)
    - auto: JSON list => argv, shell metachars => shell, else argv
    - argv: interpret WATCHDOG_START_CMD as argv (no shell operators)
    - shell: run via bash -lc (Linux) / cmd.exe /c (Windows)

Rollback:
  WATCHDOG_BACKUP=<path to env backup for rollback>   (optional; if unset, autodetect .env.bak.*)
  WATCHDOG_ROLLBACK_SH=<repo>/deploy_tools/rollback.sh

Behavior:
  WATCHDOG_DRYRUN=1
  WATCHDOG_COOLDOWN_S=120
  WATCHDOG_MAX_ACTIONS=3
  WATCHDOG_MAX_ACTIONS_WINDOW_S=900

  WATCHDOG_POST_ACTION_GRACE_S=5                      (avoid instant re-trigger after a real action)

  WATCHDOG_STALL_S=300
  WATCHDOG_PROGRESS_REGEX=<regex>                     (default provided)

  WATCHDOG_ERROR_RULES=<semicolon separated rules>
    Example:
      "JUP_BREAKER_RESET|rollback|3|60;ROUTEGUARD_RESET|restart|2|40"
    Format per rule:
      <regex>|<action>|<count>|<seconds>
    action: restart | rollback

  WATCHDOG_POLL_S=0.25
  WATCHDOG_LOGLEVEL=INFO

  WATCHDOG_STATUS_EVERY_S=30                          (writes run_dir/watchdog.status.json)
  WATCHDOG_STOP_FILE=<run_dir>/watchdog.stop          (touch to stop)

No-flap tail behavior:
  WATCHDOG_REPLAY_ON_REOPEN=0                         (default: 0 => seek end on reopen)
  WATCHDOG_DEDUPE_LINES=1                             (default: 1 => skip repeated lines)

Telegram (optional):
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_CHAT_ID=...

Run:
  python deploy_tools/watchdog.py

Notes
-----
- Designed for Render/Linux but remains import/type-safe on Windows.
- Never logs tokens/DSNs/credentials.
"""

import contextlib
import errno
import hashlib
import json
import logging
import os
import pathlib
import re
import shlex
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from collections import deque
from dataclasses import dataclass
from typing import IO, Any, Final


# ──────────────────────────────────────────────────────────────────────────────
# Logging (UTC)
# ──────────────────────────────────────────────────────────────────────────────
class _UTCFormatter(logging.Formatter):
    converter = staticmethod(time.gmtime)


def _setup_logging() -> logging.Logger:
    level_name = (os.getenv("WATCHDOG_LOGLEVEL", "INFO") or "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(
        _UTCFormatter(
            fmt="[watchdog] %(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
    )

    logger = logging.getLogger("watchdog")
    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    return logger


LOG = _setup_logging()

_SIGTERM: int | None = getattr(signal, "SIGTERM", None)
_SIGKILL: int | None = getattr(signal, "SIGKILL", None)

_TRUE: Final[set[str]] = {"1", "true", "yes", "y", "on", "t"}


def _utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_str(x: object, max_len: int = 240) -> str:
    try:
        s = str(x)
    except Exception:
        return "<?>"
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in _TRUE


def _get_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        return int(float(raw))
    except ValueError:
        LOG.warning("Invalid int env %s=%r; using default=%s", name, raw, default)
        return int(default)


def _get_float(name: str, default: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except ValueError:
        LOG.warning("Invalid float env %s=%r; using default=%s", name, raw, default)
        return float(default)


def _resolve_path(repo_root: pathlib.Path, raw: str) -> pathlib.Path:
    p = pathlib.Path(raw)
    if not p.is_absolute():
        return (repo_root / p).resolve()
    return p


def _atomic_write_text(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _looks_like_shell(s: str) -> bool:
    # Shell metachars that should not be passed as argv tokens to Popen(shell=False)
    # This catches your exact use case: "python -m scripts.preflight && bash ./start.sh"
    return any(x in s for x in ("&&", "||", "|", ";", ">", "<", "$(", "`"))


def _redact_tokenish(s: str) -> str:
    # Very small redaction to avoid accidental secret echo in logs.
    # Do NOT attempt full parsing; just mask obvious KEY=VALUE patterns.
    pat = re.compile(r"(?i)\b([A-Z0-9_]*(TOKEN|SECRET|PASSWORD|KEY|DSN)[A-Z0-9_]*)=([^\s]+)")
    return pat.sub(lambda m: f"{m.group(1)}=***", s)


def _split_cmd_argv(spec: str) -> list[str]:
    """
    Split a command string into argv tokens (shell=False always).
    On Windows, shlex.split(posix=False) behaves more predictably.
    """
    spec = (spec or "").strip()
    if not spec:
        return []
    try:
        parts = shlex.split(spec, posix=(os.name != "nt"))
        return [p for p in parts if p]
    except Exception:
        return [spec]


def _wrap_shell_cmd(spec: str) -> list[str]:
    spec = (spec or "").strip()
    if os.name == "nt":
        return ["cmd.exe", "/c", spec]
    return ["bash", "-lc", spec]


# ──────────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Rule:
    pattern: re.Pattern[str]
    action: str  # "restart" | "rollback"
    count: int
    window_s: int


@dataclass(frozen=True)
class StartCommand:
    mode: str  # argv | shell
    argv: list[str]


@dataclass(frozen=True)
class Config:
    repo_root: pathlib.Path
    run_dir: pathlib.Path

    logfile: pathlib.Path
    pidfile: pathlib.Path
    outfile: pathlib.Path

    start_sh: pathlib.Path
    start_cmd: StartCommand | None

    rollback_sh: pathlib.Path
    backup_path: pathlib.Path | None

    dryrun: bool
    cooldown_s: int
    post_action_grace_s: int

    max_actions: int
    max_actions_window_s: int

    stall_s: int
    progress_regex: re.Pattern[str]
    rules: list[Rule]

    poll_s: float
    status_every_s: int

    stop_file: pathlib.Path

    replay_on_reopen: bool
    dedupe_lines: bool

    telegram_bot_token: str
    telegram_chat_id: str


_DEFAULT_RULES: Final[list[tuple[str, str, int, int]]] = [
    (r"JUP_BREAKER_RESET", "rollback", 3, 60),
    (r"ROUTEGUARD_RESET|ROUTEGUARD_MAX_IMPACT_BPS", "restart", 2, 40),
    (r"\bquote\b.*\berror\b|QuoteError|no route", "restart", 5, 120),
]


def _compile_rule(rx: str) -> re.Pattern[str]:
    return re.compile(rx, re.IGNORECASE)


def _parse_rules(spec: str) -> list[Rule]:
    """
    WATCHDOG_ERROR_RULES format:
      "REGEX|ACTION|COUNT|SECONDS;REGEX|ACTION|COUNT|SECONDS;..."
    """
    spec = (spec or "").strip()
    if not spec:
        return [
            Rule(_compile_rule(rx), act, int(n), int(sec)) for rx, act, n, sec in _DEFAULT_RULES
        ]

    rules: list[Rule] = []
    for chunk in spec.split(";"):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = [p.strip() for p in chunk.split("|")]
        if len(parts) != 4:
            raise ValueError(f"Bad WATCHDOG_ERROR_RULES rule: {chunk!r}")
        rx, act, n, sec = parts
        act = act.lower().strip()
        if act not in {"restart", "rollback"}:
            raise ValueError(f"Rule action must be restart/rollback, got: {act!r}")
        rules.append(Rule(_compile_rule(rx), act, int(n), int(sec)))
    return rules


def _parse_start_cmd(raw: str) -> StartCommand | None:
    """
    WATCHDOG_START_CMD supports:
      - JSON list: ["bash","-lc","./start.sh"]  (always argv mode)
      - plain string:
          mode=auto: shell metachars => shell, else argv
          mode=argv: always argv
          mode=shell: always shell
    """
    raw = (raw or "").strip()
    if not raw:
        return None

    mode = (os.getenv("WATCHDOG_START_CMD_MODE", "auto") or "auto").strip().lower()
    if mode not in {"auto", "argv", "shell"}:
        LOG.warning("Invalid WATCHDOG_START_CMD_MODE=%r; using auto", mode)
        mode = "auto"

    if raw.startswith("["):
        try:
            obj = json.loads(raw)
            if isinstance(obj, list) and all(isinstance(x, str) and x for x in obj):
                return StartCommand(mode="argv", argv=[x for x in obj if x])
        except Exception as e:
            LOG.warning(
                "WATCHDOG_START_CMD JSON parse failed; treating as string (%s)", _safe_str(e)
            )

    if mode == "shell":
        return StartCommand(mode="shell", argv=_wrap_shell_cmd(raw))
    if mode == "argv":
        argv = _split_cmd_argv(raw)
        return StartCommand(mode="argv", argv=argv)

    # auto
    if _looks_like_shell(raw):
        return StartCommand(mode="shell", argv=_wrap_shell_cmd(raw))
    return StartCommand(mode="argv", argv=_split_cmd_argv(raw))


def load_config() -> Config:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    run_dir = _resolve_path(repo_root, os.getenv("RUN_DIR", str(repo_root / ".run")))

    default_logfile = (
        "/tmp/sniper_live.out" if os.name != "nt" else str(run_dir / "sniper_live.out")
    )
    logfile = _resolve_path(repo_root, os.getenv("WATCHDOG_LOGFILE", default_logfile))
    pidfile = _resolve_path(repo_root, os.getenv("WATCHDOG_PIDFILE", str(run_dir / "sniper.pid")))
    outfile = _resolve_path(
        repo_root, os.getenv("WATCHDOG_OUTFILE", str(run_dir / "sniper_live.out"))
    )
    start_sh = _resolve_path(repo_root, os.getenv("WATCHDOG_START_SH", str(repo_root / "start.sh")))

    start_cmd_raw = (os.getenv("WATCHDOG_START_CMD", "") or "").strip()
    start_cmd = _parse_start_cmd(start_cmd_raw)

    rollback_sh = _resolve_path(
        repo_root,
        os.getenv("WATCHDOG_ROLLBACK_SH", str(repo_root / "deploy_tools" / "rollback.sh")),
    )
    backup_raw = (os.getenv("WATCHDOG_BACKUP", "") or "").strip()
    backup_path = _resolve_path(repo_root, backup_raw) if backup_raw else None

    dryrun = _truthy(os.getenv("WATCHDOG_DRYRUN", "0"))
    cooldown_s = max(1, _get_int("WATCHDOG_COOLDOWN_S", 120))
    post_action_grace_s = max(0, _get_int("WATCHDOG_POST_ACTION_GRACE_S", 5))

    max_actions = max(1, _get_int("WATCHDOG_MAX_ACTIONS", 3))
    max_actions_window_s = max(10, _get_int("WATCHDOG_MAX_ACTIONS_WINDOW_S", 900))

    stall_s = max(15, _get_int("WATCHDOG_STALL_S", 300))

    progress_spec = (os.getenv("WATCHDOG_PROGRESS_REGEX", "") or "").strip()
    if not progress_spec:
        progress_spec = r"scanner|SCAN|candidate|on_new_token|on_trade|heartbeat|tick|loop|processed|RAY_META|PF_"
    try:
        progress_regex = re.compile(progress_spec, re.IGNORECASE)
    except re.error:
        LOG.warning("Invalid WATCHDOG_PROGRESS_REGEX; falling back to default")
        progress_regex = re.compile(
            r"scanner|SCAN|candidate|on_new_token|on_trade|heartbeat|tick|loop|processed|RAY_META|PF_",
            re.IGNORECASE,
        )

    try:
        rules = _parse_rules(os.getenv("WATCHDOG_ERROR_RULES", ""))
    except Exception as e:
        LOG.warning("Invalid WATCHDOG_ERROR_RULES; using defaults (%s)", _safe_str(e))
        rules = [
            Rule(_compile_rule(rx), act, int(n), int(sec)) for rx, act, n, sec in _DEFAULT_RULES
        ]

    poll_s = _get_float("WATCHDOG_POLL_S", 0.25)
    poll_s = max(0.05, min(5.0, poll_s))

    status_every_s = max(5, _get_int("WATCHDOG_STATUS_EVERY_S", 30))
    stop_file = _resolve_path(
        repo_root, os.getenv("WATCHDOG_STOP_FILE", str(run_dir / "watchdog.stop"))
    )

    replay_on_reopen = _truthy(os.getenv("WATCHDOG_REPLAY_ON_REOPEN", "0"))
    dedupe_lines = (os.getenv("WATCHDOG_DEDUPE_LINES", "1") or "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }

    tg_token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    tg_chat = (os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()

    return Config(
        repo_root=repo_root,
        run_dir=run_dir,
        logfile=logfile,
        pidfile=pidfile,
        outfile=outfile,
        start_sh=start_sh,
        start_cmd=start_cmd,
        rollback_sh=rollback_sh,
        backup_path=backup_path,
        dryrun=dryrun,
        cooldown_s=cooldown_s,
        post_action_grace_s=post_action_grace_s,
        max_actions=max_actions,
        max_actions_window_s=max_actions_window_s,
        stall_s=stall_s,
        progress_regex=progress_regex,
        rules=rules,
        poll_s=poll_s,
        status_every_s=status_every_s,
        stop_file=stop_file,
        replay_on_reopen=replay_on_reopen,
        dedupe_lines=dedupe_lines,
        telegram_bot_token=tg_token,
        telegram_chat_id=tg_chat,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Telegram (optional) — urllib (stdlib), never required
# ──────────────────────────────────────────────────────────────────────────────
def tg_send(cfg: Config, msg: str) -> None:
    if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        return
    with contextlib.suppress(Exception):
        url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": cfg.telegram_chat_id, "text": msg}).encode(
            "utf-8"
        )
        req = urllib.request.Request(url=url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310
            _ = resp.read(64)


# ──────────────────────────────────────────────────────────────────────────────
# PIDFILE-based process control
# ──────────────────────────────────────────────────────────────────────────────
def read_pid(pidfile: pathlib.Path) -> int | None:
    try:
        raw = pidfile.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        pid = int(raw)
        return pid if pid > 0 else None
    except Exception:
        return None


def pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


def _kill_pid(pid: int, sig: int) -> None:
    with contextlib.suppress(Exception):
        os.kill(pid, sig)


def _kill_process_group(pid: int, sig: int) -> bool:
    """
    On unix, if possible, kill the process group for pid.
    Must stay MyPy-safe on Windows.
    """
    if os.name == "nt":
        return False
    getpgid = getattr(os, "getpgid", None)
    killpg = getattr(os, "killpg", None)
    if not callable(getpgid) or not callable(killpg):
        return False
    with contextlib.suppress(Exception):
        pgid = int(getpgid(pid))
        killpg(pgid, sig)
        return True
    return False


def stop_via_pidfile(pidfile: pathlib.Path, timeout_s: float = 3.0) -> None:
    pid = read_pid(pidfile)
    if not pid:
        return

    # Safety: never kill ourselves if pidfile is wrong.
    if pid == os.getpid():
        LOG.error("PIDFILE points to watchdog pid=%s; refusing to kill self", pid)
        return

    if not pid_is_alive(pid):
        with contextlib.suppress(Exception):
            pidfile.unlink()
        return

    LOG.warning("Stopping existing process pid=%s (pidfile=%s)", pid, pidfile)

    if _SIGTERM is not None:
        if not _kill_process_group(pid, _SIGTERM):
            _kill_pid(pid, _SIGTERM)

    t0 = time.monotonic()
    while (time.monotonic() - t0) < timeout_s:
        if not pid_is_alive(pid):
            break
        time.sleep(0.15)

    if pid_is_alive(pid):
        LOG.error("Process did not stop; forcing termination pid=%s", pid)
        if os.name == "nt":
            with contextlib.suppress(Exception):
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
        else:
            sigkill = _SIGKILL if _SIGKILL is not None else _SIGTERM
            if sigkill is not None:
                attempted = _kill_process_group(pid, sigkill)
                if not attempted:
                    _kill_pid(pid, sigkill)

    with contextlib.suppress(Exception):
        pidfile.unlink()


def _default_start_cmd_from_sh(start_sh: pathlib.Path) -> list[str]:
    if os.name == "nt":
        return [str(start_sh)]
    return ["bash", str(start_sh)]


def _popen_creationflags() -> int:
    if os.name != "nt":
        return 0
    return int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) or 0)


def start_background(cfg: Config) -> int:
    """
    Starts background runner and writes pidfile.
    Uses WATCHDOG_START_CMD if set; otherwise uses WATCHDOG_START_SH.
    """
    cfg.outfile.parent.mkdir(parents=True, exist_ok=True)
    cfg.pidfile.parent.mkdir(parents=True, exist_ok=True)

    if cfg.start_cmd is not None:
        cmd = list(cfg.start_cmd.argv)
        cwd = str(cfg.repo_root)
        mode = cfg.start_cmd.mode
        shown = _redact_tokenish(" ".join(cmd) if mode == "argv" else str(cmd[-1] if cmd else ""))
        LOG.info("Start: using WATCHDOG_START_CMD mode=%s cmd=%s", mode, shown)
        if not cmd:
            raise ValueError("WATCHDOG_START_CMD resolved to empty argv")
    else:
        if not cfg.start_sh.exists():
            raise FileNotFoundError(f"start script not found at {cfg.start_sh}")
        cmd = _default_start_cmd_from_sh(cfg.start_sh)
        cwd = str(cfg.start_sh.parent)
        LOG.info("Start: using WATCHDOG_START_SH=%s", cfg.start_sh)

    popen_stdout: IO[str]
    with open(cfg.outfile, "a", encoding="utf-8") as popen_stdout:
        popen_kwargs: dict[str, Any] = {
            "stdout": popen_stdout,
            "stderr": popen_stdout,
            "cwd": cwd,
            "env": os.environ.copy(),
            "text": True,
            "close_fds": (os.name != "nt"),
        }
        if os.name != "nt":
            popen_kwargs["start_new_session"] = True
        else:
            flags = _popen_creationflags()
            if flags:
                popen_kwargs["creationflags"] = flags

        p = subprocess.Popen(cmd, **popen_kwargs)

    _atomic_write_text(cfg.pidfile, f"{p.pid}\n")
    return int(p.pid)


# ──────────────────────────────────────────────────────────────────────────────
# Locking for actions
# ──────────────────────────────────────────────────────────────────────────────
class ActionLock:
    """
    Lockfile using O_EXCL to prevent concurrent restarts/rollbacks.
    If lock exists and is stale, remove it after stale_after_s.
    """

    def __init__(self, path: pathlib.Path, stale_after_s: int) -> None:
        self.path = path
        self.stale_after_s = int(stale_after_s)
        self.fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        if self.path.exists():
            with contextlib.suppress(Exception):
                age = time.time() - self.path.stat().st_mtime
                if age > float(self.stale_after_s):
                    LOG.warning("Removing stale action lock age_s=%.1f path=%s", age, self.path)
                    self.path.unlink()

        try:
            self.fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.write(self.fd, f"ts={_utc_ts()} pid={os.getpid()}\n".encode("utf-8"))
            return True
        except OSError as err:
            if err.errno == errno.EEXIST:
                return False
            raise

    def release(self) -> None:
        with contextlib.suppress(Exception):
            if self.fd is not None:
                os.close(self.fd)
        with contextlib.suppress(Exception):
            if self.path.exists():
                self.path.unlink()


# ──────────────────────────────────────────────────────────────────────────────
# Log tailer (rotation/truncation-safe, no-replay reopen by default)
# ──────────────────────────────────────────────────────────────────────────────
class Tailer:
    """
    Tails a file safely across rotation/truncation.

    - Initial open: seeks to end (watch new lines only; avoids historical replays)
    - Reopen after rotation/truncation:
        - default: seeks to end (prevents replay/flap)
        - if replay_on_reopen: seeks to beginning
    """

    def __init__(self, path: pathlib.Path, *, replay_on_reopen: bool) -> None:
        self.path = path
        self.replay_on_reopen = replay_on_reopen
        self._f: IO[str] | None = None
        self._inode: tuple[int, int] | None = None  # (ino, dev) when available
        self._pos: int = 0
        self._opened_once = False

    def _open(self, *, start_at_end: bool) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        f = open(self.path, encoding="utf-8", errors="replace")
        st = os.fstat(f.fileno())
        ino = int(getattr(st, "st_ino", 0) or 0)
        dev = int(getattr(st, "st_dev", 0) or 0)
        self._inode = (ino, dev)
        f.seek(0, os.SEEK_END if start_at_end else os.SEEK_SET)
        self._pos = int(f.tell())
        self._f = f

    def _close(self) -> None:
        if self._f is None:
            return
        with contextlib.suppress(Exception):
            self._f.close()
        self._f = None
        self._inode = None
        self._pos = 0

    def _needs_reopen(self) -> bool:
        try:
            st = self.path.stat()
            cur_inode = (int(getattr(st, "st_ino", 0) or 0), int(getattr(st, "st_dev", 0) or 0))
            if self._inode is not None and cur_inode != self._inode:
                return True  # rotated/replaced
            if int(st.st_size) < self._pos:
                return True  # truncated
            return False
        except FileNotFoundError:
            return True
        except Exception:
            return False

    def readline(self) -> str | None:
        if self._f is None:
            if not self.path.exists():
                return None
            # Initial open always seeks to end (avoid historical replay).
            self._open(start_at_end=True)
            self._opened_once = True

        if self._needs_reopen():
            self._close()
            if not self.path.exists():
                return None
            # Reopen behavior: default end (no replay), optional start.
            self._open(start_at_end=not self.replay_on_reopen)
            return None

        assert self._f is not None
        line = self._f.readline()
        if not line:
            return None

        self._pos = int(self._f.tell())
        return line.rstrip("\n")


# ──────────────────────────────────────────────────────────────────────────────
# No-flap line dedupe
# ──────────────────────────────────────────────────────────────────────────────
class LineDedupe:
    def __init__(self, *, enabled: bool, max_items: int = 4096) -> None:
        self.enabled = enabled
        self.max_items = max(256, int(max_items))
        self._q: deque[bytes] = deque()
        self._set: set[bytes] = set()

    def _h(self, s: str) -> bytes:
        return hashlib.sha256(s.encode("utf-8", "ignore")).digest()

    def seen(self, line: str) -> bool:
        if not self.enabled:
            return False
        h = self._h(line)
        if h in self._set:
            return True
        self._q.append(h)
        self._set.add(h)
        while len(self._q) > self.max_items:
            old = self._q.popleft()
            self._set.discard(old)
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Policy / state
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class Event:
    ts: float
    line: str


def find_latest_env_backup(repo_root: pathlib.Path) -> pathlib.Path | None:
    candidates = list(repo_root.glob(".env.bak.*"))
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _prune_times(q: deque[float], window_s: int, *, now: float) -> None:
    while q and (now - q[0]) > float(window_s):
        q.popleft()


def _too_many_actions(action_times: deque[float], cfg: Config, *, now: float) -> bool:
    _prune_times(action_times, cfg.max_actions_window_s, now=now)
    return len(action_times) >= cfg.max_actions


def _dump_tail(run_dir: pathlib.Path, window: deque[Event], reason: str) -> None:
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "watchdog.last_tail.json"
        payload = {
            "ts": _utc_ts(),
            "reason": reason,
            "lines": [e.line for e in list(window)[-200:]],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception:
        return


def _write_status(cfg: Config, *, state: dict[str, Any]) -> None:
    """
    Writes a small JSON snapshot for ops visibility.
    Must never include secrets.
    """
    try:
        cfg.run_dir.mkdir(parents=True, exist_ok=True)
        path = cfg.run_dir / "watchdog.status.json"
        payload = {
            "ts": _utc_ts(),
            "logfile": str(cfg.logfile),
            "pidfile": str(cfg.pidfile),
            "outfile": str(cfg.outfile),
            "dryrun": int(cfg.dryrun),
            "cooldown_s": int(cfg.cooldown_s),
            "post_action_grace_s": int(cfg.post_action_grace_s),
            "stall_s": int(cfg.stall_s),
            "poll_s": float(cfg.poll_s),
            "max_actions": int(cfg.max_actions),
            "max_actions_window_s": int(cfg.max_actions_window_s),
            "rules": len(cfg.rules),
            "progress_regex": cfg.progress_regex.pattern,
            "start_mode": ("cmd:" + (cfg.start_cmd.mode if cfg.start_cmd else ""))
            if cfg.start_cmd
            else "sh",
            "replay_on_reopen": int(cfg.replay_on_reopen),
            "dedupe_lines": int(cfg.dedupe_lines),
            "stop_file": str(cfg.stop_file),
            "state": state,
        }
        _atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    except Exception:
        return


# ──────────────────────────────────────────────────────────────────────────────
# Actions (lock-safe, rate-limited, no-flap)
# ──────────────────────────────────────────────────────────────────────────────
def _action_lock(cfg: Config) -> ActionLock:
    stale_after = max(600, cfg.cooldown_s * 3, cfg.stall_s)
    return ActionLock(cfg.run_dir / "watchdog.action.lock", stale_after_s=stale_after)


def _restart_unlocked(cfg: Config, reason: str) -> int:
    stop_via_pidfile(cfg.pidfile)
    pid = start_background(cfg)
    LOG.warning("Restarted pid=%s reason=%s", pid, reason)
    return int(pid)


def _run_rollback_script(cfg: Config, backup: pathlib.Path) -> None:
    """
    Run rollback script. On Render/Linux: bash rollback.sh <backup>
    """
    if os.name == "nt":
        subprocess.run([str(cfg.rollback_sh), str(backup)], check=False)
        return
    subprocess.run(["bash", str(cfg.rollback_sh), str(backup)], env=os.environ.copy(), check=False)


def _rollback_unlocked(cfg: Config, reason: str) -> None:
    backup = cfg.backup_path or find_latest_env_backup(cfg.repo_root)
    if backup is None or not backup.exists():
        LOG.error("No backup available for rollback; fallback to restart")
        _restart_unlocked(cfg, reason=f"{reason} (no backup)")
        return

    if not cfg.rollback_sh.exists():
        LOG.error("rollback.sh missing at %s; fallback to restart", cfg.rollback_sh)
        _restart_unlocked(cfg, reason=f"{reason} (rollback.sh missing)")
        return

    LOG.error("Calling rollback.sh (backup path redacted in logs)")
    _run_rollback_script(cfg, backup)


def do_restart(cfg: Config, reason: str, window: deque[Event]) -> bool:
    LOG.error("ACTION=restart reason=%s", reason)
    _dump_tail(cfg.run_dir, window, reason=f"restart: {reason}")
    tg_send(cfg, f"[WATCHDOG] restart: {reason}")

    if cfg.dryrun:
        LOG.warning("DRYRUN=1 -> not restarting")
        return False

    lock = _action_lock(cfg)
    if not lock.acquire():
        LOG.warning("Action lock already held; skipping restart")
        return False

    try:
        _restart_unlocked(cfg, reason=reason)
        return True
    finally:
        lock.release()


def do_rollback(cfg: Config, reason: str, window: deque[Event]) -> bool:
    LOG.error("ACTION=rollback reason=%s", reason)
    _dump_tail(cfg.run_dir, window, reason=f"rollback: {reason}")
    tg_send(cfg, f"[WATCHDOG] rollback: {reason}")

    if cfg.dryrun:
        LOG.warning("DRYRUN=1 -> not rolling back")
        return False

    lock = _action_lock(cfg)
    if not lock.acquire():
        LOG.warning("Action lock already held; skipping rollback")
        return False

    try:
        _rollback_unlocked(cfg, reason=reason)
        return True
    finally:
        lock.release()


# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────
def main() -> int:
    cfg = load_config()
    cfg.run_dir.mkdir(parents=True, exist_ok=True)

    stop_requested = False

    def _handle_stop(_sig: int, _frame: object | None) -> None:
        nonlocal stop_requested
        stop_requested = True

    with contextlib.suppress(Exception):
        if _SIGTERM is not None:
            signal.signal(_SIGTERM, _handle_stop)  # type: ignore[arg-type]
    with contextlib.suppress(Exception):
        signal.signal(signal.SIGINT, _handle_stop)

    LOG.info("repo_root=%s", cfg.repo_root)
    LOG.info("run_dir=%s", cfg.run_dir)
    LOG.info("logfile=%s", cfg.logfile)
    LOG.info("pidfile=%s", cfg.pidfile)
    LOG.info("outfile=%s", cfg.outfile)
    LOG.info("start=%s", "WATCHDOG_START_CMD" if cfg.start_cmd else "WATCHDOG_START_SH")
    LOG.info("rollback_sh=%s", cfg.rollback_sh)
    LOG.info("backup=%s", "(explicit)" if cfg.backup_path else "(auto)")
    LOG.info(
        "dryrun=%s cooldown_s=%s post_action_grace_s=%s stall_s=%s poll_s=%s max_actions=%s window_s=%s",
        cfg.dryrun,
        cfg.cooldown_s,
        cfg.post_action_grace_s,
        cfg.stall_s,
        cfg.poll_s,
        cfg.max_actions,
        cfg.max_actions_window_s,
    )
    LOG.info("tail: replay_on_reopen=%s dedupe_lines=%s", cfg.replay_on_reopen, cfg.dedupe_lines)

    tailer = Tailer(cfg.logfile, replay_on_reopen=cfg.replay_on_reopen)
    dedupe = LineDedupe(enabled=cfg.dedupe_lines)
    window: deque[Event] = deque(maxlen=5000)

    action_times: deque[float] = deque()
    last_action_at = 0.0
    last_action_kind = ""
    last_action_reason = ""

    rule_hits: list[deque[float]] = [deque() for _ in cfg.rules]
    last_progress_at: float | None = None

    last_status_write = 0.0
    ignore_until = 0.0  # post-action grace window to prevent immediate flap

    while not cfg.logfile.exists() and not stop_requested:
        LOG.warning("Waiting for logfile to appear: %s", cfg.logfile)
        time.sleep(1.0)

    if stop_requested:
        LOG.warning("Stop requested before logfile appeared; exit")
        return 0

    last_progress_at = time.monotonic()
    LOG.info("Watchdog running…")

    while not stop_requested:
        now_mono = time.monotonic()
        now_wall = time.time()

        if cfg.stop_file.exists():
            stop_requested = True
            break

        if (now_mono - last_status_write) >= float(cfg.status_every_s):
            last_status_write = now_mono
            pid = read_pid(cfg.pidfile)
            _write_status(
                cfg,
                state={
                    "pid": pid or 0,
                    "pid_alive": int(pid_is_alive(pid)) if pid else 0,
                    "last_progress_age_s": float(now_mono - last_progress_at)
                    if last_progress_at
                    else -1.0,
                    "actions_in_window": len(list(action_times)),
                    "cooldown_remaining_s": max(
                        0.0, float(cfg.cooldown_s) - (now_mono - last_action_at)
                    )
                    if last_action_at
                    else 0.0,
                    "ignore_until_s": max(0.0, ignore_until - now_mono),
                    "last_action_kind": last_action_kind,
                    "last_action_reason": last_action_reason,
                    "stop_requested": int(stop_requested),
                },
            )

        line = tailer.readline()

        if line is None:
            time.sleep(cfg.poll_s)
            now2 = time.monotonic()

            # PID dead => restart (rate-limited)
            pid = read_pid(cfg.pidfile)
            if pid and not pid_is_alive(pid):
                if (now2 - last_action_at) < float(cfg.cooldown_s):
                    continue
                if _too_many_actions(action_times, cfg, now=now2):
                    LOG.error("pid dead but throttled: too many actions in window")
                    continue

                action_times.append(now2)
                last_action_at = now2
                last_action_kind = "restart"
                last_action_reason = "pidfile process not alive"

                performed = do_restart(cfg, reason=last_action_reason, window=window)
                if performed:
                    # No-flap: reset progress timer + grace window after action
                    last_progress_at = now2
                    ignore_until = now2 + float(cfg.post_action_grace_s)
                    for hits in rule_hits:
                        hits.clear()
                continue

            # Stall detection (rate-limited)
            if last_progress_at is not None:
                age = now2 - last_progress_at
                if age >= float(cfg.stall_s):
                    if (now2 - last_action_at) < float(cfg.cooldown_s):
                        continue
                    if _too_many_actions(action_times, cfg, now=now2):
                        LOG.error("STALL detected but throttled. age_s=%.1f", age)
                        tg_send(cfg, f"[WATCHDOG] stall throttled: no progress for {int(age)}s")
                        continue

                    action_times.append(now2)
                    last_action_at = now2
                    last_action_kind = "restart"
                    last_action_reason = (
                        f"stall: no progress for {int(age)}s (regex={cfg.progress_regex.pattern})"
                    )

                    performed = do_restart(cfg, reason=last_action_reason, window=window)
                    if performed:
                        last_progress_at = now2
                        ignore_until = now2 + float(cfg.post_action_grace_s)
                        for hits in rule_hits:
                            hits.clear()

            continue

        # From here, `line` is a real line.
        if dedupe.seen(line):
            continue

        window.append(Event(ts=now_wall, line=line))

        if cfg.progress_regex.search(line):
            last_progress_at = time.monotonic()

        # Post-action grace: skip rule triggers briefly to avoid instant flap on startup chatter.
        if now_mono < ignore_until:
            continue

        triggered_action: str | None = None
        triggered_reason: str | None = None
        triggered_idx: int | None = None

        for idx, rule in enumerate(cfg.rules):
            if rule.pattern.search(line):
                hits = rule_hits[idx]
                hits.append(now_mono)
                _prune_times(hits, rule.window_s, now=now_mono)
                if len(hits) >= rule.count:
                    triggered_action = rule.action
                    triggered_reason = (
                        f"rule={rule.pattern.pattern} hits>={rule.count} in {rule.window_s}s"
                    )
                    triggered_idx = idx
                    break

        if triggered_action is None:
            continue

        # Cooldown + action window throttle
        if (now_mono - last_action_at) < float(cfg.cooldown_s):
            LOG.warning("Rule matched but in cooldown: %s", triggered_reason)
            continue

        if _too_many_actions(action_times, cfg, now=now_mono):
            LOG.error("Rule matched but throttled (too many actions): %s", triggered_reason)
            tg_send(cfg, f"[WATCHDOG] throttled: {triggered_reason}")
            continue

        action_times.append(now_mono)
        last_action_at = now_mono
        last_action_kind = triggered_action
        last_action_reason = triggered_reason or "rule"

        if triggered_idx is not None:
            rule_hits[triggered_idx].clear()

        performed = False
        if triggered_action == "rollback":
            performed = do_rollback(cfg, reason=last_action_reason, window=window)
        else:
            performed = do_restart(cfg, reason=last_action_reason, window=window)

        if performed:
            # No-flap: reset progress timer + grace window after action
            last_progress_at = now_mono
            ignore_until = now_mono + float(cfg.post_action_grace_s)
            for hits in rule_hits:
                hits.clear()

    LOG.warning("Stop requested; exit watchdog")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        LOG.warning("KeyboardInterrupt -> exit")
        raise SystemExit(0) from None
