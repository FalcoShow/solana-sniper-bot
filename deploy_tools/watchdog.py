#!/usr/bin/env python3
from __future__ import annotations

"""
deploy_tools/watchdog.py — V3+++ Watchdog (LOCK-IN)
===================================================

Goal:
  Detect "stuck loops" and repeated failure patterns, then take safe action:
    - restart (default)
    - rollback + restart (optional if a backup is available)

Key design points:
  - No external Python deps (stdlib only)
  - Safe process control via PIDFILE (no pkill -f)
  - Log tailing implemented in-process (handles rotation/truncation)
  - Actions are rate-limited + protected by a lock
  - Optional Telegram notifications (NO hardcoded secrets)
  - Render + local friendly paths:
        .run/sniper.pid
        /tmp/sniper_live.out (or .run/sniper_live.out)

Configuration (env vars — all optional):
  WATCHDOG_LOGFILE=/tmp/sniper_live.out
  WATCHDOG_PIDFILE=<repo>/.run/sniper.pid
  WATCHDOG_START_SH=<repo>/start.sh
  WATCHDOG_OUTFILE=<repo>/.run/sniper_live.out

  WATCHDOG_BACKUP=<path to env backup for rollback> (optional)
  WATCHDOG_ROLLBACK_SH=<repo>/deploy_tools/rollback.sh

  WATCHDOG_DRYRUN=1
  WATCHDOG_COOLDOWN_S=120
  WATCHDOG_MAX_ACTIONS=3
  WATCHDOG_MAX_ACTIONS_WINDOW_S=900

  WATCHDOG_STALL_S=300
  WATCHDOG_PROGRESS_REGEX=<regex>     (default is sane)

  WATCHDOG_ERROR_RULES=<semicolon separated rules>
    Example:
      "JUP_BREAKER_RESET|rollback|3|60;ROUTEGUARD_RESET|restart|2|40"
    Format per rule:
      <regex>|<action>|<count>|<seconds>
    action: restart | rollback

  WATCHDOG_POLL_S=0.25                (how often to poll when no new log lines)

Telegram (optional):
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_CHAT_ID=...

Run:
  python deploy_tools/watchdog.py
"""

import contextlib
import errno
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
from typing import Final


# ──────────────────────────────────────────────────────────────────────────────
# Logging (UTC)
# ──────────────────────────────────────────────────────────────────────────────
class _UTCFormatter(logging.Formatter):
    converter = time.gmtime


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

    # Avoid duplicate handlers if imported/run multiple times
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False
    return logger


LOG = _setup_logging()


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def _truthy(val: str | None) -> bool:
    return (val or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _get_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except ValueError:
        LOG.warning("Invalid int env %s=%r; using default=%s", name, raw, default)
        return default


def _get_float(name: str, default: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        LOG.warning("Invalid float env %s=%r; using default=%s", name, raw, default)
        return default


def _resolve_path(repo_root: pathlib.Path, raw: str) -> pathlib.Path:
    """
    If env gives a relative path, anchor it at repo_root for predictability.
    """
    p = pathlib.Path(raw)
    if not p.is_absolute():
        return (repo_root / p).resolve()
    return p


def _utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


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
class Config:
    repo_root: pathlib.Path
    run_dir: pathlib.Path

    logfile: pathlib.Path
    pidfile: pathlib.Path
    start_sh: pathlib.Path
    outfile: pathlib.Path

    rollback_sh: pathlib.Path
    backup_path: pathlib.Path | None

    dryrun: bool
    cooldown_s: int

    max_actions: int
    max_actions_window_s: int

    stall_s: int
    progress_regex: re.Pattern[str]

    rules: list[Rule]

    poll_s: float

    telegram_bot_token: str
    telegram_chat_id: str


_DEFAULT_RULES: Final[list[tuple[str, str, int, int]]] = [
    (r"JUP_BREAKER_RESET", "rollback", 3, 60),
    (r"ROUTEGUARD_RESET|ROUTEGUARD_MAX_IMPACT_BPS", "restart", 2, 40),
    (r"\bquote\b.*\berror\b|QuoteError|no route", "restart", 5, 120),
]


def _compile_rule(rx: str) -> re.Pattern[str]:
    # Use IGNORECASE for resilience against log casing.
    return re.compile(rx, re.IGNORECASE)


def _parse_rules(spec: str) -> list[Rule]:
    """
    WATCHDOG_ERROR_RULES format:
      "REGEX|ACTION|COUNT|SECONDS;REGEX|ACTION|COUNT|SECONDS;..."

    Example:
      "JUP_BREAKER_RESET|rollback|3|60;ROUTEGUARD_RESET|restart|2|40"
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
        act = act.lower()
        if act not in {"restart", "rollback"}:
            raise ValueError(f"Rule action must be restart/rollback, got: {act!r}")
        rules.append(Rule(_compile_rule(rx), act, int(n), int(sec)))
    return rules


def load_config() -> Config:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    run_dir = _resolve_path(repo_root, os.getenv("RUN_DIR", str(repo_root / ".run")))

    # Keep Render-friendly default (/tmp) but avoid nonsense on Windows.
    default_logfile = (
        "/tmp/sniper_live.out" if os.name != "nt" else str(run_dir / "sniper_live.out")
    )

    logfile = _resolve_path(repo_root, os.getenv("WATCHDOG_LOGFILE", default_logfile))
    pidfile = _resolve_path(repo_root, os.getenv("WATCHDOG_PIDFILE", str(run_dir / "sniper.pid")))
    start_sh = _resolve_path(repo_root, os.getenv("WATCHDOG_START_SH", str(repo_root / "start.sh")))
    outfile = _resolve_path(
        repo_root, os.getenv("WATCHDOG_OUTFILE", str(run_dir / "sniper_live.out"))
    )

    rollback_sh = _resolve_path(
        repo_root,
        os.getenv("WATCHDOG_ROLLBACK_SH", str(repo_root / "deploy_tools" / "rollback.sh")),
    )
    backup_raw = (os.getenv("WATCHDOG_BACKUP", "") or "").strip()
    backup_path = _resolve_path(repo_root, backup_raw) if backup_raw else None

    dryrun = _truthy(os.getenv("WATCHDOG_DRYRUN", "0"))
    cooldown_s = max(1, _get_int("WATCHDOG_COOLDOWN_S", 120))

    max_actions = max(1, _get_int("WATCHDOG_MAX_ACTIONS", 3))
    max_actions_window_s = max(10, _get_int("WATCHDOG_MAX_ACTIONS_WINDOW_S", 900))

    stall_s = max(15, _get_int("WATCHDOG_STALL_S", 300))

    progress_spec = (os.getenv("WATCHDOG_PROGRESS_REGEX", "") or "").strip()
    if not progress_spec:
        # "progress" means the bot is alive and doing work (tune to your logs)
        progress_spec = r"scanner|SCAN|candidate|on_new_token|on_trade|heartbeat|tick|loop|processed|RAY_META|PF_"
    progress_regex = re.compile(progress_spec, re.IGNORECASE)

    rules = _parse_rules(os.getenv("WATCHDOG_ERROR_RULES", ""))

    poll_s = _get_float("WATCHDOG_POLL_S", 0.25)
    poll_s = max(0.05, min(5.0, poll_s))

    tg_token = (os.getenv("TELEGRAM_BOT_TOKEN", "") or "").strip()
    tg_chat = (os.getenv("TELEGRAM_CHAT_ID", "") or "").strip()

    return Config(
        repo_root=repo_root,
        run_dir=run_dir,
        logfile=logfile,
        pidfile=pidfile,
        start_sh=start_sh,
        outfile=outfile,
        rollback_sh=rollback_sh,
        backup_path=backup_path,
        dryrun=dryrun,
        cooldown_s=cooldown_s,
        max_actions=max_actions,
        max_actions_window_s=max_actions_window_s,
        stall_s=stall_s,
        progress_regex=progress_regex,
        rules=rules,
        poll_s=poll_s,
        telegram_bot_token=tg_token,
        telegram_chat_id=tg_chat,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Telegram (optional) — urllib (stdlib), never required
# ──────────────────────────────────────────────────────────────────────────────
def tg_send(cfg: Config, msg: str) -> None:
    if not cfg.telegram_bot_token or not cfg.telegram_chat_id:
        return

    # Keep this fully best-effort and silent on failure.
    with contextlib.suppress(Exception):
        url = f"https://api.telegram.org/bot{cfg.telegram_bot_token}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": cfg.telegram_chat_id, "text": msg}).encode(
            "utf-8"
        )
        req = urllib.request.Request(url=url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310 (trusted Telegram endpoint)
            _ = resp.read(64)  # small read to complete request


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
    except OSError:
        return False


def _kill_pid(pid: int, sig: int) -> None:
    with contextlib.suppress(Exception):
        os.kill(pid, sig)


def _kill_process_group(pid: int, sig: int) -> bool:
    """
    On unix, if pid is a session/group leader (we start with setsid), kill the group.
    Returns True if attempted a group kill.
    """
    if os.name == "nt":
        return False

    with contextlib.suppress(Exception):
        pgid = os.getpgid(pid)
        # If pgid resolves, attempt killpg (covers children).
        os.killpg(pgid, sig)
        return True
    return False


def stop_via_pidfile(pidfile: pathlib.Path, timeout_s: float = 3.0) -> None:
    pid = read_pid(pidfile)
    if not pid:
        return

    if not pid_is_alive(pid):
        with contextlib.suppress(Exception):
            pidfile.unlink()
        return

    LOG.warning("Stopping existing process pid=%s (pidfile=%s)", pid, pidfile)

    # Graceful stop
    if not _kill_process_group(pid, signal.SIGTERM):
        _kill_pid(pid, signal.SIGTERM)

    t0 = time.time()
    while time.time() - t0 < timeout_s:
        if not pid_is_alive(pid):
            break
        time.sleep(0.15)

    # Hard stop
    if pid_is_alive(pid):
        LOG.error("Process did not stop on SIGTERM; forcing kill pid=%s", pid)
        if os.name == "nt":
            with contextlib.suppress(Exception):
                subprocess.run(
                    ["taskkill", "/PID", str(pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
        else:
            # Try kill group first, then pid as fallback.
            attempted = _kill_process_group(pid, signal.SIGKILL)
            if not attempted:
                _kill_pid(pid, signal.SIGKILL)

    with contextlib.suppress(Exception):
        pidfile.unlink()


def _atomic_write_text(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def start_background(start_sh: pathlib.Path, outfile: pathlib.Path, pidfile: pathlib.Path) -> int:
    if not start_sh.exists():
        raise FileNotFoundError(f"start script not found at {start_sh}")

    outfile.parent.mkdir(parents=True, exist_ok=True)
    pidfile.parent.mkdir(parents=True, exist_ok=True)

    # Use bash explicitly; watchdog is intended for Render/Linux runtime.
    cmd = ["bash", str(start_sh)]
    LOG.info("Starting background: %s", " ".join(shlex.quote(c) for c in cmd))

    # Detach group on unix so we can kill the whole group later.
    popen_kwargs: dict[str, object] = {}
    if os.name != "nt":
        popen_kwargs["preexec_fn"] = os.setsid  # type: ignore[assignment]

    with open(outfile, "a", encoding="utf-8") as out:
        p = subprocess.Popen(
            cmd,
            stdout=out,
            stderr=out,
            cwd=str(start_sh.parent),
            env=os.environ.copy(),
            **popen_kwargs,
        )

    _atomic_write_text(pidfile, f"{p.pid}\n")
    return p.pid


# ──────────────────────────────────────────────────────────────────────────────
# Locking for actions
# ──────────────────────────────────────────────────────────────────────────────
class ActionLock:
    """
    Lockfile using O_EXCL to prevent concurrent restarts/rollbacks.

    If lock exists and is stale (e.g., watchdog crashed mid-action), we will
    ignore it after a conservative "stale" threshold derived from config.
    """

    def __init__(self, path: pathlib.Path, stale_after_s: int) -> None:
        self.path = path
        self.stale_after_s = stale_after_s
        self.fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        # Stale lock cleanup (best-effort)
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
# Log tailer (rotation/truncation-safe)
# ──────────────────────────────────────────────────────────────────────────────
class Tailer:
    def __init__(self, path: pathlib.Path) -> None:
        self.path = path
        self.f: object | None = None
        self._inode: tuple[int, int] | None = None
        self._pos: int = 0

    def _open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        f = open(self.path, encoding="utf-8", errors="replace")
        st = os.fstat(f.fileno())
        self._inode = (st.st_ino, st.st_dev)
        f.seek(0, os.SEEK_END)
        self._pos = f.tell()
        self.f = f

    def _close(self) -> None:
        if self.f is None:
            return
        with contextlib.suppress(Exception):
            # f is a TextIO, but typed as object for minimal imports
            self.f.close()  # type: ignore[attr-defined]
        self.f = None
        self._inode = None
        self._pos = 0

    def _needs_reopen(self) -> bool:
        try:
            st = self.path.stat()
            cur_inode = (st.st_ino, st.st_dev)
            if self._inode is not None and cur_inode != self._inode:
                return True  # rotated/replaced
            if st.st_size < self._pos:
                return True  # truncated
            return False
        except FileNotFoundError:
            return True
        except Exception:
            return False

    def readline(self) -> str | None:
        if self.f is None:
            if not self.path.exists():
                return None
            self._open()

        if self._needs_reopen():
            self._close()
            return None

        line = self.f.readline()  # type: ignore[union-attr]
        if not line:
            return None

        self._pos = self.f.tell()  # type: ignore[union-attr]
        return line.rstrip("\n")


# ──────────────────────────────────────────────────────────────────────────────
# Policy / state
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class Event:
    ts: float
    line: str


def find_latest_env_backup(repo_root: pathlib.Path) -> pathlib.Path | None:
    """
    Prefer newest .env.bak.* in repo root if WATCHDOG_BACKUP isn't set.
    """
    candidates = list(repo_root.glob(".env.bak.*"))
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def _prune_times(q: deque[float], window_s: int) -> None:
    now = time.time()
    while q and (now - q[0]) > float(window_s):
        q.popleft()


def _too_many_actions(action_times: deque[float], cfg: Config) -> bool:
    _prune_times(action_times, cfg.max_actions_window_s)
    return len(action_times) >= cfg.max_actions


def _dump_tail(run_dir: pathlib.Path, window: deque[Event], reason: str) -> None:
    """
    Write a small forensic tail snapshot for debugging.
    Uses only file output (no new env keys).
    """
    try:
        run_dir.mkdir(parents=True, exist_ok=True)
        path = run_dir / "watchdog.last_tail.log"
        payload = {
            "ts": _utc_ts(),
            "reason": reason,
            "lines": [e.line for e in list(window)[-200:]],
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception:
        # Never let debug dumping break watchdog behavior.
        return


# ──────────────────────────────────────────────────────────────────────────────
# Actions (lock-safe)
# ──────────────────────────────────────────────────────────────────────────────
def _restart_unlocked(cfg: Config, reason: str) -> None:
    stop_via_pidfile(cfg.pidfile)
    pid = start_background(cfg.start_sh, cfg.outfile, cfg.pidfile)
    LOG.warning("Restarted pid=%s reason=%s", pid, reason)


def do_restart(cfg: Config, reason: str, window: deque[Event]) -> None:
    LOG.error("ACTION=restart reason=%s", reason)
    _dump_tail(cfg.run_dir, window, reason=f"restart: {reason}")
    tg_send(cfg, f"[WATCHDOG] restart: {reason}")

    if cfg.dryrun:
        LOG.warning("DRYRUN=1 -> not restarting")
        return

    stale_after = max(600, cfg.cooldown_s * 3, cfg.stall_s)
    lock = ActionLock(cfg.run_dir / "watchdog.action.lock", stale_after_s=stale_after)
    if not lock.acquire():
        LOG.warning("Action lock already held; skipping restart")
        return

    try:
        _restart_unlocked(cfg, reason=reason)
    finally:
        lock.release()


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

    env = os.environ.copy()
    env.setdefault("RESTART", "1")
    LOG.error("Calling rollback.sh backup=%s", backup)
    subprocess.run(["bash", str(cfg.rollback_sh), str(backup)], env=env, check=False)


def do_rollback(cfg: Config, reason: str, window: deque[Event]) -> None:
    LOG.error("ACTION=rollback reason=%s", reason)
    _dump_tail(cfg.run_dir, window, reason=f"rollback: {reason}")
    tg_send(cfg, f"[WATCHDOG] rollback: {reason}")

    if cfg.dryrun:
        LOG.warning("DRYRUN=1 -> not rolling back")
        return

    stale_after = max(600, cfg.cooldown_s * 3, cfg.stall_s)
    lock = ActionLock(cfg.run_dir / "watchdog.action.lock", stale_after_s=stale_after)
    if not lock.acquire():
        LOG.warning("Action lock already held; skipping rollback")
        return

    try:
        _rollback_unlocked(cfg, reason=reason)
    finally:
        lock.release()


# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────
def main() -> int:
    cfg = load_config()
    cfg.run_dir.mkdir(parents=True, exist_ok=True)

    LOG.info("repo_root=%s", cfg.repo_root)
    LOG.info("run_dir=%s", cfg.run_dir)
    LOG.info("logfile=%s", cfg.logfile)
    LOG.info("pidfile=%s", cfg.pidfile)
    LOG.info("start_sh=%s", cfg.start_sh)
    LOG.info("outfile=%s", cfg.outfile)
    LOG.info("rollback_sh=%s", cfg.rollback_sh)
    LOG.info("backup=%s", str(cfg.backup_path) if cfg.backup_path else "(auto)")
    LOG.info(
        "dryrun=%s cooldown_s=%s stall_s=%s poll_s=%s max_actions=%s window_s=%s",
        cfg.dryrun,
        cfg.cooldown_s,
        cfg.stall_s,
        cfg.poll_s,
        cfg.max_actions,
        cfg.max_actions_window_s,
    )

    tailer = Tailer(cfg.logfile)

    # Debug window (recent log lines) used for forensic dump on action.
    window: deque[Event] = deque(maxlen=5000)

    # Action throttling
    action_times: deque[float] = deque()
    last_action_at = 0.0

    # Per-rule hit timestamps
    rule_hits: list[deque[float]] = [deque() for _ in cfg.rules]

    # Progress tracking:
    # - Only arm stall detection after we've seen at least one "progress" match.
    last_progress_at: float | None = None

    # If logfile doesn't exist yet, wait (Render boot sequence)
    while not cfg.logfile.exists():
        LOG.warning("Waiting for logfile to appear: %s", cfg.logfile)
        time.sleep(1.0)

    LOG.info("Watchdog running…")

    while True:
        now = time.time()
        line = tailer.readline()

        if line is None:
            time.sleep(cfg.poll_s)

            # If pidfile exists but process is dead -> restart quickly
            pid = read_pid(cfg.pidfile)
            if pid and not pid_is_alive(pid):
                now2 = time.time()
                if (now2 - last_action_at) >= float(cfg.cooldown_s) and not _too_many_actions(
                    action_times, cfg
                ):
                    action_times.append(now2)
                    last_action_at = now2
                    do_restart(cfg, reason="pidfile process not alive", window=window)
                continue

            # Stall detection (only after progress is armed)
            if last_progress_at is not None:
                now2 = time.time()
                age = now2 - last_progress_at
                if age >= float(cfg.stall_s):
                    if (now2 - last_action_at) < float(cfg.cooldown_s):
                        continue
                    if _too_many_actions(action_times, cfg):
                        LOG.error("STALL detected but throttled. age_s=%.1f", age)
                        tg_send(cfg, f"[WATCHDOG] stall throttled: no progress for {int(age)}s")
                        continue

                    action_times.append(now2)
                    last_action_at = now2
                    do_restart(
                        cfg,
                        reason=(
                            f"stall: no progress for {int(age)}s "
                            f"(regex={cfg.progress_regex.pattern})"
                        ),
                        window=window,
                    )
                continue

            continue

        # Got a log line
        now = time.time()
        window.append(Event(ts=now, line=line))

        # Arm/update progress on match
        if cfg.progress_regex.search(line):
            last_progress_at = now

        # Evaluate rules
        triggered_action: str | None = None
        triggered_reason: str | None = None
        triggered_idx: int | None = None

        for idx, rule in enumerate(cfg.rules):
            if rule.pattern.search(line):
                hits = rule_hits[idx]
                hits.append(now)
                while hits and (now - hits[0]) > float(rule.window_s):
                    hits.popleft()

                if len(hits) >= rule.count:
                    triggered_action = rule.action
                    triggered_reason = (
                        f"rule={rule.pattern.pattern} hits>={rule.count} in {rule.window_s}s"
                    )
                    triggered_idx = idx
                    break

        if triggered_action is None:
            continue

        # cooldown / max-actions gating
        if (now - last_action_at) < float(cfg.cooldown_s):
            LOG.warning("Rule matched but in cooldown: %s", triggered_reason)
            continue

        if _too_many_actions(action_times, cfg):
            LOG.error("Rule matched but throttled (too many actions): %s", triggered_reason)
            tg_send(cfg, f"[WATCHDOG] throttled: {triggered_reason}")
            continue

        action_times.append(now)
        last_action_at = now

        # Clear the triggering rule hits so we don't immediately re-trigger
        if triggered_idx is not None:
            rule_hits[triggered_idx].clear()

        if triggered_action == "rollback":
            do_rollback(cfg, reason=triggered_reason or "rule", window=window)
        else:
            do_restart(cfg, reason=triggered_reason or "rule", window=window)

    # unreachable
    # return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        LOG.warning("KeyboardInterrupt -> exit")
        # B904: explicitly suppress exception context/cause
        raise SystemExit(0) from None
