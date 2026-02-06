from __future__ import annotations

"""
tests/conftest.py — Pytest Runtime Contract (Phase 0–7 LOCK-IN)
==============================================================

Hard goals:
- NO production imports at conftest import-time (stdlib + pytest only).
- Windows asyncio stability for offline/no-network contract tests.
- Safe-by-default env: no live trading, no background workers, no local services.
- Prevent suite poisoning:
  * Scrub proxy envs.
  * Purge heavy modules before lightweight contract checks.
  * Block outbound network by default (socket-level), opt-in only.
  * ✅ Allow LOOPBACK ONLY while blocked: 127.0.0.1, ::1, localhost

Opt-in:
- integration:  pytest -m integration --run-integration
- network:       pytest -m network --run-network
- slow:          pytest -m slow --run-slow
"""

import asyncio
import contextlib
import ipaddress
import os
import random
import socket
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

# =============================================================================
# Platform helpers
# =============================================================================

_IS_WIN = (os.name == "nt") or sys.platform.startswith("win")

_TRUE = {"1", "true", "yes", "y", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f"}

_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def _envs(k: str, d: str = "") -> str:
    v = os.getenv(k)
    return (v if v is not None else d).strip()


def _envb(k: str, d: bool = False) -> bool:
    raw = _envs(k, "")
    if raw == "":
        return bool(d)
    s = raw.lower()
    if s in _TRUE:
        return True
    if s in _FALSE:
        return False
    return bool(d)


def _set_windows_event_loop_policy() -> None:
    """
    Import-time safe: set the most compatible asyncio policy for Windows.
    pytest-asyncio respects event_loop_policy fixture too, but this guards
    early imports that call asyncio.get_event_loop_policy().
    """
    if _IS_WIN and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        with contextlib.suppress(Exception):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


_set_windows_event_loop_policy()

# =============================================================================
# Repo bootstrap (root + optional src/)
# =============================================================================


def _find_repo_root(start: Path) -> Path:
    markers = (
        "pyproject.toml",
        "pytest.ini",
        "requirements.txt",
        "setup.cfg",
        ".git",
        "render.yaml",
        ".env.example",
    )
    cur = start.resolve()
    for p in (cur, *tuple(cur.parents)):
        with contextlib.suppress(Exception):
            if any((p / m).exists() for m in markers):
                return p

            src = p / "src"
            if src.is_dir() and any(
                (src / x).exists() for x in ("utils_pkg", "scanner", "scripts", "tests")
            ):
                return p

            if any((p / x).exists() for x in ("utils_pkg", "scanner", "scripts", "tests")):
                return p

    return cur.parents[0] if cur.parents else cur


def _bootstrap_paths() -> Path:
    here = Path(__file__).resolve()
    root = _find_repo_root(here.parent)

    root_str = str(root)
    if root_str and root_str not in sys.path:
        sys.path.insert(0, root_str)

    src = root / "src"
    if src.is_dir():
        src_str = str(src)
        if src_str not in sys.path:
            sys.path.insert(0, src_str)

    os.environ.setdefault("REPO_ROOT", root_str)
    return root


REPO_ROOT = _bootstrap_paths()

# =============================================================================
# Baseline env (import-time safe)
# =============================================================================
# These are safe to set at import time because they only disable behavior.

os.environ.setdefault("PYTEST_RUNNING", "1")
os.environ.setdefault("PYTEST_MODE", "1")
os.environ.setdefault("BACKTEST_MODE", "1")

# Default: offline + safe imports.
os.environ.setdefault("SAFE_MODE", "1")
os.environ.setdefault("NO_NETWORK", "1")
os.environ.setdefault("DISABLE_NETWORK", "1")

# Trading hard-off
os.environ.setdefault("TRADING_ENABLED", "0")
os.environ.setdefault("LIVE_TRADING", "0")
os.environ.setdefault("DISABLE_LIVE_TRADING", "1")
os.environ.setdefault("DRYRUN", "1")
os.environ.setdefault("DRY_RUN", "1")
os.environ.setdefault("DISABLE_BUYS", "1")
os.environ.setdefault("DISABLE_SELLS", "1")
os.environ.setdefault("DISABLE_SCANNERS", "1")

# Workers hard-off
os.environ.setdefault("REINVESTOR_ENABLED", "0")
os.environ.setdefault("AUTO_EXIT_WORKER_ENABLED", "0")
os.environ.setdefault("EXIT_ENGINE_WORKER_ENABLED", "0")
os.environ.setdefault("SMART_WALLETS_ENABLED", "0")

# Default suite must never run live scanner tests
os.environ.setdefault("SCANNER_LIVE_TESTS", "0")

# Prevent dotenv side effects during tests
os.environ.setdefault("CORE_LOAD_DOTENV", "0")

# Keep run-to-run deterministic by default
os.environ.setdefault("MAIN_DISABLE_RANDOM_JITTER", "1")

# Avoid accidental bytecode writes into repo
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

# =============================================================================
# Markers & CLI options
# =============================================================================

PRIMARY_MARKERS: set[str] = {
    "unit",
    "smoke",
    "integration",
    "slow",
    "network",
    "contract",
    "regression",
    "perf",
    "db",
    "redis",
    "rpc",
    "router",
    "scanners",
    "entry",
    "exit",
    "risk",
    "storage",
    "alerts",
}

WALLET_ENV_KEYS: list[str] = [
    "WALLET_PRIVATE_KEY",
    "WALLET_KEYPAIR_PATH",
    "KEYPAIR_PATH",
    "WALLET_PRIVKEY_B58",
    "WALLET_PRIVKEY_B64",
    "WALLET_PRIVKEY_JSON",
    "WALLET_KEYPAIR_JSON",
    "KEYPAIR_JSON",
    "WALLET_JSON",
]

LOCAL_SERVICE_ENV_KEYS: list[str] = [
    "POSTGRES_DSN",
    "PG_DSN",
    "PG_DSN_INTERNAL",
    "PG_DSN_EXTERNAL",
    "DATABASE_URL",
    "REDIS_URL",
    "REDIS_TLS_URL",
    "SOLANA_RPC_URL",
    "SOL_RPC_URL",
    "RPC_URL",
    "HELIUS_RPC_URL",
]

INTEGRATION_SECRET_ENV_KEYS: list[str] = [
    "NOTION_TOKEN",
    "NOTION_API_KEY",
    "NOTION_DATABASE_ID",
    "NOTION_DB_TRADES",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID",
    "DISCORD_WEBHOOK_URL",
    "SLACK_WEBHOOK_URL",
    "OPENAI_API_KEY",
    "JUP_API_KEY",
    "JUP_PRO_KEY",
]

# Proxy keys: keep both canonical + lowercase variants, but avoid a literal "no_proxy"
_PROXY_CANON = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY")
PROXY_ENV_KEYS: list[str] = [*_PROXY_CANON, *[k.lower() for k in _PROXY_CANON]]

_NO_PROXY_UPPER = "NO_PROXY"
_NO_PROXY_LOWER = _NO_PROXY_UPPER.lower()


def pytest_addoption(parser: pytest.Parser) -> None:
    # Optional ini for newer pytest-asyncio. Harmless if unused.
    with contextlib.suppress(Exception):
        parser.addini(
            "asyncio_default_fixture_loop_scope",
            "pytest-asyncio compatibility (ignored unless pytest-asyncio uses it)",
            default="function",
        )

    g = parser.getgroup("sniper")
    g.addoption("--run-integration", action="store_true", default=False)
    g.addoption("--run-network", action="store_true", default=False)
    g.addoption("--run-slow", action="store_true", default=False)
    g.addoption(
        "--allow-live-env",
        action="store_true",
        default=False,
        help="DANGEROUS: allow wallet/secrets/local-service env vars to remain in test process",
    )
    g.addoption("--seed", action="store", default="")


def _want_integration(cfg: pytest.Config) -> bool:
    return bool(cfg.getoption("--run-integration")) or _envb("PYTEST_RUN_INTEGRATION", False)


def _want_network(cfg: pytest.Config) -> bool:
    return bool(cfg.getoption("--run-network")) or _envb("PYTEST_RUN_NETWORK", False)


def _want_slow(cfg: pytest.Config) -> bool:
    return bool(cfg.getoption("--run-slow")) or _envb("PYTEST_RUN_SLOW", False)


def _allow_live_env(cfg: pytest.Config) -> bool:
    return bool(cfg.getoption("--allow-live-env")) or _envb("PYTEST_ALLOW_LIVE_ENV", False)


def pytest_configure(config: pytest.Config) -> None:
    # Register markers (safe even if duplicated)
    for m, desc in [
        ("unit", "fast pure unit tests (default when unmarked)"),
        ("smoke", "minimal go/no-go tests for wiring"),
        ("integration", "may touch local services; opt-in only"),
        ("network", "hits real internet; opt-in only"),
        ("slow", "long-running tests; opt-in only"),
        ("contract", "contract/shape tests"),
        ("regression", "prevents reintroducing past bugs"),
        ("perf", "performance-related assertions"),
        ("db", "requires database"),
        ("redis", "requires redis"),
        ("rpc", "requires solana RPC"),
        ("router", "router contract tests"),
        ("scanners", "scanner contract tests"),
        ("entry", "entry pipeline tests"),
        ("exit", "exit pipeline tests"),
        ("risk", "risk_manager / guardrails tests"),
        ("storage", "position store / persistence tests"),
        ("alerts", "telegram/discord/notion alerting tests"),
    ]:
        with contextlib.suppress(Exception):
            config.addinivalue_line("markers", f"{m}: {desc}")

    _set_windows_event_loop_policy()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        marker_names = {m.name for m in item.iter_markers()}
        if not (marker_names & PRIMARY_MARKERS):
            item.add_marker(pytest.mark.unit)


# =============================================================================
# Session-level environment guardrails (deterministic/offline)
# =============================================================================


@pytest.fixture(autouse=True, scope="session")
def _force_contract_mode_env() -> None:
    # Keep tests deterministic/offline
    os.environ.setdefault("NETWORK", "OFF")
    os.environ.setdefault("NO_NETWORK", "1")
    os.environ.setdefault("DISABLE_NETWORK", "1")
    os.environ.setdefault("SAFE_MODE", "1")
    os.environ.setdefault("PYTEST_MODE", "1")
    os.environ.setdefault("CONTRACT_TEST", "1")
    os.environ.setdefault("SNIPER_TEST_CONTRACT", "1")

    # Redis is network I/O too; force it off unless a test explicitly enables it
    os.environ.setdefault("DISABLE_REDIS", "1")
    os.environ.setdefault("REDIS_ENABLED", "0")


@pytest.fixture(autouse=True)
def _block_outbound_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Lightweight early guard (kept for compatibility): blocks socket.connect to non-loopback.
    A stronger guard also runs later in this file.
    """
    real_connect = socket.socket.connect

    def guarded_connect(self: socket.socket, address: Any) -> Any:
        host = address[0] if isinstance(address, tuple) else address
        if host not in ("127.0.0.1", "localhost", "::1"):
            raise RuntimeError(f"Network disabled in tests; attempted connect to {address!r}")
        return real_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect, raising=True)


# =============================================================================
# pytest-asyncio FUTURE-PROOFING:
# Use event_loop_policy (recommended) instead of overriding event_loop.
# =============================================================================


@pytest.fixture(scope="session")
def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    if _IS_WIN and hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
        return asyncio.WindowsProactorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


# =============================================================================
# Determinism
# =============================================================================


def _pick_seed(cfg: pytest.Config) -> str:
    opt = str(cfg.getoption("--seed") or "").strip()
    env = _envs("SNIPER_TEST_SEED", "").strip()
    return opt or env or "1337"


@pytest.fixture(scope="session", autouse=True)
def _seed_session(request: pytest.FixtureRequest) -> None:
    seed = _pick_seed(request.config)
    try:
        random.seed(int(seed))
    except Exception:
        random.seed(seed)

    os.environ.setdefault("PYTEST_SEED", str(seed))
    # NOTE: setting PYTHONHASHSEED at runtime does not change a running interpreter’s hash seed,
    # but keeping it here helps downstream tools and subprocesses.
    os.environ.setdefault("PYTHONHASHSEED", str(seed))
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")


def pytest_report_header(config: pytest.Config) -> str:
    seed = _pick_seed(config)
    pol = type(asyncio.get_event_loop_policy()).__name__
    flags = [
        f"seed={seed}",
        f"integration={'ON' if _want_integration(config) else 'OFF'}",
        f"network={'ON' if _want_network(config) else 'OFF'}",
        f"slow={'ON' if _want_slow(config) else 'OFF'}",
        f"allow_live_env={'ON (DANGEROUS)' if _allow_live_env(config) else 'OFF'}",
        f"loop_policy={pol}",
        f"repo_root={REPO_ROOT}",
    ]
    return "sniper-test-contract: " + " | ".join(flags)


# =============================================================================
# Safety env defaults (session + per-test)
# =============================================================================


def _strip_env_keys(keys: Iterable[str]) -> None:
    for k in keys:
        os.environ.pop(k, None)


def pytest_sessionstart(session: pytest.Session) -> None:
    """
    Session-level hardening so accidental environment leakage doesn't poison imports.
    Per-test fixture below further enforces safety with monkeypatch.
    """
    cfg = session.config
    if _allow_live_env(cfg):
        return

    # Always scrub proxies + wallets + secrets at session start.
    _strip_env_keys(PROXY_ENV_KEYS)

    # Ensure "no proxy" behavior for any libs that consult proxy envs.
    os.environ.setdefault(_NO_PROXY_UPPER, "*")
    os.environ.setdefault(_NO_PROXY_LOWER, "*")

    _strip_env_keys(WALLET_ENV_KEYS)
    _strip_env_keys(INTEGRATION_SECRET_ENV_KEYS)

    # Only allow local service envs when running opt-in integration/network.
    if not (_want_integration(cfg) or _want_network(cfg)):
        _strip_env_keys(LOCAL_SERVICE_ENV_KEYS)


def _strip_env(monkeypatch: pytest.MonkeyPatch, keys: Iterable[str]) -> None:
    for k in keys:
        monkeypatch.delenv(k, raising=False)


def _apply_safe_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PYTEST_RUNNING", "1")
    monkeypatch.setenv("PYTEST_MODE", "1")
    monkeypatch.setenv("BACKTEST_MODE", "1")
    monkeypatch.setenv("CORE_LOAD_DOTENV", "0")
    monkeypatch.setenv("DRYRUN", "1")
    monkeypatch.setenv("DRY_RUN", "1")

    # Default suite should be safe/no-network unless opt-in.
    monkeypatch.setenv("SAFE_MODE", "1")
    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")

    # Default suite must never run live scanner tests
    monkeypatch.setenv("SCANNER_LIVE_TESTS", "0")

    # Disable live actions / workers by default
    monkeypatch.setenv("TRADING_ENABLED", "0")
    monkeypatch.setenv("LIVE_TRADING", "0")
    monkeypatch.setenv("DISABLE_LIVE_TRADING", "1")
    monkeypatch.setenv("DISABLE_BUYS", "1")
    monkeypatch.setenv("DISABLE_SELLS", "1")
    monkeypatch.setenv("DISABLE_SCANNERS", "1")
    monkeypatch.setenv("REINVESTOR_ENABLED", "0")
    monkeypatch.setenv("AUTO_EXIT_WORKER_ENABLED", "0")
    monkeypatch.setenv("EXIT_ENGINE_WORKER_ENABLED", "0")
    monkeypatch.setenv("SMART_WALLETS_ENABLED", "0")

    # Keep logs quiet unless user explicitly bumps
    monkeypatch.setenv("LOG_LEVEL", _envs("LOG_LEVEL", "WARNING") or "WARNING")
    monkeypatch.setenv("MAIN_DISABLE_RANDOM_JITTER", "1")

    # Prevent proxy-induced weirdness / loopback connects
    for k in PROXY_ENV_KEYS:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv(_NO_PROXY_UPPER, "*")
    monkeypatch.setenv(_NO_PROXY_LOWER, "*")


@pytest.fixture(autouse=True)
def _safe_env_defaults(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = request.config
    if _allow_live_env(cfg):
        return

    _apply_safe_defaults(monkeypatch)

    is_integration = request.node.get_closest_marker("integration") is not None
    is_network = request.node.get_closest_marker("network") is not None

    # Allow network toggles only when explicitly enabled.
    if is_network and _want_network(cfg):
        monkeypatch.setenv("NO_NETWORK", "0")
        monkeypatch.setenv("DISABLE_NETWORK", "0")
        monkeypatch.setenv("SAFE_MODE", "0")

    # Always scrub secrets/wallets unless explicitly allowed.
    _strip_env(monkeypatch, WALLET_ENV_KEYS)
    _strip_env(monkeypatch, INTEGRATION_SECRET_ENV_KEYS)

    # For non-integration/network runs, also block local service envs.
    if not ((is_integration and _want_integration(cfg)) or (is_network and _want_network(cfg))):
        _strip_env(monkeypatch, LOCAL_SERVICE_ENV_KEYS)


# =============================================================================
# Socket-level network blocking (default OFFLINE)
# =============================================================================


class NetworkBlockedError(RuntimeError):
    """Raised when a test attempts non-loopback network I/O without opting in."""


def _should_allow_network(request: pytest.FixtureRequest) -> bool:
    cfg = request.config
    if _allow_live_env(cfg):
        return True
    # Explicit opt-in only
    if request.node.get_closest_marker("network") is not None and _want_network(cfg):
        return True
    return bool(
        request.node.get_closest_marker("integration") is not None and _want_integration(cfg)
    )


def _norm_host(host: Any) -> str:
    if host is None:
        return ""
    if isinstance(host, bytes):
        with contextlib.suppress(Exception):
            host = host.decode("utf-8", "ignore")
    return str(host).strip().strip("[]").lower()


def _is_loopback_host(host: Any) -> bool:
    """
    Strict allowlist:
      - 127.0.0.1
      - ::1
      - localhost
    Also allows any literal IP that ipaddress marks as loopback.
    """
    h = _norm_host(host)
    if h in _LOOPBACK_HOSTS:
        return True
    with contextlib.suppress(Exception):
        ip = ipaddress.ip_address(h)
        return bool(ip.is_loopback)
    return False


def _extract_host_from_sockaddr(addr: Any) -> str | None:
    """
    Returns the host part if this looks like an INET/INET6 sockaddr.
    - ('host', port)
    - ('host', port, flowinfo, scopeid)
    """
    if isinstance(addr, tuple) and len(addr) >= 2:
        return addr[0]
    return None


def _raise_blocked(host: Any, why: str = "") -> None:
    h = _norm_host(host) or "<unknown>"
    msg = (
        f"Network is blocked for this test run (loopback-only allowed).\nBlocked destination: {h}\n"
    )
    if why:
        msg += f"Reason: {why}\n"
    msg += (
        "Allowed without opt-in: 127.0.0.1, ::1, localhost\n"
        "To allow real network, mark the test with @pytest.mark.network and run with --run-network.\n"
        "To allow integration tests (may touch local services), mark with @pytest.mark.integration and run with --run-integration.\n"
    )
    raise NetworkBlockedError(msg)


@pytest.fixture(autouse=True)
def _block_network_by_default(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    Hard guard: block non-loopback network by default at the socket layer.

    ✅ While blocked, we still allow loopback connects:
       - 127.0.0.1
       - ::1
       - localhost

    Opt-in to real network via:
      - @pytest.mark.network + --run-network
      - @pytest.mark.integration + --run-integration
    """
    if _should_allow_network(request):
        return

    _orig_create_connection = socket.create_connection
    _orig_connect = socket.socket.connect
    _orig_connect_ex = socket.socket.connect_ex
    _orig_getaddrinfo = socket.getaddrinfo
    _orig_gethostbyname = getattr(socket, "gethostbyname", None)
    _orig_gethostbyname_ex = getattr(socket, "gethostbyname_ex", None)

    def _guard_host(host: Any, *, callsite: str) -> None:
        if host in (None, ""):
            _raise_blocked(host, why=f"{callsite}: empty host")
        if not _is_loopback_host(host):
            _raise_blocked(host, why=f"{callsite}: non-loopback host")

    def create_connection(address: Any, *args: Any, **kwargs: Any) -> Any:
        host = _extract_host_from_sockaddr(address)
        if host is not None:
            _guard_host(host, callsite="socket.create_connection")
            return _orig_create_connection(address, *args, **kwargs)

        # Non-INET address (e.g., AF_UNIX path str). Treat as local-only and allow.
        if isinstance(address, (str, bytes)):
            return _orig_create_connection(address, *args, **kwargs)

        _raise_blocked("<unknown>", why="socket.create_connection: unsupported address type")

    def connect(self: socket.socket, address: Any) -> Any:
        host = _extract_host_from_sockaddr(address)
        if host is not None:
            _guard_host(host, callsite="socket.socket.connect")
            return _orig_connect(self, address)

        # AF_UNIX path string: allow
        if isinstance(address, (str, bytes)):
            return _orig_connect(self, address)

        _raise_blocked("<unknown>", why="socket.socket.connect: unsupported address type")

    def connect_ex(self: socket.socket, address: Any) -> Any:
        host = _extract_host_from_sockaddr(address)
        if host is not None:
            _guard_host(host, callsite="socket.socket.connect_ex")
            return _orig_connect_ex(self, address)

        # AF_UNIX path string: allow
        if isinstance(address, (str, bytes)):
            return _orig_connect_ex(self, address)

        _raise_blocked("<unknown>", why="socket.socket.connect_ex: unsupported address type")

    def getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
        # Prevent DNS leaks: only allow resolving loopback targets.
        # Note: allow host=None/"" for passive bind/listen patterns (no DNS).
        if host in (None, ""):
            return _orig_getaddrinfo(host, *args, **kwargs)

        _guard_host(host, callsite="socket.getaddrinfo")
        return _orig_getaddrinfo(host, *args, **kwargs)

    def gethostbyname(host: Any) -> Any:
        _guard_host(host, callsite="socket.gethostbyname")
        assert _orig_gethostbyname is not None
        return _orig_gethostbyname(host)

    def gethostbyname_ex(host: Any) -> Any:
        _guard_host(host, callsite="socket.gethostbyname_ex")
        assert _orig_gethostbyname_ex is not None
        return _orig_gethostbyname_ex(host)

    # Patch core socket dial + resolution
    monkeypatch.setattr(socket, "create_connection", create_connection, raising=True)
    monkeypatch.setattr(socket.socket, "connect", connect, raising=True)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex, raising=True)

    with contextlib.suppress(Exception):
        monkeypatch.setattr(socket, "getaddrinfo", getaddrinfo, raising=True)
    if _orig_gethostbyname is not None:
        with contextlib.suppress(Exception):
            monkeypatch.setattr(socket, "gethostbyname", gethostbyname, raising=True)
    if _orig_gethostbyname_ex is not None:
        with contextlib.suppress(Exception):
            monkeypatch.setattr(socket, "gethostbyname_ex", gethostbyname_ex, raising=True)

    # Also make intent explicit in env for any import-time guards in code.
    monkeypatch.setenv("NO_NETWORK", "1")
    monkeypatch.setenv("DISABLE_NETWORK", "1")
    monkeypatch.setenv("SAFE_MODE", "1")


# =============================================================================
# Filesystem isolation (tests don’t write into repo)
# =============================================================================


@pytest.fixture(autouse=True)
def _isolate_fs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state = tmp_path / "state"
    cache = tmp_path / "cache"
    logs = tmp_path / "logs"
    exports = tmp_path / "exports"

    monkeypatch.setenv("STATE_DIR", str(state))
    monkeypatch.setenv("CACHE_DIR", str(cache))
    monkeypatch.setenv("LOG_DIR", str(logs))
    monkeypatch.setenv("EXPORT_DIR", str(exports))
    monkeypatch.setenv("BT_EXPORT_DIR", str(tmp_path / "bt_exports"))

    # Common app dirs that libraries might write to.
    monkeypatch.setenv("XDG_CACHE_HOME", str(cache))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))

    # Windows-specific fallbacks
    if _IS_WIN:
        monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))
        monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "localappdata"))
        monkeypatch.setenv("USERPROFILE", str(tmp_path / "userprofile"))

    # Some libs use HOME even on Windows; keep it isolated too.
    monkeypatch.setenv("HOME", str(tmp_path / "home"))


# =============================================================================
# Prevent "contracts lightweight" poisoning in the full suite
# =============================================================================

_FORBIDDEN_PREFIXES = (
    "numpy",
    "pandas",
    "httpx",
    "aiohttp",
    "requests",
    "websockets",
    "solana",
    "solders",
)


def _purge_modules_by_prefix(prefixes: tuple[str, ...]) -> None:
    for name in list(sys.modules.keys()):
        for p in prefixes:
            if name == p or name.startswith(p + "."):
                sys.modules.pop(name, None)
                break


def _is_contract_test(item: pytest.Item) -> bool:
    nodeid = (getattr(item, "nodeid", "") or "").replace("\\", "/").lower()
    if item.get_closest_marker("contract") is not None:
        return True
    return (
        ("tests/test_contract" in nodeid)
        or ("/test_contract" in nodeid)
        or ("test_contract" in nodeid)
    )


def pytest_runtest_setup(item: pytest.Item) -> None:
    cfg = item.config
    if item.get_closest_marker("integration") and not _want_integration(cfg):
        pytest.skip("integration test disabled (use --run-integration or PYTEST_RUN_INTEGRATION=1)")
    if item.get_closest_marker("network") and not _want_network(cfg):
        pytest.skip("network test disabled (use --run-network or PYTEST_RUN_NETWORK=1)")
    if item.get_closest_marker("slow") and not _want_slow(cfg):
        pytest.skip("slow test disabled (use --run-slow or PYTEST_RUN_SLOW=1)")

    # Keep contract tests lightweight (purge heavy modules before they run).
    if _is_contract_test(item):
        _purge_modules_by_prefix(_FORBIDDEN_PREFIXES)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_call(item: pytest.Item) -> None:
    # Only purge when running contract tests to keep them truly "light".
    if _is_contract_test(item):
        _purge_modules_by_prefix(_FORBIDDEN_PREFIXES)
    # DO NOT call item.runtest() here (pytest core will execute the test).


# =============================================================================
# Convenience fixtures
# =============================================================================


@pytest.fixture
def set_env(monkeypatch: pytest.MonkeyPatch):
    def _setter(**kvs: Any) -> None:
        for k, v in kvs.items():
            monkeypatch.setenv(str(k), "" if v is None else str(v))

    return _setter


@pytest.fixture
def del_env(monkeypatch: pytest.MonkeyPatch):
    def _deleter(*keys: str) -> None:
        for k in keys:
            monkeypatch.delenv(k, raising=False)

    return _deleter
