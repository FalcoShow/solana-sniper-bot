from __future__ import annotations

from pathlib import Path

p = Path("utils_pkg/router.py")
txt = p.read_text(encoding="utf-8")


def die(msg: str) -> None:
    raise SystemExit("router_mock_session_fix FAILED: " + msg)


# --- 1) Ensure helper exists: _allow_mock_session(session) ---
needle_allow = "def _allow_mock_session(session: Any | None) -> bool:"
if needle_allow not in txt:
    anchor = "def _is_pytest_runtime() -> bool:"
    i = txt.find(anchor)
    if i < 0:
        die("Missing _is_pytest_runtime() - expected from prior fix.")

    # insert just before def _allow_noncanonical_mints
    j = txt.find("def _allow_noncanonical_mints() -> bool:", i)
    if j < 0:
        die("Cannot find _allow_noncanonical_mints() after _is_pytest_runtime().")

    helper = '''

def _allow_mock_session(session: Any | None) -> bool:
    """
    Pytest/backtest contract:
    - Tests may run with NO_NETWORK=1 (network=OFF) but still inject a CapturingSession.
    - In that case we MUST call session.get/session.post to exercise endpoint shapes,
      while still never touching real network (we never create a real http client).
    """
    if session is None:
        return False

    # Absolute safety hard stops
    if _envb("SAFE_MODE", False):
        return False

    # Explicit controls if you ever want them
    if _envb("ROUTER_DENY_MOCK_SESSION", False):
        return False
    if _envb("ROUTER_ALLOW_MOCK_SESSION", False):
        return True

    return _is_pytest_runtime() or _envb("BACKTEST_MODE", False)

'''
    txt = txt[:j] + helper + txt[j:]


# --- helper to patch within a function block safely ---
def patch_first_in_function(func_header: str, old: str, new: str) -> None:
    global txt
    s = txt.find(func_header)
    if s < 0:
        die(f"Missing function header: {func_header}")

    # function block ends at next "\n\ndef " or "\n\nasync def " or end
    end = len(txt)
    for mark in ("\n\nasync def ", "\n\ndef "):
        k = txt.find(mark, s + 5)
        if k != -1:
            end = min(end, k)

    block = txt[s:end]
    k = block.find(old)
    if k < 0:
        die(f"Could not find target text inside {func_header.strip()}")
    block = block[:k] + new + block[k + len(old) :]
    txt = txt[:s] + block + txt[end:]


# --- 2) get_quote(): allow injected session even when _net_allowed() is False ---
patch_first_in_function(
    "async def get_quote",
    "if not _net_allowed():",
    "if not _net_allowed() and not _allow_mock_session(session):",
)

# --- 3) _jup_quote_http(): same rule (do NOT block when session is injected under pytest) ---
patch_first_in_function(
    "async def _jup_quote_http",
    "if not _net_allowed():\n        _raise_net_disabled()",
    "if not _net_allowed() and not _allow_mock_session(session):\n        _raise_net_disabled()",
)

# --- 4) _jup_swap_http(): same rule ---
patch_first_in_function(
    "async def _jup_swap_http",
    "if not _net_allowed():\n        _raise_net_disabled()",
    "if not _net_allowed() and not _allow_mock_session(session):\n        _raise_net_disabled()",
)

# --- 5) swap_from_quote(): must NOT early-return DRYRUN when a mock session is injected ---
# We need to move session = kwargs.pop("session", None) above the early return.
hdr = "async def swap_from_quote"
s = txt.find(hdr)
if s < 0:
    die("Missing swap_from_quote()")

end = len(txt)
for mark in ("\n\nasync def ", "\n\ndef "):
    k = txt.find(mark, s + 5)
    if k != -1:
        end = min(end, k)

block = txt[s:end]

old_snip = '    if not _net_allowed():\n        return True, "DRYRUN"\n\n    session = kwargs.pop("session", None)\n'
new_snip = '    session = kwargs.pop("session", None)\n\n    if not _net_allowed() and not _allow_mock_session(session):\n        return True, "DRYRUN"\n'
if old_snip not in block:
    die("swap_from_quote() snippet not found (file differs).")
block = block.replace(old_snip, new_snip, 1)
txt = txt[:s] + block + txt[end:]

p.write_text(txt, encoding="utf-8")
print("router_mock_session_fix OK: pytest mock-session allowed under NO_NETWORK")
