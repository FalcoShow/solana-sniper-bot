from __future__ import annotations

from pathlib import Path

p = Path("utils_pkg/router.py")
txt = p.read_text(encoding="utf-8")


def die(msg: str) -> None:
    raise SystemExit("router_mock_session_fix2 FAILED: " + msg)


# Find get_quote() block
i = txt.find("async def get_quote")
if i < 0:
    die("Could not find async def get_quote")

# Find inner _fetch() within get_quote
j = txt.find("async def _fetch()", i)
if j < 0:
    die("Could not find inner async def _fetch() inside get_quote")

# Find the first 'if not _net_allowed():' after _fetch starts
k = txt.find("if not _net_allowed():", j)
if k < 0:
    die("Could not find 'if not _net_allowed():' inside get_quote/_fetch")

# Replace only that one line
txt = (
    txt[:k]
    + "if not _net_allowed() and not _allow_mock_session(session):"
    + txt[k + len("if not _net_allowed():") :]
)

p.write_text(txt, encoding="utf-8")
print("router_mock_session_fix2 OK: get_quote/_fetch now allows mock session under NO_NETWORK")
