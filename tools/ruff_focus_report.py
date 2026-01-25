from __future__ import annotations

import codecs
import collections
import json
import sys
from pathlib import Path


def _load_json_any(path: str) -> object:
    data = Path(path).read_bytes()

    # Detect common BOMs / encodings (PowerShell redirection often produces UTF-16 LE)
    if data.startswith(codecs.BOM_UTF16_LE) or data.startswith(codecs.BOM_UTF16_BE):
        text = data.decode("utf-16")
    elif data.startswith(codecs.BOM_UTF8):
        text = data.decode("utf-8-sig")
    else:
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("utf-16")

    return json.loads(text)


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else "ruff_focus.json"
    d = _load_json_any(path)

    by: dict[str, list[dict]] = collections.defaultdict(list)
    for x in d:
        by[x["filename"]].append(x)

    for fn in sorted(by):
        print(f"\n{fn}")
        for x in sorted(
            by[fn],
            key=lambda z: (
                z["code"],
                z["location"]["row"],
                z["location"]["column"],
            ),
        ):
            loc = x["location"]
            print(f"  {x['code']}:{loc['row']}:{loc['column']} {x['message']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
