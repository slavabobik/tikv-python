"""Generate the blocking client in ``src/tikv/_sync`` from ``src/tikv/_async``.

The asyncio code is the single source of truth; the sync flavor is a
mechanical translation (the approach httpcore uses).  ``_compat.py`` is the
only hand-written module in ``_sync``.

Usage::

    python scripts/unasync.py          # regenerate
    python scripts/unasync.py --check  # fail if the generated code is stale
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "src" / "tikv" / "_async"
TARGET = ROOT / "src" / "tikv" / "_sync"
HAND_WRITTEN = {"_compat.py"}

SUBSTITUTIONS = [
    (r"\basync def\b", "def"),
    (r"\basync with\b", "with"),
    (r"\basync for\b", "for"),
    (r"\bawait ", ""),
    (r"\bAsyncIterator\b", "Iterator"),
    (r"\bAsyncRawClient\b", "RawClient"),
    (r"\b__aenter__\b", "__enter__"),
    (r"\b__aexit__\b", "__exit__"),
]


def translate(source: str, name: str) -> str:
    for pattern, replacement in SUBSTITUTIONS:
        source = re.sub(pattern, replacement, source)
    header = f"# Generated from _async/{name} by scripts/unasync.py. DO NOT EDIT.\n"
    return header + source


def main(check: bool) -> int:
    stale = []
    for path in sorted(SOURCE.glob("*.py")):
        if path.name in HAND_WRITTEN:
            continue
        target = TARGET / path.name
        expected = translate(path.read_text(), path.name)
        if target.exists() and target.read_text() == expected:
            continue
        stale.append(target)
        if not check:
            target.write_text(expected)
    if check and stale:
        names = ", ".join(str(p.relative_to(ROOT)) for p in stale)
        print(f"out of date: {names}; run scripts/unasync.py", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(check="--check" in sys.argv[1:]))
