"""Reject C APIs that return borrowed references in porto-py (25-cpython-integration-and-memory.md
§3).

    uv run python scripts/check_borrowed_refs.py             # check crates/porto-py
    uv run python scripts/check_borrowed_refs.py PATH ...    # check other files or directories

On free-threaded CPython another thread can drop an object between a call that returned a
borrowed reference to it and the use of that reference. Each banned API has a replacement that
returns a strong reference (`PyDict_GetItemRef`, `PyList_GetItemRef`, `PyWeakref_GetRef`, ...);
the error names it. Comments are not checked, so docs may name the banned APIs. A line that has
to call one anyway (inside a critical section, say) ends with `// borrowed-ref: <why>`.

Exit status: 0 when clean, 1 when a banned API is used, 2 when a path does not exist.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PATHS = (REPOSITORY_ROOT / "crates" / "porto-py",)
ALLOW_MARKER = "// borrowed-ref:"

#: Each banned API and what to use instead.
BANNED = {
    "PyDict_GetItem": "PyDict_GetItemRef",
    "PyDict_GetItemWithError": "PyDict_GetItemRef",
    "PyDict_GetItemString": "PyDict_GetItemStringRef",
    "_PyDict_GetItemStringWithError": "PyDict_GetItemStringRef",
    "PyDict_SetDefault": "PyDict_SetDefaultRef",
    "PyDict_Next": "PyDictMethods::locked_for_each (it holds the dict's critical section)",
    "PyList_GetItem": "PyList_GetItemRef",
    "PyList_GET_ITEM": "PyList_GetItemRef",
    "PyWeakref_GetObject": "PyWeakref_GetRef",
    "PyWeakref_GET_OBJECT": "PyWeakref_GetRef",
    "PyImport_AddModule": "PyImport_AddModuleRef",
    "PyImport_AddModuleObject": "PyImport_AddModuleRef",
    "PyEval_GetBuiltins": "PyEval_GetFrameBuiltins",
    "PyEval_GetGlobals": "PyEval_GetFrameGlobals",
    "PyEval_GetLocals": "PyEval_GetFrameLocals",
    "PySys_GetObject": "the `sys` module's attributes (PySys_GetAttr on 3.14+)",
    "PyCell_GET": "PyCell_Get",
}

#: A banned name as a whole identifier, so `PyDict_GetItemRef` does not match `PyDict_GetItem`.
BANNED_CALL = re.compile(r"\b(" + "|".join(sorted(BANNED, key=len, reverse=True)) + r")\b")
LINE_COMMENT = re.compile(r"//.*$")


def rust_files(paths: list[Path]) -> list[Path]:
    """The `.rs` files under `paths` (files are taken as they are), skipping build output."""
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(file for file in sorted(path.rglob("*.rs")) if "target" not in file.parts)
        else:
            files.append(path)
    return files


def violations(file: Path) -> list[str]:
    """`file:line: message` for each banned call in `file`."""
    found: list[str] = []
    text = file.read_text(encoding="utf-8")
    for number, line in enumerate(text.splitlines(), start=1):
        if ALLOW_MARKER in line:
            continue
        for match in BANNED_CALL.finditer(LINE_COMMENT.sub("", line)):
            name = match.group(1)
            found.append(
                f"{display(file)}:{number}: {name} returns a borrowed reference, which another "
                f"thread can invalidate on free-threaded CPython; use {BANNED[name]} instead, "
                f"or end the line with `{ALLOW_MARKER} <why it is safe>`"
            )
    return found


def display(file: Path) -> str:
    """`file` relative to the repository when it is inside it."""
    resolved = file.resolve()
    if resolved.is_relative_to(REPOSITORY_ROOT):
        return resolved.relative_to(REPOSITORY_ROOT).as_posix()
    return str(file)


def main(argv: list[str]) -> int:
    paths = [Path(argument) for argument in argv] or list(DEFAULT_PATHS)
    missing = [str(path) for path in paths if not path.exists()]
    if missing:
        print(f"error: no such file or directory: {', '.join(missing)}", file=sys.stderr)
        return 2
    files = rust_files(paths)
    found = [message for file in files for message in violations(file)]
    for message in found:
        print(message, file=sys.stderr)
    if found:
        print(f"{len(found)} borrowed-reference call(s) found", file=sys.stderr)
        return 1
    print(f"{len(files)} Rust files, no borrowed-reference calls")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
