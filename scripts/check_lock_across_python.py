"""Reject a Rust lock held across a call into Python in porto-py (25-cpython-integration-and-
memory.md §2 P3).

    uv run python scripts/check_lock_across_python.py             # check crates/porto-py
    uv run python scripts/check_lock_across_python.py PATH ...    # check other files or directories

Python code may re-enter porto: a `__del__`, a descriptor, a `__eq__`, a coroutine's `close()`.
If it runs while this thread holds a lock that the re-entry takes, the thread deadlocks on itself
(and on a lane that is every task). So no lock guard may be alive at a call that can run Python
code: copy what is needed out of the lock, drop the guard, then call.

This is a lint, not a proof. It looks for a lock guard bound with `let` (`let guard =
x.lock()`, `lock_state()`, `read()`, `write()`, `try_lock()`) and flags a call that can run
Python code between that line and the end of the guard's block or an explicit `drop(guard)`:
`call*`, `getattr`/`setattr`, `get_item`/`set_item`, `is_truthy`, `write_unraisable`,
`Python::attach`, and the `ffi::` calls that run code. It does not see a guard that is a
temporary (`x.lock().push(y.call0()?)`), a guard held by a callee, or a drop of the last
reference to a Python object (which runs its finalizer) while a guard is alive; reviewers still
have to. Comments are not checked. A binding that is safe anyway (the callee never re-enters
porto) ends its line with `// lock-across-python: <why>`.

Exit status: 0 when clean, 1 when a lock is held across a call, 2 when a path does not exist.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PATHS = (REPOSITORY_ROOT / "crates" / "porto-py",)
ALLOW_MARKER = "// lock-across-python:"

#: `let guard = <expr>.lock()...;`: a guard bound for the rest of its block. Matched against the
#: whole statement, joined onto one line: rustfmt breaks a long chain (`let guard = state` then
#: `.lock()` on the next line).
GUARD_BINDING = re.compile(
    r"^\s*let\s+(?:mut\s+)?(?P<name>[A-Za-z_]\w*)\s*(?::[^=]+)?=\s*[^;]*"
    r"\.\s*(?:lock|lock_state|read|write|try_lock|lock_arc)\s*\(\)"
)
#: How many lines a `let` statement may span before the lint gives up on it.
MAX_STATEMENT_LINES = 12
#: Calls that can run Python code.
PYTHON_CALL = re.compile(
    r"\.(?:call|call0|call1|call_method|call_method0|call_method1|call_method_bound|getattr"
    r"|setattr|get_item|set_item|is_truthy|write_unraisable|repr|str)\("
    r"|\bPython::attach\b|\bPython::with_gil\b"
    r"|\bffi::(?:PyObject_(?:Call|GetAttr|SetAttr|Str|Repr|IsTrue|RichCompare)\w*"
    r"|PyIter_Send|PyErr_(?:CheckSignals|WarnEx)|PyContext_(?:Enter|Exit|Copy\w*)"
    r"|PyBuffer_Release|PyCFunction_\w+|PyEval_\w+)\b"
)
LINE_COMMENT = re.compile(r"//.*$")
STRING_LITERAL = re.compile(r'"(?:\\.|[^"\\])*"')


def rust_files(paths: list[Path]) -> list[Path]:
    """The `.rs` files under `paths` (files are taken as they are), skipping build output."""
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(file for file in sorted(path.rglob("*.rs")) if "target" not in file.parts)
        else:
            files.append(path)
    return files


def code_of(line: str) -> str:
    """`line` without string literals and the trailing comment."""
    return LINE_COMMENT.sub("", STRING_LITERAL.sub('""', line))


def brace_delta(code: str) -> int:
    return code.count("{") - code.count("}")


def statement_at(lines: list[str], start: int) -> tuple[str, int]:
    """The code of the statement that begins at line `start`, on one line, and the index of its
    last line: the lines up to the first one that ends in `;` (or `{`, for a block expression)."""
    parts: list[str] = []
    end = start
    for end in range(start, min(start + MAX_STATEMENT_LINES, len(lines))):
        code = code_of(lines[end])
        parts.append(code)
        if code.rstrip().endswith((";", "{", "}")):
            break
    return " ".join(part.strip() for part in parts), end


def violations(file: Path) -> list[str]:
    """`file:line: message` for each lock guard alive at a call that can run Python code."""
    lines = file.read_text(encoding="utf-8").splitlines()
    found: list[str] = []
    for start, line in enumerate(lines):
        if not line.lstrip().startswith("let "):
            continue
        statement, last = statement_at(lines, start)
        binding = GUARD_BINDING.match(statement)
        if binding is None or any(ALLOW_MARKER in lines[n] for n in range(start, last + 1)):
            continue
        name = binding.group("name")
        released = re.compile(rf"\bdrop\(\s*{re.escape(name)}\s*\)")
        # The statement's last line may open a block (`let g = x.lock(); if ... {`): depth is
        # measured from the line after it.
        depth = 0
        for number in range(last + 1, len(lines)):
            code = code_of(lines[number])
            if released.search(code):
                break
            depth += brace_delta(code)
            if depth < 0:
                break  # the guard's block ended
            call = PYTHON_CALL.search(code)
            if call is not None and ALLOW_MARKER not in lines[number]:
                found.append(
                    f"{display(file)}:{number + 1}: `{call.group(0).strip('.(')}` can run Python "
                    f"code while the lock guard `{name}` (line {start + 1}) is held, and Python "
                    f"may re-enter porto and deadlock on it (25 P3); drop the guard first, or end "
                    f"line {start + 1} with `{ALLOW_MARKER} <why it cannot re-enter>`"
                )
                break
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
        print(f"{len(found)} lock(s) held across a call into Python found", file=sys.stderr)
        return 1
    print(f"{len(files)} Rust files, no lock held across a call into Python")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
