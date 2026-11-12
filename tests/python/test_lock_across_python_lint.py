"""scripts/check_lock_across_python.py, the CI lint for rule P3 (25-cpython-integration-and-
memory.md §2): no Rust lock is held across a call that can run Python code. It passes on porto-py
and fails on a planted guard."""

import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "scripts" / "check_lock_across_python.py"


def lint(*paths: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, paths)],
        capture_output=True,
        text=True,
        check=False,
    )


def planted(tmp_path: Path, *lines: str) -> Path:
    source = tmp_path / "planted.rs"
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return source


def test_porto_py_holds_no_lock_across_a_call_into_python() -> None:
    result = lint()
    assert result.returncode == 0, result.stderr
    assert "no lock held across a call into Python" in result.stdout


@pytest.mark.parametrize(
    "call",
    [
        "callback.call0(py)?;",
        "callback.call1(py, (1,))?;",
        "object.getattr(py, name)?;",
        "module.call_method0(py, name)?;",
        "Python::attach(|py| drop(value));",
        "unsafe { ffi::PyObject_CallObject(f, a) };",
        "unsafe { ffi::PyIter_Send(it, v, &mut out) };",
    ],
)
def test_a_guard_held_across_a_python_call_fails_the_lint(tmp_path: Path, call: str) -> None:
    source = planted(
        tmp_path,
        "fn f(state: &Mutex<State>, callback: Py<PyAny>) {",
        "    let guard = state.lock();",
        f"    {call}",
        "}",
    )
    result = lint(source)
    assert result.returncode == 1
    assert "planted.rs:3:" in result.stderr
    assert "while the lock guard `guard` (line 2) is held" in result.stderr
    assert "drop the guard first" in result.stderr


@pytest.mark.parametrize("acquire", ["x.lock()", "x.lock_state()", "x.read()", "x.write()"])
def test_every_kind_of_guard_is_caught(tmp_path: Path, acquire: str) -> None:
    source = planted(
        tmp_path,
        "fn f() {",
        f"    let mut guard = {acquire};",
        "    callback.call0(py)?;",
        "}",
    )
    assert lint(source).returncode == 1


def test_a_guard_bound_across_several_lines_is_caught(tmp_path: Path) -> None:
    # rustfmt's shape for a long chain: the receiver, then `.lock()` on a line of its own.
    source = planted(
        tmp_path,
        "fn f(registry: &Registry) {",
        "    let mut guard = registry",
        "        .entries",
        "        .lock()",
        "        .unwrap_or_else(PoisonError::into_inner);",
        "    callback.call0(py)?;",
        "}",
    )
    result = lint(source)
    assert result.returncode == 1
    assert "planted.rs:6:" in result.stderr
    assert "guard `guard` (line 2)" in result.stderr


def test_a_multi_line_binding_that_is_not_a_guard_or_is_marked_passes(tmp_path: Path) -> None:
    source = planted(
        tmp_path,
        "fn f(registry: &Registry) {",
        "    let copy = registry",
        "        .entries",
        "        .clone();",
        "    callback.call0(py)?;",
        "    let guard = registry",
        "        .entries",
        "        .lock(); // lock-across-python: the callee never re-enters porto",
        "    callback.call0(py)?;",
        "}",
    )
    result = lint(source)
    assert result.returncode == 0, result.stderr


def test_a_guard_dropped_or_out_of_scope_before_the_call_passes(tmp_path: Path) -> None:
    source = planted(
        tmp_path,
        "fn dropped(state: &Mutex<State>) {",
        "    let guard = state.lock();",
        "    let taken = guard.take();",
        "    drop(guard);",
        "    taken.call0(py)?;",
        "}",
        "fn scoped(state: &Mutex<State>) {",
        "    let taken = {",
        "        let guard = state.lock();",
        "        guard.take()",
        "    };",
        "    taken.call0(py)?;",
        "}",
        "fn nested(state: &Mutex<State>) {",
        "    if ready {",
        "        let guard = state.lock();",
        "        guard.push(1);",
        "    }",
        "    callback.call0(py)?;",
        "}",
    )
    result = lint(source)
    assert result.returncode == 0, result.stderr


def test_comments_strings_and_marked_bindings_pass(tmp_path: Path) -> None:
    source = planted(
        tmp_path,
        "fn f(state: &Mutex<State>) {",
        "    let guard = state.lock();",
        "    // callback.call0(py) would deadlock here",
        '    log("callback.call0(py)");',
        "    drop(guard);",
        "}",
        "fn marked(state: &Mutex<State>) {",
        "    let guard = state.lock(); // lock-across-python: the callee is a plain function",
        "    callback.call0(py)?;",
        "}",
    )
    result = lint(source)
    assert result.returncode == 0, result.stderr


def test_a_missing_path_is_an_error(tmp_path: Path) -> None:
    result = lint(tmp_path / "nowhere")
    assert result.returncode == 2
    assert "no such file or directory" in result.stderr
