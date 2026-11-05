"""scripts/check_borrowed_refs.py, the CI lint that bans borrowed-reference C APIs in porto-py
(25-cpython-integration-and-memory.md §3): it passes on the crate and fails on a planted call."""

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPOSITORY_ROOT / "scripts" / "check_borrowed_refs.py"
CRATE_SOURCE = REPOSITORY_ROOT / "crates" / "porto-py" / "src"


def lint(*paths: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, paths)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_porto_py_has_no_borrowed_reference_calls() -> None:
    result = lint()
    assert result.returncode == 0, result.stderr
    assert "no borrowed-reference calls" in result.stdout


def test_a_call_planted_in_the_crate_fails_the_lint(tmp_path: Path) -> None:
    source = tmp_path / "src"
    shutil.copytree(CRATE_SOURCE, source)
    step = source / "driver" / "step.rs"
    lines = step.read_text(encoding="utf-8").splitlines()
    lines.append("fn planted(d: *mut ffi::PyObject, k: *mut ffi::PyObject) {")
    lines.append("    let _ = unsafe { ffi::PyDict_GetItem(d, k) };")
    lines.append("}")
    step.write_text("\n".join(lines) + "\n", encoding="utf-8")

    result = lint(source)

    assert result.returncode == 1
    assert f"step.rs:{len(lines) - 1}: PyDict_GetItem returns a borrowed reference" in (
        result.stderr
    )
    assert "use PyDict_GetItemRef instead" in result.stderr


@pytest.mark.parametrize(
    "call",
    [
        "ffi::PyList_GetItem(list, 0)",
        "ffi::PyList_GET_ITEM(list, 0)",
        "ffi::PyWeakref_GetObject(reference)",
        "ffi::PyDict_GetItemWithError(d, k)",
        "ffi::PyDict_GetItemString(d, name)",
        "ffi::PyDict_Next(d, &mut position, &mut key, &mut value)",
        "ffi::PyImport_AddModule(name)",
    ],
)
def test_every_banned_api_is_caught(tmp_path: Path, call: str) -> None:
    planted = tmp_path / "planted.rs"
    planted.write_text(f"fn f() {{ unsafe {{ {call} }}; }}\n", encoding="utf-8")
    result = lint(planted)
    assert result.returncode == 1
    assert call.split("(")[0].removeprefix("ffi::") in result.stderr


def test_strong_reference_apis_comments_and_marked_lines_pass(tmp_path: Path) -> None:
    clean = tmp_path / "clean.rs"
    clean.write_text(
        "\n".join(
            [
                "//! Never call PyDict_GetItem here.",
                "fn f() {",
                "    unsafe { ffi::PyDict_GetItemRef(d, k, &mut out) }; // not PyList_GetItem",
                "    unsafe { ffi::PyList_GetItemRef(list, 0) };",
                "    unsafe { ffi::PyWeakref_GetRef(reference, &mut out) };",
                "    unsafe { ffi::PyDict_Next(d, p, k, v) }; // borrowed-ref: critical section",
                "}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    result = lint(clean)
    assert result.returncode == 0, result.stderr


def test_a_missing_path_is_an_error(tmp_path: Path) -> None:
    result = lint(tmp_path / "missing")
    assert result.returncode == 2
    assert "no such file or directory" in result.stderr
