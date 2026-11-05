"""Free-threaded CPython (25-cpython-integration-and-memory.md §1, ADR-024): porto._core declares
``Py_MOD_GIL_NOT_USED``, so importing porto on 3.14t leaves the GIL off.

Each check runs in a fresh interpreter: the test process already imported porto, and whatever
else pytest loaded may have turned the GIL on for its own reasons.
"""

import os
import subprocess
import sys
import sysconfig

import pytest

FREE_THREADED = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))


def run_python(code: str) -> subprocess.CompletedProcess[str]:
    # PYTHON_GIL=1 (or -X gil=1) would force the GIL on whatever porto declares.
    environment = {name: value for name, value in os.environ.items() if name != "PYTHON_GIL"}
    return subprocess.run(
        [sys.executable, "-W", "error::RuntimeWarning", "-c", code],
        capture_output=True,
        text=True,
        check=False,
        env=environment,
    )


@pytest.mark.skipif(not FREE_THREADED, reason="needs a free-threaded build (3.14t)")
def test_importing_porto_does_not_enable_the_gil() -> None:
    result = run_python(
        "import sys\n"
        "assert not sys._is_gil_enabled(), 'the GIL was on before porto was imported'\n"
        "import porto, porto._core\n"
        "print(sys._is_gil_enabled())\n"
    )
    # A module without Py_MOD_GIL_NOT_USED makes CPython warn (an error here) and enable the GIL.
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "False"


@pytest.mark.skipif(not FREE_THREADED, reason="needs a free-threaded build (3.14t)")
def test_running_tasks_does_not_enable_the_gil() -> None:
    result = run_python(
        "import sys, porto\n"
        "async def child():\n"
        "    await porto.sleep(0)\n"
        "async def main():\n"
        "    async with porto.scope() as scope:\n"
        "        for _ in range(10):\n"
        "            scope.spawn(child)\n"
        "porto.run(main)\n"
        "print(sys._is_gil_enabled())\n"
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "False"


@pytest.mark.skipif(FREE_THREADED, reason="checks the GIL build")
def test_porto_imports_on_a_gil_build() -> None:
    result = run_python("import porto, porto._core\nprint(porto.__version__)\n")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip()
