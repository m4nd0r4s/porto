"""The panic policy (ADR-027, 25-cpython-integration-and-memory.md §8, ST-27).

A panic inside a task fails that task with ``porto.RustPanic`` (message and location), which
propagates into its scope like any exception; the lane keeps running. A panic in scheduler
internals aborts the process.
"""

import signal
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import porto
from porto import _core

REPO = Path(__file__).resolve().parents[2]


def _panic_of(error: BaseException) -> porto.RustPanic:
    """The one ``RustPanic`` in ``error`` (a scope raises a lone failure bare, several grouped)."""
    errors = error.exceptions if isinstance(error, BaseExceptionGroup) else (error,)
    panics = [each for each in errors if isinstance(each, porto.RustPanic)]
    assert len(panics) == 1, errors
    return panics[0]


def test_a_panicking_rust_future_fails_its_task_with_rust_panic_and_the_lane_keeps_running() -> (
    None
):
    finished: list[int] = []

    async def survivor() -> None:
        for _ in range(5):
            await porto.sleep(0)
        finished.append(1)

    async def victim() -> None:
        await porto.sleep(0)
        await _core._panicking_future("the future broke")

    async def main() -> porto.RustPanic:
        async with porto.scope() as outer:
            outer.spawn(survivor)
            try:
                async with porto.scope() as inner:
                    inner.spawn(victim)
                    await porto.sleep(10)
            except BaseException as error:
                return _panic_of(error)
        raise AssertionError("the panic did not reach the scope")

    panic = porto.run(main)
    assert panic.message == "the future broke"
    assert panic.location is not None
    assert "errors.rs" in panic.location
    assert finished == [1], "the lane ran the other task to the end"


def test_a_joiner_of_the_panicked_task_gets_rust_panic_and_main_goes_on() -> None:
    async def victim() -> None:
        await _core._panicking_future("boom")

    async def main() -> str:
        async with porto.scope() as scope:
            handle = scope.spawn(victim)
            with pytest.raises(porto.RustPanic, match="boom"):
                await handle
        return "main finished"

    # The failed child cancels its scope and the scope re-raises it on exit, but the lane lives
    # on: the joiner above already saw the panic.
    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert _panic_of(raised.value).message == "boom"


def test_a_panic_in_a_pyfunction_called_by_a_task_fails_the_task_with_rust_panic() -> None:
    async def victim() -> None:
        _core._panic_in_pyfunction("inside a pyfunction")

    async def main() -> None:
        async with porto.scope() as scope:
            scope.spawn(victim)

    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    panic = _panic_of(raised.value)
    assert panic.message == "inside a pyfunction"
    assert panic.location is not None
    assert "errors.rs" in panic.location
    assert type(panic.__cause__).__name__ == "PanicException"


def test_a_task_that_handles_the_panic_exception_keeps_running() -> None:
    async def main() -> str:
        try:
            _core._panic_in_pyfunction("handled")
        except BaseException as error:
            return type(error).__name__
        return "no error"

    assert porto.run(main) == "PanicException"


def test_a_panic_leaves_the_runtime_usable_for_the_next_run() -> None:
    async def victim() -> None:
        await _core._panicking_future("first run")

    async def main() -> None:
        async with porto.scope() as scope:
            scope.spawn(victim)

    with pytest.raises(ExceptionGroup):
        porto.run(main)

    async def fine() -> int:
        await porto.sleep(0)
        return 7

    assert porto.run(fine) == 7


def _run_child(body: str) -> subprocess.CompletedProcess[str]:
    code = "import porto\nfrom porto import _core\n" + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        cwd=REPO,
    )


def test_a_panic_in_scheduler_internals_logs_and_aborts_the_process() -> None:
    child = _run_child(
        """
        async def main():
            _core._arm_scheduler_panic()
            await porto.sleep(30)

        porto.run(main)
        print("not reached")
        """
    )
    assert "not reached" not in child.stdout
    assert "a Rust panic in the scheduler" in child.stderr
    assert "test hook: a panic in the scheduler" in child.stderr
    if sys.platform == "win32":
        assert child.returncode != 0
    else:
        assert child.returncode == -signal.SIGABRT


def test_a_task_panic_does_not_abort_the_process() -> None:
    child = _run_child(
        """
        async def victim():
            await _core._panicking_future("task-level")

        async def main():
            async with porto.scope() as scope:
                scope.spawn(victim)

        try:
            porto.run(main)
        except BaseException as error:
            print("survived", type(error).__name__)
        """
    )
    assert child.returncode == 0, child.stderr
    assert "survived" in child.stdout


def test_the_cargo_profiles_keep_panic_unwind() -> None:
    """ADR-027: the build also refuses `panic = "abort"` (``compile_error!`` in porto-core and
    porto-py), this pins the profiles that would set it."""
    import tomllib

    profiles = tomllib.loads((REPO / "Cargo.toml").read_text())["profile"]
    assert profiles["release"]["panic"] == "unwind"
    assert profiles["dev"]["panic"] == "unwind"
    assert profiles["dist"].get("panic", "unwind") == "unwind"
    assert profiles["dist"]["inherits"] == "release"
    for config in (REPO / ".cargo" / "config.toml", REPO / ".cargo" / "config"):
        if config.exists():
            assert "panic=abort" not in config.read_text().replace(" ", "")
