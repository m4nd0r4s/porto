"""The shutdown sequence (26-errors-config-lifecycle.md §2 steps 3-6, 12 §11, ST-20).

``Runtime.shutdown(timeout)`` and the signals (``test_signal_defaults.py``) start it: the
``porto.lifecycle.shutdown_requested()`` event is set, main gets the graceful period to finish by
itself, then the root scope is cancelled and given ``cancel_timeout``, and what is left is
abandoned.
"""

import os
import sys
import threading
import time
import warnings
from typing import Any

import pytest

import porto
from porto import _core


def recorded(function: Any) -> list[str]:
    """The messages of the warnings ``function()`` emitted."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            function()
        except BaseException as error:
            return [f"raised {error!r}", *(str(warning.message) for warning in caught)]
    return [str(warning.message) for warning in caught]


def test_the_event_is_unset_until_a_shutdown_is_requested() -> None:
    async def main() -> tuple[bool, bool]:
        event = porto.lifecycle.shutdown_requested()
        before = event.is_set()
        porto.runtime().shutdown()
        await porto.checkpoint()
        return before, event.is_set()

    assert porto.run(main) == (False, True)


def test_the_event_needs_a_running_runtime() -> None:
    with pytest.raises(porto.NoRuntimeError, match=r"shutdown_requested"):
        porto.lifecycle.shutdown_requested()


def test_main_that_finishes_within_the_graceful_timeout_returns_normally() -> None:
    seen: list[str] = []

    async def main() -> str:
        porto.runtime().shutdown(timeout=5)
        await porto.lifecycle.shutdown_requested().wait()
        seen.append("requested")
        await porto.sleep(0.05)
        seen.append("finished")
        return "done"

    started = time.monotonic()
    assert porto.run(main) == "done"
    assert seen == ["requested", "finished"]
    assert time.monotonic() - started < 2, "a main that ended was not waited out"


def test_shutdown_from_another_thread_returns_once_the_runtime_stopped() -> None:
    runtime = porto.Runtime()
    ready = threading.Event()
    results: list[Any] = []

    async def main() -> str:
        ready.set()
        await porto.lifecycle.shutdown_requested().wait()
        return "stopped by itself"

    thread = threading.Thread(target=lambda: results.append(runtime.block_on(main)))
    thread.start()
    assert ready.wait(10)
    runtime.shutdown(timeout=5)
    assert repr(runtime) == "<porto.Runtime stopped>", "shutdown returned before the run ended"
    thread.join(10)
    assert results == ["stopped by itself"]


def test_main_that_ignores_the_request_is_cancelled_after_the_graceful_timeout() -> None:
    closed: list[str] = []

    async def main() -> None:
        try:
            await porto.sleep(30)
        finally:
            closed.append("main")

    async def requester(runtime: porto.Runtime) -> None:
        await porto.sleep(0.05)
        runtime.shutdown(timeout=0.2)

    async def run_it() -> None:
        runtime = porto.runtime()
        porto.runtime().root_scope.spawn(requester, runtime)
        await main()

    started = time.monotonic()
    with pytest.raises(porto.Cancelled):
        porto.run(run_it)
    elapsed = time.monotonic() - started
    assert closed == ["main"], "the cancellation ran main's finally"
    assert 0.2 <= elapsed < 3, f"cancelled after the graceful timeout, took {elapsed:.2f} s"


def test_a_task_that_outlives_cancel_timeout_is_abandoned_with_a_warning_naming_it() -> None:
    async def stuck() -> None:
        try:
            with porto.shield():
                await porto.sleep(30)
        except BaseException:  # the GeneratorExit of close(), swallowed
            await _core._yield_trap()

    async def main() -> None:
        porto.runtime().root_scope.spawn(stuck, name="stuck-one")
        await porto.sleep(0.01)

    messages = recorded(lambda: porto.run(main, cancel_timeout=0.1))
    text = "\n".join(messages)
    assert "1 task(s) of the root scope" in text, text
    assert "could not be closed" in text, text
    assert "stuck-one" in text, text
    assert "test_lifecycle_shutdown.py" in text, text
    assert "in stuck" in text, text


def test_a_task_whose_cleanup_only_closes_is_abandoned_without_the_close_warning() -> None:
    cleaned: list[str] = []

    async def slow() -> None:
        try:
            with porto.shield():
                await porto.sleep(30)
        finally:
            cleaned.append("slow")

    async def main() -> None:
        porto.runtime().root_scope.spawn(slow, name="slow")
        await porto.sleep(0.01)

    messages = recorded(lambda: porto.run(main, cancel_timeout=0.1))
    text = "\n".join(messages)
    assert "1 task(s) of the root scope" in text, text
    assert "could not be closed" not in text, text
    assert cleaned == ["slow"], "closing the coroutine ran its synchronous finally"


def test_a_forced_shutdown_abandons_at_once() -> None:
    cleaned: list[str] = []

    async def main() -> None:
        try:
            with porto.shield():
                await porto.sleep(30)
        finally:
            cleaned.append("main")

    async def run_it() -> None:
        porto.runtime().root_scope.spawn(requester)
        await main()

    async def requester() -> None:
        await porto.sleep(0.05)
        porto.runtime().shutdown(timeout=0)

    started = time.monotonic()
    messages = recorded(lambda: porto.run(run_it, cancel_timeout=0.3))
    assert time.monotonic() - started < 3
    assert any("abandoned" in message for message in messages), messages
    assert cleaned == ["main"]


def open_descriptors() -> int:
    return len(os.listdir("/dev/fd"))


def test_a_run_leaves_no_threads_and_no_descriptors_behind() -> None:
    async def work() -> int:
        return 7

    async def main() -> int:
        async with porto.scope() as scope:
            scope.spawn(porto.to_thread, sum, [1, 2, 3])
            scope.spawn(work)
        porto.runtime().shutdown(timeout=1)
        await porto.lifecycle.shutdown_requested().wait()
        return await porto.to_thread(sum, [4, 5])

    # The first run warms process-wide state (imports, lazily opened descriptors).
    assert porto.run(main) == 9
    threads = threading.active_count()
    descriptors = open_descriptors() if sys.platform != "win32" else 0
    for _ in range(3):
        assert porto.run(main) == 9
    deadline = time.monotonic() + 3
    while threading.active_count() > threads and time.monotonic() < deadline:
        time.sleep(0.01)
    assert threading.active_count() == threads, "a lane or pool thread outlived porto.run"
    if sys.platform != "win32":
        assert open_descriptors() == descriptors, "porto.run leaked a file descriptor"
