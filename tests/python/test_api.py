"""The public API of the M0 spike: ``porto.run``, ``porto.scope``, ``Scope.spawn``, ``Task``,
``porto.sleep`` and ``porto.checkpoint`` (20-python-api-reference.md, and the parts of
15-structured-concurrency-and-cancellation.md without cancellation).
"""

import contextvars
import os
import signal
import sys
import threading
import time
from collections.abc import Generator, Iterator
from typing import Any

import pytest

import porto
from porto import _core

# porto.run


def test_run_calls_an_async_function_with_its_arguments() -> None:
    async def main(a: int, b: str) -> str:
        await porto.checkpoint()
        return f"{a}{b}"

    assert porto.run(main, 1, "x") == "1x"


def test_run_accepts_a_coroutine_object() -> None:
    async def main() -> int:
        await porto.sleep(0)
        return 7

    assert porto.run(main()) == 7


def test_run_accepts_another_awaitable() -> None:
    class Awaitable:
        def __await__(self) -> Generator[Any, Any, str]:
            yield from _core._yield_trap().__await__()
            return "awaited"

    assert porto.run(Awaitable()) == "awaited"


def test_run_raises_the_exception_of_main() -> None:
    async def main() -> None:
        await porto.checkpoint()
        raise LookupError("from main")

    with pytest.raises(LookupError, match="from main"):
        porto.run(main)


def test_run_rejects_a_sync_function_and_says_what_to_do() -> None:
    def main() -> int:
        return 1

    with pytest.raises(TypeError, match=r"main\(\) returned int.*define it with `async def`"):
        porto.run(main)  # type: ignore[arg-type]


def test_run_rejects_a_coroutine_object_with_arguments() -> None:
    async def main(value: int) -> int:
        return value

    coro = main(1)
    with pytest.raises(TypeError, match="got a coroutine object and arguments"):
        porto.run(coro, 2)  # type: ignore[call-arg]
    coro.close()


def test_run_rejects_something_that_is_not_awaitable() -> None:
    with pytest.raises(TypeError, match="expects a coroutine object or an async function, got int"):
        porto.run(42)  # type: ignore[arg-type]


def test_run_inside_a_running_runtime_raises_and_says_what_to_do() -> None:
    async def inner() -> None:
        pass

    async def main() -> None:
        porto.run(inner)

    with pytest.raises(RuntimeError, match=r"inside a running porto runtime; `await`"):
        porto.run(main)


def test_run_can_run_again_after_it_finished() -> None:
    async def main(value: int) -> int:
        await porto.sleep(0.001)
        return value

    assert [porto.run(main, value) for value in range(3)] == [0, 1, 2]


# Scopes and tasks


async def double(value: int) -> int:
    await porto.checkpoint()
    return value * 2


def test_spawn_accepts_a_coroutine_object_and_a_function_with_arguments() -> None:
    async def main() -> tuple[int, int]:
        async with porto.scope() as s:
            from_coro = s.spawn(double(1))
            from_function = s.spawn(double, 2)
        return from_coro.result(), from_function.result()

    assert porto.run(main) == (2, 4)


def test_the_scope_exit_waits_for_every_child() -> None:
    finished: list[str] = []

    async def child(name: str, delay: float) -> None:
        await porto.sleep(delay)
        finished.append(name)

    async def main() -> list[str]:
        async with porto.scope() as s:
            s.spawn(child, "slow", 0.03)
            s.spawn(child, "fast", 0.01)
            s.spawn(child, "now", 0)
        return list(finished)

    assert porto.run(main) == ["now", "fast", "slow"]


def test_task_result_before_the_task_finished_raises_and_says_what_to_do() -> None:
    async def main() -> int:
        async with porto.scope() as s:
            task = s.spawn(double, 21)
            assert not task.done()
            with pytest.raises(
                porto.InvalidStateError, match=r"before the task finished; read it after"
            ):
                task.result()
        assert task.done()
        return task.result()

    assert porto.run(main) == 42


def test_task_result_reraises_what_the_task_raised() -> None:
    error = KeyError("child")
    tasks: list[porto.Task[None]] = []

    async def fail() -> None:
        raise error

    async def main() -> None:
        async with porto.scope() as s:
            tasks.append(s.spawn(fail))

    with pytest.raises(ExceptionGroup):
        porto.run(main)
    with pytest.raises(KeyError) as raised:
        tasks[0].result()
    assert raised.value is error


def test_nested_scopes_run_their_children() -> None:
    async def leaf(value: int) -> int:
        await porto.sleep(0)
        return value

    async def branch(base: int) -> int:
        async with porto.scope() as s:
            tasks = [s.spawn(leaf, base + offset) for offset in range(3)]
        return sum(task.result() for task in tasks)

    async def main() -> int:
        async with porto.scope() as outer:
            branches = [outer.spawn(branch, base) for base in (0, 10, 20)]
            async with porto.scope() as inner:
                own = inner.spawn(leaf, 100)
        return sum(task.result() for task in branches) + own.result()

    assert porto.run(main) == (0 + 1 + 2) + (10 + 11 + 12) + (20 + 21 + 22) + 100


def test_children_can_spawn_into_the_scope_while_it_waits() -> None:
    log: list[int] = []

    async def main() -> None:
        async def chain(remaining: int) -> None:
            await porto.sleep(0.001)
            log.append(remaining)
            if remaining:
                s.spawn(chain, remaining - 1)

        async with porto.scope() as s:
            s.spawn(chain, 3)

    porto.run(main)
    assert log == [3, 2, 1, 0]


def test_an_empty_scope_exits_right_away() -> None:
    async def main() -> str:
        async with porto.scope():
            pass
        return "done"

    assert porto.run(main) == "done"


# Failures (C4, ADR-009 strict)


async def fail_with(error: BaseException, delay: float = 0) -> None:
    await porto.sleep(delay)
    raise error


def test_one_failing_child_still_raises_an_exception_group() -> None:
    error = ValueError("only one")

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(fail_with, error)
            s.spawn(double, 1)

    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert raised.value.exceptions == (error,)
    assert "porto scope failed" in str(raised.value)


def test_the_group_holds_every_failure_in_the_order_the_children_finished() -> None:
    first, second = ValueError("first"), KeyError("second")

    async def fail_when_cancelled(error: BaseException) -> None:
        try:
            await porto.sleep(30)
        finally:
            raise error

    async def main() -> None:
        async with porto.scope() as s:
            # The first failure cancels the scope, which makes the sibling raise the second.
            s.spawn(fail_when_cancelled, second)
            s.spawn(fail_with, first, 0.01)

    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert raised.value.exceptions == (first, second)


def test_the_body_exception_comes_first_in_the_group() -> None:
    body, child = RuntimeError("body"), ValueError("child")

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(fail_with, child, 0.01)
            raise body

    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert raised.value.exceptions == (body, child)


def test_a_body_exception_without_child_failures_propagates_unchanged() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(double, 1)
            raise LookupError("body only")

    with pytest.raises(LookupError, match="body only"):
        porto.run(main)


def test_siblings_of_a_failing_child_run_to_completion() -> None:
    # The failure cancels the scope (C4), but nothing delivers that cancellation to the
    # siblings yet (porto-szb.36): they run to completion.
    tasks: list[porto.Task[int]] = []

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(fail_with, ValueError("early"))
            tasks.append(s.spawn(double, 5))

    with pytest.raises(ExceptionGroup):
        porto.run(main)
    assert tasks[0].result() == 10


def test_a_child_keyboard_interrupt_arrives_in_a_base_exception_group() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(fail_with, KeyboardInterrupt())

    with pytest.raises(BaseExceptionGroup) as raised:
        porto.run(main)
    assert not isinstance(raised.value, ExceptionGroup)
    assert isinstance(raised.value.exceptions[0], KeyboardInterrupt)


def test_a_detached_task_raising_system_exit_stops_the_run() -> None:
    async def child() -> None:
        await porto.checkpoint()
        raise SystemExit(4)

    async def main() -> None:
        _core._spawn(child())
        await porto.sleep(30)

    started = time.monotonic()
    with pytest.raises(SystemExit) as raised:
        porto.run(main)
    assert raised.value.code == 4
    assert time.monotonic() - started < 5


# Misuse


def test_spawn_after_the_scope_closed_raises() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            pass
        coro = double(1)
        try:
            s.spawn(coro)
        finally:
            coro.close()

    with pytest.raises(RuntimeError, match="scope is closed"):
        porto.run(main)


def test_spawn_before_entering_the_scope_raises() -> None:
    async def main() -> None:
        s = porto.scope()
        s.spawn(double, 1)

    with pytest.raises(RuntimeError, match=r"not entered yet; spawn inside its block"):
        porto.run(main)


def test_a_scope_can_be_entered_only_once() -> None:
    async def main() -> None:
        s = porto.scope()
        async with s:
            pass
        async with s:
            pass

    with pytest.raises(RuntimeError, match="entered only once"):
        porto.run(main)


def test_sleep_and_checkpoint_outside_run_raise_and_say_what_to_do() -> None:
    with pytest.raises(porto.NoRuntimeError, match=r"porto.sleep\(\) was called outside porto.run"):
        porto.sleep(1)
    with pytest.raises(
        porto.NoRuntimeError, match=r"porto.checkpoint\(\) was called outside porto.run"
    ):
        porto.checkpoint()


def test_current_time_outside_run_raises_and_says_what_to_do() -> None:
    with pytest.raises(
        porto.NoRuntimeError, match=r"porto.current_time\(\) was called outside porto.run"
    ):
        porto.current_time()
    with pytest.raises(porto.NoRuntimeError, match=r"use time.monotonic\(\)"):
        porto.monotonic()


def test_a_negative_sleep_is_a_checkpoint() -> None:
    # 14 §3: seconds <= 0 is a pure checkpoint, not an error (more in test_time.py).
    async def main() -> None:
        await porto.sleep(-1)

    porto.run(main)


def test_a_nan_sleep_raises_and_says_what_to_do() -> None:
    async def main() -> None:
        await porto.sleep(float("nan"))

    with pytest.raises(ValueError, match=r"got NaN seconds.*0 or less just yields"):
        porto.run(main)


def test_task_is_subscriptable_at_runtime() -> None:
    alias: Any = porto.Task[int]
    assert alias.__origin__ is porto.Task


# Time


def test_monotonic_is_current_time() -> None:
    assert porto.monotonic is porto.current_time
    assert _core.monotonic is _core.current_time


def test_current_time_counts_from_the_runtime_start_and_never_goes_backwards() -> None:
    async def main() -> list[float]:
        readings = [porto.current_time()]
        for _ in range(1000):
            readings.append(porto.current_time())
        await porto.sleep(0.01)
        readings.append(porto.monotonic())
        return readings

    first = porto.run(main)
    assert 0 <= first[0] < 1
    assert first == sorted(first)
    assert first[-1] - first[0] >= 0.01
    time.sleep(0.02)
    # A new run has a new clock: it starts at zero again.
    second = porto.run(main)
    assert second[0] < first[-1]


def test_sleep_deadlines_are_on_the_runtime_clock() -> None:
    async def main() -> None:
        trap = _core._sleep_trap(0.01)
        assert trap.deadline is not None
        assert porto.current_time() <= trap.deadline <= porto.current_time() + 0.01
        await trap
        assert porto.current_time() >= trap.deadline

    porto.run(main)


def test_sleeps_wake_in_deadline_order_never_early() -> None:
    delays = {"c": 0.03, "a": 0.01, "e": 0.05, "b": 0.02, "d": 0.04}
    woke: list[str] = []

    async def sleeper(name: str, delay: float) -> None:
        # "Never early" is promised on porto's clock, which sleep deadlines are measured on
        # (std's Instant: QueryPerformanceCounter on Windows, as time.perf_counter). Python
        # 3.11's time.monotonic on Windows reads GetTickCount64, which moves in ~15.6 ms
        # ticks, so it can measure a 40 ms sleep as 31 ms (porto-4a9).
        start = porto.current_time()
        await porto.sleep(delay)
        assert porto.current_time() - start >= delay, f"{name} woke early"
        woke.append(name)

    async def main() -> None:
        async with porto.scope() as s:
            for name, delay in delays.items():
                s.spawn(sleeper, name, delay)

    porto.run(main)
    assert woke == ["a", "b", "c", "d", "e"]


def test_checkpoints_interleave_tasks_in_fifo_order() -> None:
    log: list[str] = []

    async def worker(name: str) -> None:
        for round in range(3):
            log.append(f"{name}{round}")
            await porto.checkpoint()

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(worker, "a")
            s.spawn(worker, "b")

    porto.run(main)
    assert log == ["a0", "b0", "a1", "b1", "a2", "b2"]


# Context variables

request_id: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="unset")


def test_a_child_sees_the_context_of_its_parent_at_spawn_time() -> None:
    async def child() -> str:
        await porto.checkpoint()
        return request_id.get()

    async def main() -> tuple[str, str]:
        request_id.set("first")
        async with porto.scope() as s:
            first = s.spawn(child)
            request_id.set("second")
            second = s.spawn(child)
        return first.result(), second.result()

    assert porto.run(main) == ("first", "second")


def test_changes_in_a_child_do_not_leak_back_to_the_parent_or_siblings() -> None:
    async def writer() -> str:
        request_id.set("child")
        await porto.checkpoint()
        return request_id.get()

    async def reader() -> str:
        await porto.checkpoint()
        await porto.checkpoint()
        return request_id.get()

    async def main() -> tuple[str, str, str]:
        request_id.set("parent")
        async with porto.scope() as s:
            written = s.spawn(writer)
            read = s.spawn(reader)
        return written.result(), read.result(), request_id.get()

    token = request_id.set("caller")
    try:
        assert porto.run(main) == ("child", "parent", "parent")
        assert request_id.get() == "caller"
    finally:
        request_id.reset(token)


# Ctrl-C

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="os.kill(SIGINT) ends the process")


@pytest.fixture
def interrupt_soon() -> Iterator[None]:
    """Sends SIGINT to this process 0.1 s from now, from another thread, with Python's default
    handler (which raises KeyboardInterrupt) installed."""
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    timer = threading.Timer(0.1, os.kill, (os.getpid(), signal.SIGINT))
    timer.start()
    try:
        yield
    finally:
        timer.cancel()
        timer.join()
        signal.signal(signal.SIGINT, previous)


@posix_only
@pytest.mark.usefixtures("interrupt_soon")
def test_ctrl_c_during_a_long_sleep_raises_keyboard_interrupt_out_of_run() -> None:
    async def main() -> None:
        await porto.sleep(30)

    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        porto.run(main)
    assert time.monotonic() - started < 2


@posix_only
@pytest.mark.usefixtures("interrupt_soon")
def test_ctrl_c_while_a_scope_waits_closes_its_children() -> None:
    closed: list[str] = []

    async def child(name: str) -> None:
        try:
            await porto.sleep(30)
        finally:
            closed.append(name)

    async def main() -> None:
        try:
            async with porto.scope() as s:
                s.spawn(child, "a")
                s.spawn(child, "b")
        finally:
            closed.append("main")

    started = time.monotonic()
    with pytest.raises(KeyboardInterrupt):
        porto.run(main)
    assert time.monotonic() - started < 2
    assert sorted(closed) == ["a", "b", "main"]


@posix_only
@pytest.mark.usefixtures("interrupt_soon")
def test_ctrl_c_while_main_sleeps_inside_a_scope_body_closes_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unraisable: list[Any] = []
    monkeypatch.setattr(sys, "unraisablehook", unraisable.append)
    closed: list[str] = []

    async def sleeping_child() -> None:
        try:
            await porto.sleep(30)
        finally:
            closed.append("child")

    async def main() -> None:
        try:
            async with porto.scope() as s:
                s.spawn(sleeping_child)
                await porto.sleep(30)
        finally:
            closed.append("main")

    with pytest.raises(KeyboardInterrupt):
        porto.run(main)
    assert sorted(closed) == ["child", "main"]
    assert unraisable == [], "closing main raised something other than GeneratorExit"
