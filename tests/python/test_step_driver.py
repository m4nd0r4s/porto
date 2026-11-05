"""The step driver: coroutines as porto tasks (11-task-model-and-trap-protocol.md §2-§5).

``_core._run(coro)`` runs one coroutine to completion on a fresh single-lane runtime;
``_core._spawn(coro)`` starts another task on it. Tasks yield with ``await _core._yield_trap()``
and sleep with ``await _core._sleep_trap(delay)``.
"""

import asyncio
import contextlib
import contextvars
import sys
import threading
import time
import traceback
import types
from collections.abc import Callable, Coroutine, Generator
from typing import Any

import pytest

import porto
from porto import _core


async def checkpoint() -> None:
    await _core._yield_trap()


async def sleep(delay: float) -> None:
    await _core._sleep_trap(delay)


async def wait_until(condition: Callable[[], bool]) -> None:
    """Yields until ``condition()`` holds (there is no join yet)."""
    while not condition():
        await checkpoint()


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return _core._run(coro)


# Results and exceptions


def test_run_returns_the_coroutine_result() -> None:
    result = object()

    async def main() -> object:
        return result

    assert run(main()) is result


def test_run_raises_the_coroutine_exception_with_its_traceback() -> None:
    error = LookupError("from the task")

    async def fail() -> None:
        await checkpoint()
        raise error

    async def main() -> None:
        await fail()

    with pytest.raises(LookupError) as raised:
        run(main())
    assert raised.value is error
    frames = [frame.name for frame in traceback.extract_tb(raised.value.__traceback__)]
    assert frames[-2:] == ["main", "fail"]


def test_an_exception_keeps_its_cause() -> None:
    async def main() -> None:
        try:
            raise KeyError("inner")
        except KeyError as inner:
            raise RuntimeError("outer") from inner

    with pytest.raises(RuntimeError, match="outer") as raised:
        run(main())
    assert isinstance(raised.value.__cause__, KeyError)


def test_base_exceptions_propagate_too() -> None:
    async def main() -> None:
        await checkpoint()
        raise SystemExit(3)

    with pytest.raises(SystemExit) as raised:
        run(main())
    assert raised.value.code == 3


def test_the_coroutine_is_finished_after_run() -> None:
    async def main() -> int:
        await checkpoint()
        return 1

    coro = main()
    assert run(coro) == 1
    assert coro.cr_frame is None


def test_the_runtime_can_run_again_after_a_failure() -> None:
    async def fail() -> None:
        raise ValueError("first")

    async def succeed() -> str:
        await sleep(0)
        return "second"

    with pytest.raises(ValueError, match="first"):
        run(fail())
    assert run(succeed()) == "second"


# Yield


def test_many_yields_round_trip_through_the_run_queue() -> None:
    async def main(count: int) -> int:
        for _ in range(count):
            assert await _core._yield_trap() is None
        return count

    assert run(main(100_000)) == 100_000


def test_two_tasks_that_yield_interleave_in_fifo_order() -> None:
    log: list[str] = []

    async def worker(name: str, rounds: int) -> None:
        for round in range(rounds):
            log.append(f"{name}{round}")
            await checkpoint()

    async def main() -> None:
        _core._spawn(worker("b", 3))
        await worker("a", 3)

    run(main())
    assert log == ["a0", "b0", "a1", "b1", "a2", "b2"]


def test_a_bare_yield_is_a_yield() -> None:
    log: list[str] = []

    @types.coroutine
    def bare_yield() -> Generator[None, None, None]:
        yield

    async def other() -> None:
        log.append("other")

    async def main() -> None:
        _core._spawn(other())
        await bare_yield()
        log.append("main")

    run(main())
    assert log == ["other", "main"]


def test_spawned_tasks_run_in_spawn_order() -> None:
    log: list[int] = []

    async def child(index: int) -> None:
        log.append(index)

    async def main() -> None:
        for index in range(1_000):
            _core._spawn(child(index))
        await wait_until(lambda: len(log) == 1_000)

    run(main())
    assert log == list(range(1_000))


def test_tasks_still_running_when_main_returns_are_closed() -> None:
    closed: list[bool] = []

    async def forever() -> None:
        try:
            while True:
                await checkpoint()
        finally:
            closed.append(True)

    async def main() -> None:
        _core._spawn(forever())
        await checkpoint()

    run(main())
    assert closed == [True]


def test_a_detached_task_exception_goes_to_the_unraisable_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reported: list[Any] = []
    monkeypatch.setattr(sys, "unraisablehook", reported.append)
    done: list[bool] = []

    async def child() -> None:
        done.append(True)
        raise KeyError("lost")

    async def main() -> str:
        _core._spawn(child())
        await wait_until(lambda: bool(done))
        return "main survives"

    assert run(main()) == "main survives"
    assert len(reported) == 1
    assert isinstance(reported[0].exc_value, KeyError)


# Sleep


def test_a_sleep_returns_none_no_earlier_than_its_deadline() -> None:
    async def main() -> None:
        for delay in (0.0, 0.001, 0.01):
            trap = _core._sleep_trap(delay)
            assert trap.deadline is not None
            assert await trap is None
            assert _core.current_time() >= trap.deadline

    run(main())


def test_sleeps_wake_in_deadline_order_never_early() -> None:
    start = 0.0
    woke: list[str] = []
    delays = {"c": 0.03, "a": 0.01, "e": 0.05, "b": 0.02, "d": 0.04}

    async def sleeper(name: str, delay: float) -> None:
        trap = _core._sleep_trap(delay)
        assert trap.deadline is not None
        await trap
        now = _core.current_time()
        assert now >= trap.deadline >= start + delay, f"{name} woke early"
        woke.append(name)

    async def main() -> None:
        nonlocal start
        start = _core.current_time()
        for name, delay in delays.items():
            _core._spawn(sleeper(name, delay))
        await wait_until(lambda: len(woke) == len(delays))

    run(main())
    assert woke == ["a", "b", "c", "d", "e"]


def test_a_zero_sleep_still_yields() -> None:
    log: list[str] = []

    async def other() -> None:
        log.append("other")

    async def main() -> None:
        _core._spawn(other())
        await sleep(0)
        log.append("main")

    run(main())
    assert log == ["other", "main"]


def test_tasks_run_while_another_sleeps() -> None:
    log: list[str] = []

    async def sleeper() -> None:
        await sleep(0.02)
        log.append("sleeper")

    async def busy() -> None:
        for _ in range(3):
            log.append("busy")
            await checkpoint()

    async def main() -> None:
        _core._spawn(sleeper())
        _core._spawn(busy())
        await wait_until(lambda: "sleeper" in log)

    run(main())
    assert log == ["busy", "busy", "busy", "sleeper"]


def test_other_python_threads_run_while_the_lane_sleeps() -> None:
    # The lane detaches its thread state while parked. If it held it (on a GIL build), the
    # worker could only take the GIL once the sleep is over.
    go = threading.Event()
    worker_ran_at: list[float] = []

    def worker() -> None:
        go.wait()
        worker_ran_at.append(time.monotonic())

    thread = threading.Thread(target=worker)
    thread.start()

    async def main() -> float:
        started = time.monotonic()
        go.set()
        await sleep(0.2)
        return started

    try:
        started = run(main())
    finally:
        go.set()
        thread.join()
    assert worker_ran_at[0] - started < 0.1


# Unsupported awaitables


def test_an_asyncio_future_raises_type_error_inside_the_coroutine() -> None:
    loop = asyncio.new_event_loop()

    async def main() -> str:
        future: asyncio.Future[None] = loop.create_future()
        try:
            await future
        except TypeError as error:
            message = str(error)
            assert "asyncio.Future" in message
            assert "porto.asyncio" in message
            return "caught"
        return "not raised"

    try:
        assert run(main()) == "caught"
    finally:
        loop.close()


def test_an_unsupported_object_raises_at_the_await_that_yielded_it() -> None:
    @types.coroutine
    def yield_a_number() -> Generator[int, None, None]:
        yield 42

    async def main() -> None:
        await yield_a_number()

    with pytest.raises(TypeError, match=r"yielded an object of type int\).*asyncio compatibility"):
        run(main())


def test_a_coroutine_can_recover_from_an_unsupported_await_and_continue() -> None:
    @types.coroutine
    def yield_junk() -> Generator[object, None, None]:
        yield object()

    async def main() -> int:
        with contextlib.suppress(TypeError):
            await yield_junk()
        await sleep(0)
        return 7

    assert run(main()) == 7


def test_a_coroutine_that_returns_from_the_thrown_error_finishes() -> None:
    @types.coroutine
    def yield_junk() -> Generator[object, None, None]:
        yield "junk"

    async def main() -> str:
        try:
            await yield_junk()
        except TypeError:
            return "returned from the except"
        return "not raised"

    assert run(main()) == "returned from the except"


def test_a_trap_yielded_without_await_raises_runtime_error() -> None:
    @types.coroutine
    def yield_a_trap() -> Generator[object, None, None]:
        yield _core._yield_trap()

    async def main() -> None:
        await yield_a_trap()

    with pytest.raises(RuntimeError, match="without being awaited"):
        run(main())


# Nested coroutine chains


def test_nested_coroutine_chains_route_traps_and_results() -> None:
    async def descend(depth: int) -> int:
        if depth == 0:
            await checkpoint()
            await sleep(0.001)
            return 0
        await checkpoint()
        return 1 + await descend(depth - 1)

    async def main() -> int:
        return await descend(50)

    assert run(main()) == 50


def test_an_exception_unwinds_a_nested_chain_to_the_frame_that_catches_it() -> None:
    async def leaf() -> None:
        await sleep(0)
        raise ValueError("deep")

    async def middle() -> None:
        await leaf()

    async def main() -> str:
        try:
            await middle()
        except ValueError as error:
            await checkpoint()
            return f"caught {error}"
        return "not raised"

    assert run(main()) == "caught deep"


def test_custom_awaitables_that_delegate_to_traps_work() -> None:
    class Yielder:
        def __await__(self) -> Generator[Any, Any, str]:
            yield from _core._yield_trap().__await__()
            return "custom"

    async def main() -> str:
        return await Yielder()

    assert run(main()) == "custom"


def test_readies_complete_without_suspending() -> None:
    async def main() -> object:
        return await _core._make_ready("ready")

    assert run(main()) == "ready"


# Context variables

request_id: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="unset")


def test_a_task_runs_in_a_copy_of_the_context_it_was_spawned_from() -> None:
    seen: dict[str, str] = {}

    async def child() -> None:
        seen["child at start"] = request_id.get()
        request_id.set("child")
        await checkpoint()
        seen["child after yield"] = request_id.get()

    async def main() -> None:
        seen["main at start"] = request_id.get()
        request_id.set("main")
        _core._spawn(child())
        await wait_until(lambda: "child after yield" in seen)
        seen["main at end"] = request_id.get()

    token = request_id.set("caller")
    try:
        run(main())
        assert request_id.get() == "caller"
    finally:
        request_id.reset(token)
    assert seen == {
        "main at start": "caller",
        "child at start": "main",
        "child after yield": "child",
        "main at end": "main",
    }


# Misuse


def test_run_rejects_an_async_function_with_a_hint() -> None:
    async def main() -> None:
        pass

    with pytest.raises(TypeError, match=r"call the async function: _run\(main\(\)\)"):
        _core._run(main)  # type: ignore[arg-type]


def test_run_rejects_a_non_coroutine() -> None:
    with pytest.raises(TypeError, match="expects a coroutine object, got int"):
        _core._run(42)  # type: ignore[arg-type]


def test_run_cannot_be_nested() -> None:
    async def inner() -> None:
        pass

    async def main() -> None:
        coro = inner()
        try:
            _core._run(coro)
        finally:
            coro.close()

    with pytest.raises(RuntimeError, match="cannot start a runtime inside a running porto task"):
        run(main())


def test_spawn_outside_a_task_raises() -> None:
    async def child() -> None:
        pass

    coro = child()
    with pytest.raises(porto.NoRuntimeError, match="must be called from a task running under _run"):
        _core._spawn(coro)
    coro.close()
