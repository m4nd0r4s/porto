"""``Scope.start()`` and ``Started[T]`` (15-structured-concurrency-and-cancellation.md §3).

``await s.start(fn, *args)`` runs ``fn(*args, started=Started())`` as a task in a hidden scope
the caller waits in, until the task calls ``started.set(value)``: then ``start`` returns
``value`` and the task is a child of ``s`` (Trio's ``nursery.start``). What the task raises
before ``set`` is raised from ``start`` as it is, and cancelling the caller cancels the task.
"""

import contextvars

import pytest

import porto
from porto._core import _spawn as _core_spawn  # pyright: ignore[reportPrivateUsage]


async def server(port: int, *, started: porto.Started[int]) -> None:
    started.set(port + 1)
    await porto.sleep_forever()


async def settle() -> None:
    for _ in range(3):
        await porto.checkpoint()


def test_start_returns_the_value_the_task_set_and_leaves_the_task_running_in_the_scope() -> None:
    async def main() -> tuple[int, str]:
        async with porto.scope() as s:
            port = await s.start(server, 8000)
            await settle()
            state = repr(s)
            s.cancel()
        return port, state

    port, state = porto.run(main)
    assert port == 8001
    assert "1 live tasks" in state, "the task counts in the scope start was called on"


def test_set_without_a_value_returns_none() -> None:
    async def ready(*, started: porto.Started[None]) -> None:
        started.set()

    async def main() -> None:
        async with porto.scope() as s:
            assert await s.start(ready) is None

    porto.run(main)


def test_start_passes_the_positional_arguments_and_the_name() -> None:
    names: list[str] = []

    async def named(first: int, second: str, *, started: porto.Started[str]) -> None:
        names.append(porto.current_task().name)
        started.set(f"{first}{second}")

    named_qualname = named.__qualname__

    async def main() -> tuple[str, str]:
        async with porto.scope() as s:
            plain = await s.start(named, 1, "a")
            custom = await s.start(named, 2, "b", name="the server")
        return plain, custom

    assert porto.run(main) == ("1a", "2b")
    assert names == [named_qualname, "the server"]


def test_an_exception_before_set_is_raised_from_start_as_it_is_not_as_a_group() -> None:
    async def broken(*, started: porto.Started[int]) -> None:
        await porto.checkpoint()
        raise ValueError("could not bind")

    async def main() -> bool:
        async with porto.scope() as s:
            with pytest.raises(ValueError, match="could not bind"):
                await s.start(broken)
            # The scope start was called on is untouched: it was not cancelled, nothing failed.
            return s.cancel_called

    assert porto.run(main) is False


def test_a_failure_after_set_is_reported_by_the_scope_start_was_called_on() -> None:
    async def fails_later(*, started: porto.Started[str]) -> None:
        started.set("ready")
        await porto.checkpoint()
        raise ValueError("late failure")

    async def scoped(values: list[str]) -> None:
        async with porto.scope() as s:
            values.append(await s.start(fails_later))
            await porto.sleep_forever()

    async def main() -> str:
        values: list[str] = []
        with pytest.raises(ExceptionGroup) as raised:
            await scoped(values)
        assert values == ["ready"]
        (error,) = raised.value.exceptions
        assert isinstance(error, ValueError)
        return str(error)

    assert porto.run(main) == "late failure"


def test_cancelling_the_caller_before_set_cancels_the_task_and_start_raises_cancelled() -> None:
    log: list[str] = []

    async def slow(*, started: porto.Started[None]) -> None:
        try:
            await porto.sleep(60)
        except porto.Cancelled:
            log.append("task cancelled")
            raise
        started.set()

    async def caller(s: porto.Scope) -> None:
        await s.start(slow)

    async def main() -> bool:
        async with porto.scope() as s:
            async with porto.scope() as outer:
                task = outer.spawn(caller, s)
                await settle()
                outer.cancel()
            assert task.cancelled()
            return s.cancel_called

    assert porto.run(main) is False
    assert log == ["task cancelled"]


def test_a_timeout_around_start_cancels_a_task_that_never_reports() -> None:
    async def never(*, started: porto.Started[None]) -> None:
        await porto.sleep_forever()

    async def main() -> None:
        async with porto.scope() as s:
            with pytest.raises(TimeoutError):
                async with porto.timeout(0.02):
                    await s.start(never)

    porto.run(main)


def test_start_raises_when_the_task_finishes_without_calling_set() -> None:
    async def forgetful(*, started: porto.Started[int]) -> None:
        return None

    async def main() -> None:
        async with porto.scope() as s:
            with pytest.raises(RuntimeError, match=r"without calling\s+started\.set"):
                await s.start(forgetful)

    porto.run(main)


def test_calling_set_twice_raises_runtime_error() -> None:
    errors: list[RuntimeError] = []

    async def twice(*, started: porto.Started[int]) -> None:
        started.set(1)
        try:
            started.set(2)
        except RuntimeError as error:
            errors.append(error)

    async def main() -> int:
        async with porto.scope() as s:
            return await s.start(twice)

    assert porto.run(main) == 1
    (error,) = errors
    assert "called twice" in str(error)
    assert "a single time" in str(error)


def test_set_twice_raises_in_the_task_and_a_task_that_lets_it_out_fails_its_scope() -> None:
    async def careless(*, started: porto.Started[int]) -> None:
        started.set(1)
        started.set(2)

    async def main() -> None:
        async with porto.scope() as s:
            assert await s.start(careless) == 1
            await porto.sleep_forever()

    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert any(isinstance(error, RuntimeError) for error in raised.value.exceptions)


def test_after_set_the_task_is_cancelled_by_its_new_scope_and_not_by_the_callers() -> None:
    cancelled: list[str] = []

    async def parked(*, started: porto.Started[None]) -> None:
        started.set()
        try:
            await porto.sleep_forever()
        except porto.Cancelled:
            cancelled.append("by its scope")
            raise

    async def main() -> bool:
        async with porto.scope() as s:
            async with porto.scope() as callers:
                await s.start(parked)
                callers.cancel()
            await settle()
            still_running = not cancelled
            s.cancel()
        return still_running

    assert porto.run(main) is True
    assert cancelled == ["by its scope"]


def test_scopes_the_task_entered_before_set_follow_it_into_its_new_scope() -> None:
    cancelled: list[str] = []

    async def worker(*, started: porto.Started[None]) -> None:
        async with porto.scope() as inner:
            inner.spawn(park, "grandchild")
            started.set()
            await porto.sleep_forever()

    async def park(name: str) -> None:
        try:
            await porto.sleep_forever()
        except porto.Cancelled:
            cancelled.append(name)
            raise

    async def main() -> None:
        async with porto.scope() as s:
            await s.start(worker)
            await settle()
            s.cancel()

    porto.run(main)
    assert cancelled == ["grandchild"]


def test_the_scope_waits_for_the_started_task_at_its_exit() -> None:
    log: list[str] = []

    async def finishes_soon(*, started: porto.Started[None]) -> None:
        started.set()
        await porto.sleep(0.01)
        log.append("task done")

    async def main() -> None:
        async with porto.scope() as s:
            await s.start(finishes_soon)
            log.append("block done")
        log.append("scope exited")

    porto.run(main)
    assert log == ["block done", "task done", "scope exited"]


def test_start_in_the_root_scope() -> None:
    async def main() -> int:
        import porto._core as core

        root = core._root_scope()  # pyright: ignore[reportPrivateUsage]
        return await root.start(server, 1)

    assert porto.run(main) == 2


def test_the_started_task_runs_in_a_copy_of_the_callers_context() -> None:
    var: contextvars.ContextVar[str] = contextvars.ContextVar("var")

    async def reads(*, started: porto.Started[str]) -> None:
        started.set(var.get())

    async def main() -> str:
        var.set("callers")
        async with porto.scope() as s:
            return await s.start(reads)

    assert porto.run(main) == "callers"


def test_a_task_in_no_scope_can_start_into_a_scope_and_that_scope_then_reaches_it() -> None:
    # The caller is an internal task that entered no scope: the hidden scope has no parent
    # until `set` gives it the scope `start` was called on.
    cancelled: list[str] = []
    started_value: list[int] = []

    async def parked(*, started: porto.Started[int]) -> None:
        started.set(5)
        try:
            await porto.sleep_forever()
        except porto.Cancelled:
            cancelled.append("by its scope")
            raise

    async def internal(s: porto.Scope) -> None:
        started_value.append(await s.start(parked))

    async def main() -> None:
        async with porto.scope() as s:
            _core_spawn(internal(s))
            for _ in range(5):
                await porto.checkpoint()
            assert started_value == [5]
            s.cancel()

    porto.run(main)
    assert cancelled == ["by its scope"]


# What a user can get wrong ---------------------------------------------------------------------


def test_a_function_that_does_not_take_started_raises_the_type_error_from_start() -> None:
    async def plain() -> None:
        return None

    async def main() -> None:
        async with porto.scope() as s:
            with pytest.raises(TypeError, match="started"):
                await s.start(plain)
            assert not s.cancel_called

    porto.run(main)


def test_a_function_that_is_not_async_raises_the_type_error_from_start() -> None:
    def sync_function(*, started: porto.Started[None]) -> int:
        return 1

    async def main() -> None:
        async with porto.scope() as s:
            with pytest.raises(TypeError, match="async"):
                await s.start(sync_function)  # pyright: ignore[reportArgumentType]

    porto.run(main)


def test_start_takes_only_name_as_a_keyword_and_says_what_to_do() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            with pytest.raises(TypeError, match=r"functools\.partial"):
                await s.start(server, 1, port=2)  # pyright: ignore[reportCallIssue]
            with pytest.raises(TypeError, match="name= must be a str"):
                await s.start(server, 1, name=3)  # pyright: ignore[reportArgumentType]

    porto.run(main)


def test_start_on_a_closed_scope_or_one_not_entered_raises_runtime_error() -> None:
    async def main() -> None:
        scope = porto.scope()
        with pytest.raises(RuntimeError, match="not entered"):
            await scope.start(server, 1)
        async with scope:
            pass
        with pytest.raises(RuntimeError, match="closed"):
            await scope.start(server, 1)
        with porto.shield() as shield, pytest.raises(RuntimeError, match="sync `with`"):
            await shield.start(server, 1)

    porto.run(main)


def test_start_outside_a_runtime_raises_no_runtime_error() -> None:
    scope = porto.scope()
    coroutine = scope.start(server, 1)
    with pytest.raises(porto.NoRuntimeError, match=r"Scope\.start"):
        coroutine.send(None)
    coroutine.close()


def test_started_cannot_be_made_directly_and_the_error_says_what_to_do() -> None:
    with pytest.raises(TypeError, match=r"made by Scope\.start\(\).*scope\.start\(work"):
        porto.Started()


def test_set_by_another_task_raises_runtime_error_and_leaves_the_start_waiting() -> None:
    stolen: list[porto.Started[None]] = []
    errors: list[RuntimeError] = []

    async def worker(*, started: porto.Started[None]) -> None:
        stolen.append(started)
        await porto.sleep(0.02)
        started.set()

    async def thief() -> None:
        try:
            stolen[0].set()
        except RuntimeError as error:
            errors.append(error)

    async def main() -> None:
        async with porto.scope() as s:

            async def start_it() -> None:
                await s.start(worker)

            s.spawn(start_it)
            await settle()
            s.spawn(thief)

    porto.run(main)
    (error,) = errors
    assert "the task Scope.start() started" in str(error)


def test_set_after_the_scope_closed_raises_runtime_error_from_the_task_and_from_start() -> None:
    gate = porto.Event()
    shared: list[porto.Scope] = []
    closed = porto.Event()

    async def owner() -> None:
        async with porto.scope() as s:
            shared.append(s)
            await gate.wait()
        closed.set()

    async def late(*, started: porto.Started[None]) -> None:
        gate.set()
        await closed.wait()
        started.set()

    async def main() -> None:
        async with porto.scope() as outer:
            outer.spawn(owner)
            await settle()
            with pytest.raises(RuntimeError, match="closed"):
                await shared[0].start(late)

    porto.run(main)


def test_repr_shows_whether_it_was_set() -> None:
    reprs: list[str] = []

    async def reporting(*, started: porto.Started[None]) -> None:
        reprs.append(repr(started))
        started.set()
        reprs.append(repr(started))

    async def main() -> None:
        async with porto.scope() as s:
            await s.start(reporting)

    porto.run(main)
    assert reprs == ["<porto.Started waiting>", "<porto.Started set>"]
