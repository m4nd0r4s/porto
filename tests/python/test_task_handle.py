"""``porto.Task``, ``porto.current_task()`` and ``porto.lowlevel``
(11-task-model-and-trap-protocol.md §7, 20-python-api-reference.md).

The handle is made lazily: ``Scope.spawn`` makes one, ``current_task()`` makes one for a task that
has none, and an internal spawn (``_spawn``) makes none, which ``_task_handles_created`` counts.
Awaiting a task that finished takes no trap (``Task.__await__`` returns a ``_Ready``), awaiting
one that runs yields a ``join`` trap.
"""

import gc
import sys
import weakref
from typing import Any

import pytest

import porto
from porto import _core


class Marker:
    """Something a coroutine holds, to tell when it is freed."""


async def double(n: int) -> int:
    await porto.checkpoint()
    return n * 2


async def fail(message: str) -> None:
    await porto.checkpoint()
    raise ValueError(message)


async def park_forever() -> None:
    await porto.sleep_forever()


def handles() -> int:
    return _core._task_handles_created()


# The methods ------------------------------------------------------------------------------


def test_id_is_unique_and_increases_in_spawn_order() -> None:
    async def main() -> list[int]:
        async with porto.scope() as s:
            tasks = [s.spawn(double, n) for n in range(5)]
        return [task.id for task in tasks]

    ids = porto.run(main)
    assert ids == sorted(set(ids))
    assert all(isinstance(task_id, int) and task_id > 0 for task_id in ids)


def test_done_is_false_while_the_task_runs_and_true_once_it_finished() -> None:
    async def main() -> tuple[bool, bool]:
        async with porto.scope() as s:
            task = s.spawn(double, 4)
            before = task.done()
        return before, task.done()

    assert porto.run(main) == (False, True)


def test_result_and_exception_before_the_task_finished_say_what_to_do() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(double, 1)
            with pytest.raises(porto.InvalidStateError, match=r"Task\.result\(\).*async with"):
                task.result()
            with pytest.raises(porto.InvalidStateError, match=r"Task\.exception\(\).*await"):
                task.exception()

    porto.run(main)


def test_result_returns_the_value_and_exception_is_none_for_a_task_that_returned() -> None:
    async def main() -> tuple[int, BaseException | None, bool]:
        async with porto.scope() as s:
            task = s.spawn(double, 21)
        return task.result(), task.exception(), task.cancelled()

    assert porto.run(main) == (42, None, False)


def test_a_failed_task_reports_its_exception_and_re_raises_it_from_result() -> None:
    async def main() -> None:
        tasks: list[porto.Task[None]] = []
        with pytest.raises(ExceptionGroup):
            async with porto.scope() as s:
                tasks.append(s.spawn(fail, "boom"))
        (task,) = tasks
        assert task.done()
        assert not task.cancelled()
        error = task.exception()
        assert isinstance(error, ValueError)
        assert str(error) == "boom"
        with pytest.raises(ValueError, match="boom") as raised:
            task.result()
        assert raised.value is error

    porto.run(main)


def test_cancelled_is_true_for_a_task_its_scope_cancelled_and_its_result_raises_cancelled() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(park_forever)
            await porto.checkpoint()
            s.cancel()
        assert task.done()
        assert task.cancelled()
        with pytest.raises(porto.Cancelled):
            task.result()
        with pytest.raises(porto.Cancelled):
            task.exception()

    porto.run(main)


def test_pinned_name_and_repr() -> None:
    async def main() -> list[str]:
        async with porto.scope() as s:
            plain = s.spawn(double, 1)
            named = s.spawn(double, 1, name="doubler", pin=True)
            before = [repr(plain), repr(named)]
        return [
            *before,
            repr(plain),
            plain.name,
            named.name,
            str(plain.pinned),
            str(named.pinned),
        ]

    before_plain, before_named, after, plain_name, named_name, plain_pinned, named_pinned = (
        porto.run(main)
    )
    assert before_plain == "<porto.Task 'double' running>"
    assert before_named == "<porto.Task 'doubler' running, pinned>"
    assert after == "<porto.Task 'double' returned>"
    assert (plain_name, named_name) == ("double", "doubler")
    assert (plain_pinned, named_pinned) == ("False", "True")


def test_lane_is_the_lane_the_task_runs_on_and_none_once_it_finished() -> None:
    async def main() -> tuple[int | None, int | None]:
        async with porto.scope() as s:
            task = s.spawn(double, 1)
            running = task.lane
        return running, task.lane

    assert porto.run(main) == (0, None)


# Awaiting a task ---------------------------------------------------------------------------


def test_awaiting_a_task_that_runs_yields_a_join_trap_and_returns_its_result() -> None:
    async def main() -> tuple[str, int]:
        async with porto.scope() as s:
            task = s.spawn(double, 5)
            awaitable = task.__await__()
            assert isinstance(awaitable, _core._Trap)
            kind = awaitable.kind
            awaitable.close()
            value = await task
        return kind, value

    assert porto.run(main) == ("join", 10)


def test_awaiting_a_task_that_finished_takes_no_trap() -> None:
    async def main() -> tuple[str, int]:
        async with porto.scope() as s:
            task = s.spawn(double, 6)
        awaitable = task.__await__()
        assert not isinstance(awaitable, _core._Trap), "a finished task is a synchronous await"
        kind = type(awaitable).__name__
        return kind, await task

    assert porto.run(main) == ("_Ready", 12)


def test_awaiting_a_task_that_failed_re_raises_its_exception() -> None:
    async def main() -> None:
        tasks: list[porto.Task[None]] = []
        with pytest.raises(ExceptionGroup):
            async with porto.scope() as s:
                tasks.append(s.spawn(fail, "boom"))
        (task,) = tasks
        with pytest.raises(ValueError, match="boom") as raised:
            await task
        assert raised.value is task.exception()

    porto.run(main)


def test_a_task_waiting_for_a_failing_task_gets_its_exception() -> None:
    caught: list[str] = []

    async def waiter(target: porto.Task[None]) -> None:
        try:
            await target
        except ValueError as error:
            caught.append(str(error))

    async def spawn_both() -> None:
        async with porto.scope() as s:
            failing = s.spawn(fail, "late")
            s.spawn(waiter, failing)

    async def main() -> None:
        with pytest.raises(ExceptionGroup):
            await spawn_both()

    porto.run(main)
    assert caught == ["late"]


def test_awaiting_a_cancelled_task_raises_a_fresh_cancelled_caused_by_its_own() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(park_forever)
            await porto.checkpoint()
            s.cancel()
        with pytest.raises(porto.Cancelled) as first:
            await task
        with pytest.raises(porto.Cancelled) as second:
            await task
        assert first.value is not second.value
        assert isinstance(first.value.__cause__, porto.Cancelled)

    porto.run(main)


def test_a_task_that_awaits_itself_gets_a_runtime_error_that_says_what_to_do() -> None:
    async def selfish() -> None:
        await porto.current_task()

    async def main() -> None:
        tasks: list[porto.Task[None]] = []
        with pytest.raises(ExceptionGroup):
            async with porto.scope() as s:
                tasks.append(s.spawn(selfish))
        error = tasks[0].exception()
        assert isinstance(error, RuntimeError)
        assert "awaited itself" in str(error)
        assert "from another task" in str(error)

    porto.run(main)


def test_a_cancelled_waiter_gives_its_wait_up_and_the_target_keeps_running() -> None:
    async def main() -> bool:
        async with porto.scope() as outer:
            target = outer.spawn(double, 3)
            async with porto.scope() as inner:

                async def waiter() -> None:
                    await target

                inner.spawn(waiter)
                await porto.checkpoint()
                inner.cancel()
        return target.result() == 6

    assert porto.run(main) is True


def test_awaiting_a_cancelled_task_after_its_runtime_stopped_raises_cancelled() -> None:
    saved: list[porto.Task[None]] = []

    async def main() -> None:
        async with porto.scope() as s:
            saved.append(s.spawn(park_forever))
            await porto.checkpoint()
            s.cancel()

    porto.run(main)
    (task,) = saved
    assert task.done()
    with pytest.raises(porto.Cancelled):
        task.__await__()


# current_task ------------------------------------------------------------------------------


def test_current_task_outside_a_runtime_raises_no_runtime_error() -> None:
    with pytest.raises(porto.NoRuntimeError, match="current_task"):
        porto.current_task()


def test_current_task_is_the_handle_spawn_returned() -> None:
    seen: list[porto.Task[Any]] = []

    async def child() -> None:
        seen.append(porto.current_task())

    async def main() -> porto.Task[None]:
        async with porto.scope() as s:
            task = s.spawn(child, name="the child")
        return task

    task = porto.run(main)
    assert len(seen) == 1
    assert seen[0] is task
    assert task.name == "the child"


def test_current_task_in_main_is_named_after_main() -> None:
    async def main() -> tuple[str, bool]:
        task = porto.current_task()
        return task.name, task.done()

    assert porto.run(main) == (main.__qualname__, False)


def test_a_task_with_no_handle_gets_one_from_current_task_and_keeps_it() -> None:
    names: list[str] = []
    same: list[bool] = []

    async def internal() -> None:
        first = porto.current_task()
        names.append(first.name)
        same.append(porto.current_task() is first)
        await porto.checkpoint()
        same.append(porto.current_task() is first)

    async def main() -> None:
        _core._spawn(internal())
        for _ in range(5):
            await porto.checkpoint()

    before = handles()
    porto.run(main)
    # main's own handle, then the internal task's, made by current_task().
    assert handles() - before == 2
    assert names[0].startswith("Task-")
    assert same == [True, True]


def test_the_handle_current_task_makes_is_finished_with_its_task() -> None:
    kept: list[porto.Task[Any]] = []

    async def internal() -> int:
        kept.append(porto.current_task())
        await porto.checkpoint()
        return 7

    async def main() -> None:
        _core._spawn(internal())
        for _ in range(5):
            await porto.checkpoint()

    porto.run(main)
    (handle,) = kept
    assert handle.done()
    assert handle.result() == 7
    assert handle.lane is None


# The handle is lazy -------------------------------------------------------------------------


def test_an_internal_spawn_creates_no_python_handle() -> None:
    async def internal() -> None:
        await porto.checkpoint()

    async def main() -> int:
        before = handles()
        for _ in range(50):
            _core._spawn(internal())
        for _ in range(5):
            await porto.checkpoint()
        return handles() - before

    assert porto.run(main) == 0


def test_a_joinable_internal_spawn_creates_no_python_handle() -> None:
    async def internal() -> int:
        return 3

    async def main() -> int:
        before = handles()
        target = _core._spawn_joinable(internal())
        assert await target.join() == 3
        return handles() - before

    assert porto.run(main) == 0


def test_scope_spawn_creates_exactly_one_handle_per_task() -> None:
    async def main() -> int:
        before = handles()
        async with porto.scope() as s:
            for n in range(10):
                s.spawn(double, n)
        return handles() - before

    assert porto.run(main) == 10


def test_a_detached_task_that_took_a_handle_still_reports_its_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reported: list[Any] = []
    monkeypatch.setattr(sys, "unraisablehook", reported.append)
    kept: list[porto.Task[None]] = []

    async def internal() -> None:
        kept.append(porto.current_task())
        await porto.checkpoint()
        raise ValueError("nobody reads the handle")

    async def main() -> None:
        _core._spawn(internal())
        for _ in range(5):
            await porto.checkpoint()

    porto.run(main)
    assert len(reported) == 1
    assert isinstance(reported[0].exc_value, ValueError)
    assert isinstance(kept[0].exception(), ValueError), "the handle has the outcome too"


@pytest.mark.parametrize("stop", [KeyboardInterrupt, SystemExit])
def test_a_detached_task_that_took_a_handle_still_stops_the_run_on_an_interrupt(
    stop: type[BaseException],
) -> None:
    async def internal() -> None:
        porto.current_task()
        raise stop()

    async def main() -> None:
        _core._spawn(internal())
        await porto.sleep_forever()

    with pytest.raises(stop):
        porto.run(main)


# Lifetime -----------------------------------------------------------------------------------


def test_a_result_that_references_its_task_is_collected_with_it() -> None:
    # task -> outcome -> box -> task: the collector must see through the handle.
    async def child(box: list[object]) -> list[object]:
        await porto.checkpoint()
        return box

    async def main() -> weakref.ref[Marker]:
        marker = Marker()
        box: list[object] = [marker]
        async with porto.scope() as s:
            task = s.spawn(child, box)
        box.append(task)
        return weakref.ref(marker)

    alive = porto.run(main)
    gc.collect()
    assert alive() is None


def test_a_traceback_that_references_its_task_is_collected_with_it() -> None:
    # task -> exception -> traceback -> main's frame -> task, once `await task` re-raises it.
    async def main() -> weakref.ref[Marker]:
        marker = Marker()
        tasks: list[porto.Task[None]] = []
        with pytest.raises(ExceptionGroup):
            async with porto.scope() as s:
                tasks.append(s.spawn(fail, "boom"))
        task = tasks[0]
        error = task.exception()
        assert error is not None
        error.marker = marker  # pyright: ignore[reportAttributeAccessIssue]
        with pytest.raises(ValueError, match="boom"):
            await task
        return weakref.ref(marker)

    alive = porto.run(main)
    gc.collect()
    assert alive() is None


def test_a_child_abandoned_at_shutdown_is_freed_with_its_runtime() -> None:
    # A task parked forever that swallows its cancellation is abandoned when the runtime stops.
    # Its handle must not keep it (and so its coroutine and what that holds) alive.
    refs: list[weakref.ref[Marker]] = []

    async def stubborn(held: Marker) -> None:
        while True:
            try:
                await porto.sleep_forever()
            except porto.Cancelled:
                continue

    async def main() -> porto.Task[None]:
        marker = Marker()
        refs.append(weakref.ref(marker))
        return _core._root_scope().spawn(stubborn, marker)

    with pytest.warns(RuntimeWarning, match="abandoned"):
        task = porto.run(main, cancel_timeout=0.05)
    assert task.done()
    assert task.cancelled()
    assert refs[0]() is None, "the abandoned task's coroutine outlived its runtime"


def test_a_task_handle_outlives_its_runtime_with_its_outcome() -> None:
    async def main() -> porto.Task[int]:
        async with porto.scope() as s:
            return s.spawn(double, 4)

    task = porto.run(main)
    assert task.result() == 8
    assert task.lane is None


# porto.lowlevel -----------------------------------------------------------------------------


def test_lowlevel_trap_yield_yields_once_like_checkpoint() -> None:
    async def main() -> str:
        trap = porto.lowlevel.trap_yield()
        kind = trap.kind
        await trap
        return kind

    assert porto.run(main) == "yield"


def test_lowlevel_trap_yield_raises_cancelled_like_a_checkpoint() -> None:
    async def child() -> None:
        await porto.lowlevel.trap_yield()

    async def main() -> bool:
        async with porto.scope() as s:
            s.spawn(child)
            s.cancel()
        return s.cancelled_caught

    assert porto.run(main) is True


def test_lowlevel_trap_yield_never_awaited_warns() -> None:
    async def main() -> None:
        porto.lowlevel.trap_yield()
        gc.collect()

    with pytest.warns(RuntimeWarning, match="never awaited"):
        porto.run(main)


def test_lowlevel_trap_wait_notify_waits_for_a_notify_and_takes_a_stored_permit() -> None:
    order: list[str] = []

    async def waiter(notify: porto.Notify) -> None:
        order.append("waiting")
        await porto.lowlevel.trap_wait_notify(notify)
        order.append("woken")

    async def main() -> None:
        notify = porto.Notify()
        async with porto.scope() as s:
            s.spawn(waiter, notify)
            await porto.checkpoint()
            assert order == ["waiting"]
            notify.notify_one()
        assert order == ["waiting", "woken"]
        # A permit stored by notify_one() is taken at once, without waiting.
        notify.notify_one()
        await porto.lowlevel.trap_wait_notify(notify)

    porto.run(main)


def test_lowlevel_trap_wait_notify_is_cancellable() -> None:
    async def main() -> bool:
        notify = porto.Notify()
        async with porto.scope() as s:

            async def waiter() -> None:
                await porto.lowlevel.trap_wait_notify(notify)

            s.spawn(waiter)
            await porto.checkpoint()
            s.cancel()
        return s.cancelled_caught

    assert porto.run(main) is True


def test_lowlevel_trap_wait_notify_refuses_what_is_not_a_notify() -> None:
    async def main() -> None:
        with pytest.raises(TypeError, match=r"takes a porto\.Notify, got int"):
            porto.lowlevel.trap_wait_notify(3)  # pyright: ignore[reportArgumentType]

    porto.run(main)


def test_lowlevel_current_lane_is_the_lane_of_the_calling_task() -> None:
    async def child() -> int:
        return porto.lowlevel.current_lane()

    async def main() -> list[int]:
        async with porto.scope() as s:
            task = s.spawn(child)
        return [porto.lowlevel.current_lane(), task.result()]

    assert porto.run(main) == [0, 0]


@pytest.mark.parametrize("name", ["trap_yield", "current_lane"])
def test_lowlevel_calls_outside_a_runtime_raise_no_runtime_error(name: str) -> None:
    with pytest.raises(porto.NoRuntimeError, match=name):
        getattr(porto.lowlevel, name)()


def test_lowlevel_trap_wait_notify_outside_a_runtime_raises_no_runtime_error() -> None:
    with pytest.raises(porto.NoRuntimeError, match="trap_wait_notify"):
        porto.lowlevel.trap_wait_notify(porto.Notify())
