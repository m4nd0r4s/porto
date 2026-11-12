"""``Task.cancel()`` cancels an implicit per-task scope (15-structured-concurrency-and-
cancellation.md §4).

It is the same as the task having been spawned inside a scope that ``Scope.cancel()`` was called
on: only that task (and what runs in scopes it entered) gets ``Cancelled``, the siblings and the
scope it was spawned into are not cancelled, and the ``Cancelled`` that ends the task is not a
failure. The implicit scope is made when the task first enters a scope; until then the request is
a flag the task reads at its checkpoints.
"""

import threading
import time

import pytest

import porto


async def park_forever() -> None:
    await porto.sleep_forever()


async def double(n: int) -> int:
    await porto.checkpoint()
    return n * 2


async def settle() -> None:
    for _ in range(3):
        await porto.checkpoint()


def test_cancelling_one_child_leaves_its_siblings_running_and_the_parent_exits_cleanly() -> None:
    async def main() -> tuple[int, bool, bool, bool]:
        async with porto.scope() as s:
            victim = s.spawn(park_forever)
            sibling = s.spawn(double, 4)
            await settle()
            victim.cancel()
            await settle()
        return sibling.result(), s.cancel_called, s.cancelled_caught, victim.cancelled()

    result, cancel_called, cancelled_caught, cancelled = porto.run(main)
    assert result == 8, "the sibling ran to its end"
    assert not cancel_called, "the scope itself was not cancelled"
    assert not cancelled_caught, "the scope absorbed nothing: the cancel was the task's"
    assert cancelled


def test_the_cancelled_task_is_done_cancelled_and_its_result_raises_cancelled() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(park_forever)
            await settle()
            assert not task.done()
            task.cancel()
            await settle()
        assert task.done()
        assert task.cancelled()
        assert repr(task) == "<porto.Task 'park_forever' cancelled>"
        with pytest.raises(porto.Cancelled) as raised:
            task.result()
        assert isinstance(raised.value.__cause__, porto.Cancelled)
        assert "Task.cancel() was called" in str(raised.value.__cause__)
        with pytest.raises(porto.Cancelled):
            await task

    porto.run(main)


def test_cancelling_a_task_that_finished_does_nothing() -> None:
    async def main() -> tuple[bool, int]:
        async with porto.scope() as s:
            task = s.spawn(double, 5)
        task.cancel()
        task.cancel()
        return task.cancelled(), task.result()

    assert porto.run(main) == (False, 10)


def test_cancelling_twice_does_nothing_more() -> None:
    async def main() -> bool:
        async with porto.scope() as s:
            task = s.spawn(park_forever)
            await settle()
            task.cancel()
            task.cancel()
        return task.cancelled()

    assert porto.run(main) is True


def test_cancelling_wakes_a_task_in_each_kind_of_wait() -> None:
    async def wait_notify(notify: porto.Notify) -> None:
        await notify.notified()

    async def wait_event(event: porto.Event) -> None:
        await event.wait()

    async def wait_lock(lock: porto.Lock) -> None:
        async with lock:
            pass

    async def wait_task(target: porto.Task[None]) -> None:
        await target

    async def main() -> list[bool]:
        notify, event, lock = porto.Notify(), porto.Event(), porto.Lock()
        await lock.acquire()
        async with porto.scope() as s:
            blocker = s.spawn(park_forever)
            tasks = [
                s.spawn(porto.sleep, 60),
                s.spawn(wait_notify, notify),
                s.spawn(wait_event, event),
                s.spawn(wait_lock, lock),
                s.spawn(wait_task, blocker),
            ]
            await settle()
            for task in tasks:
                task.cancel()
            await settle()
            assert all(task.done() for task in tasks)
            blocker.cancel()
        lock.release()
        return [task.cancelled() for task in tasks]

    assert porto.run(main) == [True] * 5


def test_a_task_that_swallows_the_cancel_gets_it_again_at_its_next_await() -> None:
    log: list[str] = []

    async def stubborn() -> str:
        for round_ in range(3):
            try:
                await porto.sleep_forever()
            except porto.Cancelled:
                log.append(f"swallowed {round_}")
        return "survived"

    async def main() -> tuple[bool, str]:
        async with porto.scope() as s:
            task = s.spawn(stubborn)
            await settle()
            task.cancel()
        return task.cancelled(), task.result()

    assert porto.run(main) == (False, "survived")
    assert log == ["swallowed 0", "swallowed 1", "swallowed 2"], "level-triggered (C1)"


def test_a_task_cancelled_before_it_ran_gets_cancelled_at_its_first_checkpoint() -> None:
    ran: list[str] = []

    async def child() -> None:
        ran.append("started")
        await porto.checkpoint()
        ran.append("unreachable")

    async def main() -> bool:
        async with porto.scope() as s:
            task = s.spawn(child)
            task.cancel()
        return task.cancelled()

    assert porto.run(main) is True
    assert ran == ["started"], "C6: it runs up to its first checkpoint, as in a cancelled scope"


def test_a_task_that_never_awaits_finishes_normally_even_if_cancelled_before_it_ran() -> None:
    async def quick() -> int:
        return 3

    async def main() -> tuple[bool, int]:
        async with porto.scope() as s:
            task = s.spawn(quick)
            task.cancel()
        return task.cancelled(), task.result()

    assert porto.run(main) == (False, 3)


def test_the_cancel_reaches_the_tasks_in_scopes_the_task_entered_and_is_not_absorbed() -> None:
    inner_scopes: list[porto.Scope] = []

    async def worker() -> None:
        async with porto.scope() as inner:
            inner_scopes.append(inner)
            grandchildren = [inner.spawn(park_forever) for _ in range(3)]
            await porto.sleep_forever()
        del grandchildren  # never reached: the Cancelled leaves the block

    async def main() -> tuple[bool, bool, bool, bool]:
        async with porto.scope() as s:
            task = s.spawn(worker)
            await settle()
            task.cancel()
        (inner,) = inner_scopes
        return (
            task.cancelled(),
            s.cancel_called,
            inner.cancel_called,
            inner.cancelled_caught,
        )

    cancelled, outer_called, inner_called, inner_caught = porto.run(main)
    assert cancelled
    assert not outer_called
    assert not inner_called, "the inner scope was not cancelled itself: its task's scope was"
    assert not inner_caught, "so it absorbed nothing and the Cancelled went on to end the task"


def test_a_task_cancelled_before_it_entered_a_scope_makes_that_scope_cancelled_from_the_start() -> (
    None
):
    seen: list[str] = []

    async def worker() -> None:
        try:
            await porto.sleep_forever()
        except porto.Cancelled:
            seen.append("cancelled once")
        # Cancelled before any scope was entered: what it enters now is inside the cancel.
        async with porto.scope() as inner:
            child = inner.spawn(park_forever)
            await porto.sleep_forever()
        del child

    async def main() -> tuple[bool, list[str]]:
        async with porto.scope() as s:
            task = s.spawn(worker)
            await settle()
            task.cancel()
        return task.cancelled(), seen

    assert porto.run(main) == (True, ["cancelled once"])


def test_a_shield_the_task_entered_holds_the_cancel_off_until_it_is_left() -> None:
    log: list[str] = []

    async def worker() -> None:
        with porto.shield():
            await porto.sleep(0.02)
            log.append("cleanup done")
        log.append("left the shield")
        await porto.checkpoint()
        log.append("unreachable")

    async def main() -> bool:
        async with porto.scope() as s:
            task = s.spawn(worker)
            await settle()
            task.cancel()
        return task.cancelled()

    assert porto.run(main) is True
    assert log == ["cleanup done", "left the shield"]


def test_a_task_can_cancel_itself() -> None:
    async def worker() -> None:
        porto.current_task().cancel()
        await porto.checkpoint()

    async def main() -> bool:
        async with porto.scope() as s:
            task = s.spawn(worker)
        return task.cancelled()

    assert porto.run(main) is True


def test_cancelling_main_ends_porto_run_with_cancelled() -> None:
    async def main() -> None:
        porto.current_task().cancel()
        await porto.checkpoint()

    with pytest.raises(porto.Cancelled):
        porto.run(main)


def test_a_cancelled_waiter_gives_up_its_wait_and_the_awaited_task_keeps_running() -> None:
    async def main() -> tuple[bool, int]:
        async with porto.scope() as s:
            gate = porto.Event()

            async def slow() -> int:
                await gate.wait()
                return 9

            target = s.spawn(slow)

            async def waiter() -> None:
                await target

            waiting = s.spawn(waiter)
            await settle()
            waiting.cancel()
            await settle()
            assert waiting.cancelled()
            assert not target.done()
            gate.set()
        return waiting.cancelled(), target.result()

    assert porto.run(main) == (True, 9)


def test_cancelling_from_another_thread_wakes_a_parked_task() -> None:
    async def main() -> bool:
        async with porto.scope() as s:
            task = s.spawn(park_forever)
            await settle()
            thread = threading.Thread(target=lambda: (time.sleep(0.02), task.cancel()))
            thread.start()
            # The scope's exit parks the lane until the thread cancels the task.
        thread.join()
        return task.cancelled()

    assert porto.run(main) is True


def test_a_child_cancelled_while_its_parent_scope_is_cancelled_too_still_ends_cancelled() -> None:
    async def main() -> bool:
        async with porto.scope() as s:
            task = s.spawn(park_forever)
            await settle()
            task.cancel()
            s.cancel()
        return task.cancelled()

    assert porto.run(main) is True


def test_a_task_cancelled_inside_a_nested_scope_does_not_cancel_the_scope_it_was_spawned_into() -> (
    None
):
    async def main() -> tuple[bool, bool]:
        async with porto.scope() as outer:
            async with porto.scope() as nested:
                victim = nested.spawn(park_forever)
                other = nested.spawn(double, 1)
                await settle()
                victim.cancel()
            return outer.cancel_called, victim.cancelled() and other.result() == 2

    assert porto.run(main) == (False, True)
