"""Cancellation delivery (15-structured-concurrency-and-cancellation.md C1-C4, C10; 11 §5.2-§5.3).

``Scope.cancel()`` cancels a scope; every task in it (and in the scopes nested in it) gets
``porto.Cancelled`` at each checkpoint until it leaves the scope, and a waiting task is woken,
also when the cancel comes from another thread. ``_WaitProbe().wait()`` is a ``Notified`` trap
(it stands in for a lock or a channel receive until those land) and
``_spawn_joinable(coro).join()`` a ``Join`` trap.
"""

import contextlib
import threading
import time
from collections.abc import Callable
from typing import Any

import pytest

import porto
from porto import _core


async def checkpoint() -> None:
    await _core._yield_trap()


async def wait_until(condition: Callable[[], bool]) -> None:
    while not condition():
        await checkpoint()


async def boom() -> None:
    raise ValueError("boom")


def test_C1_level_triggered() -> None:
    probe = _core._WaitProbe()
    caught: list[str] = []

    async def main() -> str:
        async with porto.scope() as s:
            s.cancel()
            # Every checkpoint raises while the scope is cancelled, however often the task
            # swallows it (which it must not do: this only shows the level).
            awaits: list[tuple[str, Callable[[], Any]]] = [
                ("checkpoint", porto.checkpoint),
                ("sleep", lambda: porto.sleep(30)),
                ("sleep(0)", lambda: porto.sleep(0)),
                ("notified", probe.wait),
                ("sync completion", lambda: _core._checkpoint_if_needed(1)),
                ("checkpoint again", porto.checkpoint),
            ]
            for name, make in awaits:
                try:
                    await make()
                except porto.Cancelled:
                    caught.append(name)
            await porto.sleep(30)
            pytest.fail("not reached")
        assert s.cancelled_caught
        # Outside the scope the task is not cancelled.
        await porto.sleep(0)
        return "absorbed"

    assert porto.run(main) == "absorbed"
    assert caught == [
        "checkpoint",
        "sleep",
        "sleep(0)",
        "notified",
        "sync completion",
        "checkpoint again",
    ]
    assert probe.waiting == 0, "a cancelled wait never queues"


def test_C1_a_task_that_keeps_swallowing_it_still_yields_to_the_others() -> None:
    # A coroutine that keeps catching `Cancelled` spends its budget on the throws, so the
    # other tasks keep running.
    seen: list[int] = []

    async def swallower(scope: porto.Scope) -> None:
        async with scope:
            scope.cancel()
            for n in range(500):
                try:
                    await porto.checkpoint()
                except porto.Cancelled:
                    seen.append(n)

    async def main() -> int:
        rounds = 0
        async with porto.scope() as s:
            s.spawn(swallower, porto.scope())
            while len(seen) < 500:
                rounds += 1
                await porto.checkpoint()
        return rounds

    assert porto.run(main) > 1, "the swallower yielded to main"
    assert seen == list(range(500))


def test_C2_only_at_checkpoints() -> None:
    events: list[str] = []
    spinning = threading.Event()
    cancelled = threading.Event()

    async def child() -> None:
        events.append("started")
        spinning.set()
        # Pure Python: the cancel from the other thread lands meanwhile and does not interrupt
        # it.
        if not cancelled.wait(timeout=10):
            pytest.fail("the canceller thread did not run")
        events.append("pure Python ran to the end")
        try:
            await porto.checkpoint()
        except porto.Cancelled:
            events.append("cancelled at the checkpoint")
            raise

    def canceller(scope: porto.Scope) -> None:
        spinning.wait(timeout=10)
        scope.cancel()
        cancelled.set()

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(child)
            thread = threading.Thread(target=canceller, args=(s,))
            thread.start()
        thread.join()

    porto.run(main)
    assert events == ["started", "pure Python ran to the end", "cancelled at the checkpoint"]


def test_C2_the_cancelling_task_runs_on_until_its_next_await() -> None:
    events: list[str] = []

    async def main() -> None:
        async with porto.scope() as s:
            s.cancel()
            events.append("after cancel()")
            await porto.checkpoint()
            events.append("not reached")
        events.append("after the block")

    porto.run(main)
    assert events == ["after cancel()", "after the block"]


@pytest.mark.parametrize("wait", ["sleep", "join", "notified", "lock", "recv", "event"])
def test_C10_cancel_from_foreign_thread(wait: str) -> None:
    # A task blocked in a wait is woken by a cancel from a plain thread.
    probe = _core._WaitProbe()
    lock = porto.Lock()
    event = porto.Event()
    _sender, receiver = porto.channel[int](1)
    waiting = threading.Event()
    outcome: list[str] = []

    async def forever() -> None:
        await probe.wait()

    async def blocked() -> None:
        waiting.set()
        try:
            if wait == "sleep":
                await porto.sleep(3600)
            elif wait == "join":
                await _core._spawn_joinable(forever()).join()
            elif wait == "notified":
                await probe.wait()
            elif wait == "lock":
                await lock.acquire()
            elif wait == "recv":
                await receiver.recv()
            else:
                await event.wait()
        except porto.Cancelled:
            outcome.append("cancelled")
            raise

    def canceller(scope: porto.Scope) -> None:
        waiting.wait(timeout=10)
        # Let the lane park in the wait first.
        time.sleep(0.05)
        scope.cancel("from a thread")

    async def main() -> None:
        await lock.acquire()
        async with porto.scope() as s:
            s.spawn(blocked)
            thread = threading.Thread(target=canceller, args=(s,))
            thread.start()
            await porto.sleep(3600)
        thread.join()
        assert s.cancelled_caught
        assert repr(s) == '<porto.Scope closed, cancelled: "from a thread", 0 live tasks>'

    started = time.monotonic()
    porto.run(main)
    assert time.monotonic() - started < 5
    assert outcome == ["cancelled"]
    # The cancelled waiter left nothing behind: the lock is still main's alone.
    assert lock.locked()
    lock.release()
    assert not lock.locked()


def test_C4_a_failing_child_stops_a_sleeping_sibling_promptly() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(porto.sleep, 10)
            s.spawn(boom)

    started = time.monotonic()
    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert time.monotonic() - started < 2
    assert [repr(error) for error in raised.value.exceptions] == ["ValueError('boom')"]


def test_C4_a_failing_child_cancels_the_body_and_its_cancelled_is_absorbed() -> None:
    events: list[str] = []

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(boom)
            try:
                await porto.sleep(10)
            except porto.Cancelled as cancelled:
                events.append(str(cancelled))
                raise

    with pytest.raises(ExceptionGroup) as raised:
        porto.run(main)
    assert [repr(error) for error in raised.value.exceptions] == ["ValueError('boom')"]
    (message,) = events
    assert "a task of its scope, or of one it is nested in, failed" in message
    assert "let the Cancelled propagate" in message


def test_C3_an_outer_cancel_goes_through_an_inner_scope_and_is_absorbed_by_the_outer() -> None:
    events: list[str] = []

    async def main() -> None:
        async with porto.scope() as outer:
            async with porto.scope() as inner:
                outer.cancel()
                try:
                    await porto.checkpoint()
                finally:
                    events.append("inner body cancelled")
            events.append("not reached: the inner scope does not absorb it")
        events.append(f"outer caught {outer.cancelled_caught}, inner {inner.cancelled_caught}")
        assert outer.cancel_called
        assert not inner.cancel_called

    porto.run(main)
    assert events == ["inner body cancelled", "outer caught True, inner False"]


def test_C3_first_answer_wins_counts_the_children_cancelled_by_the_winner() -> None:
    # Children race and the winner calls s.cancel(). The body finished before the cancel, so
    # only the losers get Cancelled: the scope absorbed them, and cancelled_caught says so.
    async def ask(seconds: float, s: porto.Scope) -> float:
        await porto.sleep(seconds)
        s.cancel("got an answer")
        return seconds

    async def main() -> None:
        async with porto.scope() as s:
            tasks = [s.spawn(ask, seconds, s) for seconds in (0.01, 3600, 3600)]
        assert s.cancel_called
        assert s.cancelled_caught
        assert tasks[0].result() == 0.01
        assert [repr(task) for task in tasks[1:]] == ["<porto.Task cancelled>"] * 2

    porto.run(main)


def test_C3_a_cancel_only_the_body_gets_is_caught() -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.cancel()
            await porto.checkpoint()
            pytest.fail("not reached")
        assert s.cancelled_caught

    porto.run(main)


def test_C3_a_cancel_nobody_lets_out_is_not_caught() -> None:
    # The body and the child both swallow their Cancelled (a bug, shown here only for the
    # flag): nothing reached the exit or ended a task, so nothing was absorbed.
    async def stubborn() -> None:
        with contextlib.suppress(porto.Cancelled):
            await porto.sleep(3600)

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(stubborn)
            await checkpoint()
            s.cancel()
            with contextlib.suppress(porto.Cancelled):
                await porto.checkpoint()
        assert s.cancel_called
        assert not s.cancelled_caught

    porto.run(main)


def test_C3_children_ended_by_an_outer_cancel_count_for_the_outer_scope_only() -> None:
    # The inner body never awaits: only the inner scope's children get the outer's Cancelled.
    # They count for the scope whose cancel caused them (the outer), never for the inner one.
    async def sleeper() -> None:
        await porto.sleep(3600)

    async def canceller(outer: porto.Scope) -> None:
        await checkpoint()
        outer.cancel()

    async def main() -> None:
        async with porto.scope() as outer, porto.scope() as inner:
            inner.spawn(sleeper)
            inner.spawn(canceller, outer)
        assert outer.cancelled_caught
        assert not inner.cancel_called
        assert not inner.cancelled_caught

    porto.run(main)


def test_C3_a_child_ended_by_an_inner_cancel_does_not_count_for_the_outer_scope() -> None:
    async def sleeper() -> None:
        await porto.sleep(3600)

    async def main() -> None:
        async with porto.scope() as outer, porto.scope() as inner:
            inner.spawn(sleeper)
            await checkpoint()
            inner.cancel()
        assert inner.cancelled_caught
        assert not outer.cancel_called
        assert not outer.cancelled_caught

    porto.run(main)


def test_a_cancel_reaches_tasks_spawned_into_nested_scopes() -> None:
    done: list[str] = []

    async def grandchild() -> None:
        try:
            await porto.sleep(3600)
        except porto.Cancelled:
            done.append("grandchild")
            raise

    async def child() -> None:
        async with porto.scope() as inner:
            inner.spawn(grandchild)
            try:
                await porto.sleep(3600)
            except porto.Cancelled:
                done.append("child")
                raise

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(child)
            for _ in range(4):
                await checkpoint()
            s.cancel()

    porto.run(main)
    assert sorted(done) == ["child", "grandchild"]


def test_C6_a_child_spawned_into_a_cancelled_scope_raises_at_its_first_checkpoint() -> None:
    events: list[str] = []

    async def child() -> None:
        events.append("started")
        await porto.checkpoint()
        events.append("not reached")

    async def main() -> None:
        async with porto.scope() as s:
            s.cancel()
            task = s.spawn(child)
        assert repr(task) == "<porto.Task cancelled>"

    porto.run(main)
    assert events == ["started"]


def test_a_scope_cancelled_before_it_is_entered_starts_cancelled() -> None:
    async def main() -> bool:
        s = porto.scope()
        s.cancel()
        assert s.cancel_called
        async with s:
            await porto.checkpoint()
            pytest.fail("not reached")
        return s.cancelled_caught

    assert porto.run(main) is True


def test_cancel_again_or_after_the_exit_does_nothing() -> None:
    async def main() -> str:
        async with porto.scope() as s:
            pass
        assert not s.cancel_called
        s.cancel("late")
        s.cancel("later")
        await porto.checkpoint()
        return repr(s)

    assert porto.run(main) == '<porto.Scope closed, cancelled: "late", 0 live tasks>'


def test_a_cancelled_scope_does_not_reach_a_task_outside_it() -> None:
    async def bystander() -> str:
        for _ in range(10):
            await porto.checkpoint()
        return "untouched"

    async def main() -> str:
        task = _core._spawn_joinable(bystander())
        async with porto.scope() as s:
            s.cancel()
        return await task.join()

    assert porto.run(main) == "untouched"


def test_szb105_a_wait_resolving_when_the_budget_runs_out_keeps_its_grant_through_a_cancel() -> (
    None
):
    # The task reaches its wait with no budget left, while a permit (a lock) is free. Before
    # its coroutine can take the permit the scope is cancelled. The permit must not be lost:
    # either the task resumes holding it and gets `Cancelled` at its next await (and gives it
    # back), or it never takes it.
    probe = _core._WaitProbe()
    probe.release()
    events: list[str] = []

    async def holder() -> None:
        # Spend the budget without awaiting: the call itself spends a unit.
        while isinstance(_core._checkpoint_if_needed(None), _core._Ready):
            pass
        events.append("budget spent")
        await probe.wait()
        events.append("holding the permit")
        try:
            await porto.checkpoint()
        finally:
            events.append("releasing it")
            probe.release()

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(holder)
            await wait_until(lambda: bool(events))
            # The holder yielded without polling its wait: the permit is still free.
            events.append(f"permits {probe.permits}")
            s.cancel()

    porto.run(main)
    assert events == ["budget spent", "permits 1", "holding the permit", "releasing it"]
    assert probe.permits == 1


def test_a_task_waiting_in_a_scope_is_freed_when_the_run_ends() -> None:
    # The scope keeps the waker of a task in a cancellable wait (for a cancel to wake it), a
    # cycle the lane breaks at the end of the run like the other linked waits.
    closed: list[str] = []

    async def sleeper() -> None:
        try:
            await porto.sleep(3600)
        finally:
            closed.append("sleeper")

    async def owner() -> None:
        async with porto.scope() as s:
            s.spawn(sleeper)

    async def main() -> None:
        _core._spawn(owner())
        for _ in range(4):
            await checkpoint()

    _core._run(main())
    assert closed == ["sleeper"]


def test_szb105_a_lock_handed_over_just_before_a_cancel_is_never_leaked() -> None:
    # main releases the lock to the waiting task (a grant in its wait node), then cancels the
    # scope before that task runs. The wait is over, so the task resumes holding the lock and
    # gets `Cancelled` at its next await, which its `async with` turns into a release.
    lock = porto.Lock()
    events: list[str] = []

    async def holder() -> None:
        events.append("waiting")
        try:
            async with lock:
                events.append("holding")
                await porto.checkpoint()
                events.append("not reached")
        except porto.Cancelled:
            events.append("cancelled")
            raise

    async def main() -> None:
        await lock.acquire()
        async with porto.scope() as s:
            s.spawn(holder)
            await wait_until(lambda: bool(events))
            await porto.checkpoint()
            lock.release()
            s.cancel()
        assert not lock.locked()

    porto.run(main)
    assert events == ["waiting", "holding", "cancelled"]


def test_an_exception_leaving_a_lock_block_is_not_replaced_by_the_scope_cancel() -> None:
    # Lock.__aexit__ must not checkpoint while an exception is in flight: its Cancelled would
    # replace the error, and the cancelled scope would then absorb it.
    lock = porto.Lock()

    async def main() -> None:
        async with porto.scope() as s, lock:
            s.cancel()
            raise ValueError("kept")

    with pytest.raises(ValueError, match="kept"):
        porto.run(main)
    assert not lock.locked()


def test_a_keyboard_interrupt_leaving_a_mutex_block_survives_a_cancelled_scope() -> None:
    mutex = porto.Mutex[int](0)

    async def main() -> None:
        async with porto.scope() as s, mutex as guard:
            guard.set(1)
            s.cancel()
            raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        porto.run(main)
    assert not mutex.locked()


def test_reading_a_cancelled_child_raises_a_fresh_cancelled_each_time() -> None:
    async def main() -> tuple[BaseException, BaseException]:
        async with porto.scope() as inner:
            task = inner.spawn(porto.sleep, 30)
            await porto.sleep(0)
            inner.cancel()
        raised: list[BaseException] = []
        for _ in range(2):
            try:
                task.result()
            except porto.Cancelled as error:
                raised.append(error)
        return raised[0], raised[1]

    first, second = porto.run(main)
    assert first is not second
    assert isinstance(first.__cause__, porto.Cancelled)
    assert first.__cause__ is second.__cause__
