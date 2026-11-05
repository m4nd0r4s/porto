"""Cancellation delivery (15-structured-concurrency-and-cancellation.md C1-C4, C10; 11 §5.2-§5.3).

``Scope.cancel()`` cancels a scope; every task in it (and in the scopes nested in it) gets
``porto.Cancelled`` at each checkpoint until it leaves the scope, and a waiting task is woken,
also when the cancel comes from another thread. ``_WaitProbe().wait()`` is a ``Notified`` trap
(it stands in for a lock or a channel receive until those land) and
``_spawn_joinable(coro).join()`` a ``Join`` trap.
"""

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


@pytest.mark.parametrize("wait", ["sleep", "join", "notified"])
def test_C10_cancel_from_foreign_thread(wait: str) -> None:
    # A task blocked in a wait is woken by a cancel from a plain thread. Channel receives and
    # locks wait the same way as the `Notified` probe; their own tests cover them once they
    # land (porto-szb.47, porto-szb.103).
    probe = _core._WaitProbe()
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
            else:
                await probe.wait()
        except porto.Cancelled:
            outcome.append("cancelled")
            raise

    def canceller(scope: porto.Scope) -> None:
        waiting.wait(timeout=10)
        # Let the lane park in the wait first.
        time.sleep(0.05)
        scope.cancel("from a thread")

    async def main() -> None:
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
