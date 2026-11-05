"""Scopes on porto-core's scope tree (15-structured-concurrency-and-cancellation.md §1.1, C4,
C5): children are counted and their failures recorded by the scope's node, and the exit waits
on a ``ScopeExit`` trap.

A failing child cancels its scope (``cancelled: a child failed`` in its repr); the siblings
and the body only stop once checkpoints deliver the cancellation (porto-szb.36). Until then,
``_interrupt(token, Cancelled())`` stands in for that delivery.
"""

import gc
import weakref
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

import porto
from porto import _core


async def checkpoint() -> None:
    await _core._yield_trap()


async def wait_until(condition: Callable[[], bool]) -> None:
    while not condition():
        await checkpoint()


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return _core._run(coro)


async def fail_with(error: BaseException) -> None:
    raise error


class Blocked:
    """A child that waits on a probe until it is notified or interrupted."""

    def __init__(self) -> None:
        self.probe = _core._WaitProbe()
        self.token: _core._TaskToken | None = None

    async def run(self) -> str:
        self.token = _core._current_task_token()
        await self.probe.wait()
        return "notified"

    def interrupt(self, exception: BaseException) -> None:
        assert self.token is not None
        _core._interrupt(self.token, exception)


def test_C4_a_child_that_raises_is_in_the_group_and_cancels_the_scope() -> None:
    reprs: list[str] = []

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(fail_with, ValueError("boom"))
            await wait_until(lambda: "0 live" in repr(s))
            reprs.append(repr(s))

    with pytest.raises(ExceptionGroup) as raised:
        run(main())
    assert [repr(error) for error in raised.value.exceptions] == ["ValueError('boom')"]
    assert reprs == ["<porto.Scope open, cancelled: a child failed, 0 live tasks>"]


def test_C4_a_child_that_returns_does_not_cancel_the_scope() -> None:
    reprs: list[str] = []

    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(checkpoint)
        reprs.append(repr(s))
        assert task.result() is None

    run(main())
    assert reprs == ["<porto.Scope closed, 0 live tasks>"]


def test_C4_a_cancelled_absorbed_by_its_cancelled_scope_is_not_a_failure() -> None:
    blocked = Blocked()

    async def main() -> None:
        async with porto.scope() as s:
            task = s.spawn(blocked.run)
            await wait_until(lambda: blocked.probe.waiting == 1)
            s.spawn(fail_with, ValueError("first"))
            await wait_until(lambda: "cancelled" in repr(s))
            # What cancel delivery (porto-szb.36) will throw into the sibling.
            blocked.interrupt(porto.Cancelled())
            await wait_until(task.done)
            # The task itself still reports how it ended.
            with pytest.raises(porto.Cancelled):
                task.result()

    with pytest.raises(ExceptionGroup) as raised:
        run(main())
    assert [repr(error) for error in raised.value.exceptions] == ["ValueError('first')"]


def test_C4_a_cancelled_while_the_scope_is_not_cancelled_is_a_failure() -> None:
    blocked = Blocked()

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(blocked.run)
            await wait_until(lambda: blocked.probe.waiting == 1)
            blocked.interrupt(porto.Cancelled("not from this scope"))

    with pytest.raises(BaseExceptionGroup) as raised:
        run(main())
    (error,) = raised.value.exceptions
    assert isinstance(error, porto.Cancelled)
    assert str(error) == "not from this scope"


def test_C5_the_exit_waits_in_a_scope_exit_trap_until_the_last_child_ends() -> None:
    blocked = Blocked()
    seen: list[str] = []

    async def watcher(scope: porto.Scope) -> None:
        await wait_until(lambda: "exit waiting" in repr(scope))
        seen.append(repr(scope))
        blocked.probe.notify_all()

    async def main() -> None:
        async with porto.scope() as outer:
            async with porto.scope() as s:
                s.spawn(blocked.run)
                outer.spawn(watcher, s)
                await wait_until(lambda: blocked.probe.waiting == 1)
            seen.append(repr(s))

    run(main())
    assert seen == [
        "<porto.Scope open, 1 live tasks, exit waiting>",
        "<porto.Scope closed, 0 live tasks>",
    ]


def test_T3_interrupting_the_exit_wait_leaves_no_exit_waiter() -> None:
    blocked = Blocked()
    owner: list[_core._TaskToken] = []
    seen: list[str] = []

    async def owner_task(scope: porto.Scope) -> str:
        owner.append(_core._current_task_token())
        try:
            async with scope:
                scope.spawn(blocked.run)
        except KeyError:
            return "interrupted"
        return "exited"

    async def main() -> None:
        scope = porto.scope()
        task = _core._spawn_joinable(owner_task(scope))
        await wait_until(lambda: "exit waiting" in repr(scope))
        _core._interrupt(owner[0], KeyError("stop waiting"))
        assert await task.join() == "interrupted"
        seen.append(repr(scope))
        # The child ending later finds no waiter to wake.
        blocked.probe.notify_all()
        await wait_until(lambda: "0 live" in repr(scope))
        seen.append(repr(scope))

    run(main())
    assert seen == [
        "<porto.Scope open, 1 live tasks>",
        "<porto.Scope open, 0 live tasks>",
    ]


def test_a_child_freed_while_idle_ends_and_wakes_the_exit() -> None:
    # The detached owner waits on its scope while its child sleeps; when main returns, the
    # runtime drops the child's timer, which frees the child (it was idle): the scope counts it
    # out and wakes the owner, which is then cancelled (the runtime is gone).
    closed: list[str] = []

    async def child() -> None:
        try:
            await porto.sleep(3600)
        finally:
            closed.append("child")

    async def owner() -> None:
        try:
            async with porto.scope() as s:
                s.spawn(child)
        finally:
            closed.append("owner")

    async def main() -> None:
        _core._spawn(owner())
        for _ in range(4):
            await checkpoint()

    run(main())
    assert sorted(closed) == ["child", "owner"]


def test_a_child_cancelled_in_the_run_queue_ends_and_wakes_the_exit() -> None:
    # A child still queued when main returns is cancelled by the runtime's drop.
    closed: list[str] = []

    async def child() -> None:
        try:
            while True:
                await checkpoint()
        finally:
            closed.append("child")

    async def owner() -> None:
        try:
            async with porto.scope() as s:
                s.spawn(child)
        finally:
            closed.append("owner")

    async def main() -> None:
        _core._spawn(owner())
        for _ in range(4):
            await checkpoint()

    run(main())
    assert sorted(closed) == ["child", "owner"]


def test_a_failed_child_is_freed_once_its_scope_exits() -> None:
    refs: list[weakref.ref[Any]] = []

    async def main() -> None:
        coro = fail_with(ValueError("boom"))
        refs.append(weakref.ref(coro))
        async with porto.scope() as s:
            s.spawn(coro)
        del coro

    with pytest.raises(ExceptionGroup):
        run(main())
    gc.collect()
    assert refs[0]() is None
