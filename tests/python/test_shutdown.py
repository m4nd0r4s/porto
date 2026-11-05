"""What the end of a run does to tasks still waiting (11-task-model-and-trap-protocol.md §3.2,
§5.3 T3).

A task waiting on a join, a sync primitive or a scope exit is held by what it waits on, which
it holds in turn: nothing outside the cycle ever ends the wait. The run's end cancels such
tasks, which gives their waits up (T3) and closes their coroutines, so nothing outlives
``_run``. ``_WaitProbe().wait()`` is a ``Notified`` trap, ``_spawn_joinable(coro).join()`` a
``Join`` trap.
"""

import weakref
from collections.abc import Callable, Coroutine
from typing import Any

import porto
from porto import _core


async def checkpoint() -> None:
    await _core._yield_trap()


async def wait_until(condition: Callable[[], bool]) -> None:
    while not condition():
        await checkpoint()


class Closed:
    """Records which coroutines were closed, and keeps weak references to them."""

    def __init__(self) -> None:
        self.names: list[str] = []
        self.refs: list[weakref.ref[Any]] = []

    def track(self, coro: Coroutine[Any, Any, Any]) -> Coroutine[Any, Any, Any]:
        self.refs.append(weakref.ref(coro))
        return coro

    def all_freed(self) -> bool:
        # No `gc.collect()`: a task that is freed releases its coroutine at once.
        return all(ref() is None for ref in self.refs)


def test_a_task_waiting_on_a_primitive_forever_is_closed_by_the_end_of_run() -> None:
    closed = Closed()
    probe = _core._WaitProbe()

    async def waiter() -> None:
        try:
            await probe.wait()
        finally:
            closed.names.append("waiter")

    async def main() -> None:
        _core._spawn(closed.track(waiter()))
        await wait_until(lambda: probe.waiting == 1)

    _core._run(main())
    assert closed.names == ["waiter"]
    assert probe.waiting == 0
    assert closed.all_freed()
    # A later notify finds nobody, and runs nothing.
    assert probe.notify_all() == 0
    assert closed.names == ["waiter"]


def test_a_task_joined_forever_and_its_target_are_closed_by_the_end_of_run() -> None:
    closed = Closed()
    probe = _core._WaitProbe()

    async def target() -> None:
        try:
            await probe.wait()
        finally:
            closed.names.append("target")

    async def joiner(joined: "_core._JoinTarget[None]") -> None:
        try:
            await joined.join()
        finally:
            closed.names.append("joiner")

    async def main() -> None:
        joined = _core._spawn_joinable(closed.track(target()))
        _core._spawn(closed.track(joiner(joined)))
        await wait_until(lambda: probe.waiting == 1)
        for _ in range(4):
            await checkpoint()

    _core._run(main())
    assert sorted(closed.names) == ["joiner", "target"]
    assert closed.all_freed()


def test_a_task_joining_a_task_that_waits_on_it_is_closed_by_the_end_of_run() -> None:
    # Two tasks that each join the other: a cycle of two joins.
    closed = Closed()
    joined: list[_core._JoinTarget[None]] = []

    async def first() -> None:
        try:
            await wait_until(lambda: len(joined) == 2)
            await joined[1].join()
        finally:
            closed.names.append("first")

    async def second() -> None:
        try:
            await wait_until(lambda: len(joined) == 2)
            await joined[0].join()
        finally:
            closed.names.append("second")

    async def main() -> None:
        joined.append(_core._spawn_joinable(closed.track(first())))
        joined.append(_core._spawn_joinable(closed.track(second())))
        for _ in range(8):
            await checkpoint()
        joined.clear()

    _core._run(main())
    assert sorted(closed.names) == ["first", "second"]
    assert closed.all_freed()


def test_a_scope_exit_waiting_on_a_child_blocked_forever_is_closed_by_the_end_of_run() -> None:
    closed = Closed()
    probe = _core._WaitProbe()

    async def child() -> None:
        try:
            await probe.wait()
        finally:
            closed.names.append("child")

    async def owner() -> None:
        try:
            async with porto.scope() as s:
                s.spawn(closed.track(child()))
        finally:
            closed.names.append("owner")

    async def main() -> None:
        _core._spawn(closed.track(owner()))
        await wait_until(lambda: probe.waiting == 1)
        for _ in range(4):
            await checkpoint()

    _core._run(main())
    assert sorted(closed.names) == ["child", "owner"]
    assert closed.all_freed()


def test_the_next_run_starts_with_nothing_left_from_the_last_one() -> None:
    probe = _core._WaitProbe()

    async def waiter() -> None:
        await probe.wait()

    async def first() -> None:
        _core._spawn(waiter())
        await wait_until(lambda: probe.waiting == 1)

    async def second() -> int:
        return probe.waiting

    _core._run(first())
    assert _core._run(second()) == 0
