"""The trap kinds and the trap rules T1-T4 (11-task-model-and-trap-protocol.md §5.2, §5.3).

Trap kinds that no public API makes yet come from test hooks: ``_WaitProbe().wait()`` is a
``Notified`` trap, ``_spawn_joinable(coro).join()`` a ``Join`` trap. ``_interrupt(token, exc)``
throws into a waiting task the way cancellation will (porto-szb.36): the wait is given up
first (T3), then the exception is raised at the ``await``.
"""

import asyncio
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import pytest

from porto import _core

# The cooperative budget per poll (12-scheduler-and-lanes.md §1, coop_budget).
COOP_BUDGET = 128


async def checkpoint() -> None:
    await _core._yield_trap()


async def wait_until(condition: Callable[[], bool]) -> None:
    while not condition():
        await checkpoint()


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return _core._run(coro)


async def await_it(awaitable: Awaitable[object]) -> object:
    return await awaitable


def yields_of(awaitable: Awaitable[object], answer: object) -> tuple[list[object], object]:
    """Drives ``await awaitable`` by hand: what it yielded, and what the await evaluated to when
    every yield is answered with ``answer``."""
    coro = await_it(awaitable)
    yielded: list[object] = []
    value: object = None
    while True:
        try:
            yielded.append(coro.send(value))
        except StopIteration as stop:
            return yielded, stop.value
        value = answer


class Ticker:
    """A task that counts its turns: it advances only when the lane runs other tasks."""

    def __init__(self) -> None:
        self.ticks = 0
        self.stopped = False

    async def run(self) -> None:
        while not self.stopped:
            self.ticks += 1
            await checkpoint()


# T1: every Porto awaitable completes synchronously or yields exactly one trap


def test_T1_a_ready_awaitable_completes_without_a_trap() -> None:
    assert yields_of(_core._make_ready(5), answer=None) == ([], 5)


def test_T1_each_trap_kind_yields_exactly_one_trap() -> None:
    async def blocked(probe: _core._WaitProbe) -> None:
        await probe.wait()

    async def main() -> None:
        probe = _core._WaitProbe()
        target = _core._spawn_joinable(blocked(probe))
        traps: list[tuple[str, Awaitable[object]]] = [
            ("yield", _core._yield_trap()),
            ("sleep", _core._sleep_trap(1.0)),
            ("notified", probe.wait()),
            ("join", target.join()),
        ]
        for kind, trap in traps:
            yielded, value = yields_of(trap, answer="sent")
            assert yielded == [trap], kind
            assert isinstance(trap, _core._Trap)
            assert trap.kind == kind
            assert value == "sent"
        await wait_until(lambda: probe.waiting == 1)
        probe.notify_all()

    run(main())


def test_T1_scope_entry_and_exit_complete_synchronously() -> None:
    async def main() -> None:
        scope = _core.scope()
        assert yields_of(scope.__aenter__(), answer=None) == ([], scope)
        assert yields_of(scope.__aexit__(None, None, None), answer=None) == ([], None)

    run(main())


def test_T1_a_wait_that_is_already_over_resumes_without_a_round_trip() -> None:
    async def seven() -> int:
        return 7

    async def main() -> None:
        target = _core._spawn_joinable(seven())
        await wait_until(lambda: target.done)
        ticker = Ticker()
        _core._spawn(ticker.run())
        await checkpoint()
        before = ticker.ticks
        assert await target.join() == 7
        assert ticker.ticks == before, "the join went through the scheduler"
        ticker.stopped = True

    run(main())


# T2: synchronous completion is a checkpoint


def test_T2_checkpoint_on_sync_completion() -> None:
    rounds = 10 * COOP_BUDGET

    async def main() -> tuple[int, int]:
        ticker = Ticker()
        _core._spawn(ticker.run())
        await checkpoint()
        # Plain ready awaitables never suspend: the other task starves.
        before = ticker.ticks
        for index in range(rounds):
            assert await _core._make_ready(index) == index
        starved = ticker.ticks - before
        # Through checkpoint_if_needed, the budget makes the loop yield now and then.
        before = ticker.ticks
        for index in range(rounds):
            assert await _core._checkpoint_if_needed(index) == index
        served = ticker.ticks - before
        ticker.stopped = True
        return starved, served

    starved, served = run(main())
    assert starved == 0
    assert served >= rounds // COOP_BUDGET - 1


def test_T2_the_budget_turns_a_sync_completion_into_a_yield_trap() -> None:
    async def main() -> list[object]:
        await checkpoint()
        return [_core._checkpoint_if_needed(index) for index in range(COOP_BUDGET + 1)]

    awaitables = run(main())
    traps = [index for index, item in enumerate(awaitables) if isinstance(item, _core._Trap)]
    assert traps, "the budget never ran out"
    first = awaitables[traps[0]]
    assert isinstance(first, _core._Trap)
    assert first.kind == "yield"
    assert all(isinstance(item, _core._Ready) for item in awaitables[: traps[0]])
    # The trap resumes with the stashed value.
    assert yields_of(first, answer=traps[0]) == ([first], traps[0])


def test_T2_outside_a_task_a_sync_completion_is_a_ready_awaitable() -> None:
    ready = _core._checkpoint_if_needed("value")
    assert isinstance(ready, _core._Ready)
    assert yields_of(ready, answer=None) == ([], "value")


def test_T2_entering_and_exiting_empty_scopes_does_not_starve_the_lane() -> None:
    async def main() -> int:
        ticker = Ticker()
        _core._spawn(ticker.run())
        await checkpoint()
        before = ticker.ticks
        for _ in range(4 * COOP_BUDGET):
            async with _core.scope():
                pass
        ticker.stopped = True
        return ticker.ticks - before

    assert run(main()) > 0


# T3: a pending trap given up unregisters its wakers and loses nothing


def test_T3_trap_cancel_safe() -> None:
    probe = _core._WaitProbe()
    tokens: list[_core._TaskToken] = []

    async def waiter() -> str:
        tokens.append(_core._current_task_token())
        try:
            await probe.wait()
        except KeyError:
            return "interrupted"
        return "notified"

    async def main() -> None:
        task = _core._spawn_joinable(waiter())
        await wait_until(lambda: probe.waiting == 1)
        _core._interrupt(tokens[0], KeyError("cancelled"))
        assert await task.join() == "interrupted"
        # The waker is unregistered: a later notify finds nobody.
        assert probe.waiting == 0
        assert probe.notify_all() == 0

    run(main())


def test_T3_a_wait_granted_before_the_interrupt_keeps_its_grant() -> None:
    probe = _core._WaitProbe()
    tokens: list[_core._TaskToken] = []

    async def waiter() -> list[str]:
        tokens.append(_core._current_task_token())
        seen: list[str] = []
        for _ in range(2):
            try:
                await probe.wait()
                seen.append("notified")
            except KeyError:
                seen.append("interrupted")
        return seen

    async def main() -> None:
        task = _core._spawn_joinable(waiter())
        await wait_until(lambda: probe.waiting == 1)
        # The grant lands first: the wait is over, so the grant is not lost to the interrupt,
        # which the task gets at its next wait instead.
        assert probe.notify_one()
        _core._interrupt(tokens[0], KeyError("cancelled"))
        assert await task.join() == ["notified", "interrupted"]
        assert probe.waiting == 0

    run(main())


def test_T3_a_join_given_up_leaves_the_target_running() -> None:
    probe = _core._WaitProbe()
    tokens: list[_core._TaskToken] = []

    async def target_body() -> str:
        await probe.wait()
        return "target done"

    async def joiner(target: "_core._JoinTarget[str]") -> str:
        tokens.append(_core._current_task_token())
        try:
            return await target.join()
        except KeyError:
            return "joiner interrupted"

    async def main() -> None:
        target = _core._spawn_joinable(target_body())
        await wait_until(lambda: probe.waiting == 1)
        joining = _core._spawn_joinable(joiner(target))
        await wait_until(lambda: bool(tokens))
        await checkpoint()
        _core._interrupt(tokens[0], KeyError("cancelled"))
        assert await joining.join() == "joiner interrupted"
        # The joiner left the target's join list; the target finishes, and joins, normally.
        probe.notify_all()
        assert await target.join() == "target done"

    run(main())


def test_T3_a_sleep_given_up_resumes_before_its_deadline() -> None:
    tokens: list[_core._TaskToken] = []

    async def sleeper() -> float:
        tokens.append(_core._current_task_token())
        start = _core.current_time()
        with pytest.raises(KeyError):
            await _core._sleep_trap(60.0)
        return _core.current_time() - start

    async def main() -> float:
        task = _core._spawn_joinable(sleeper())
        await wait_until(lambda: bool(tokens))
        await checkpoint()
        _core._interrupt(tokens[0], KeyError("cancelled"))
        return await task.join()

    assert run(main()) < 10.0


def test_T3_an_interrupt_waits_for_a_wait_that_is_not_over() -> None:
    async def main() -> str:
        probe = _core._WaitProbe()
        _core._interrupt(_core._current_task_token(), KeyError("later"))
        # Yields are over at once: the interrupt stays filed.
        await checkpoint()
        await checkpoint()
        with pytest.raises(KeyError, match="later"):
            await probe.wait()
        assert probe.waiting == 0
        return "done"

    assert run(main()) == "done"


# Join


def test_a_join_returns_the_result_or_raises_the_exception() -> None:
    error = LookupError("from the target")

    async def succeed() -> str:
        await checkpoint()
        return "result"

    async def fail() -> None:
        await checkpoint()
        raise error

    async def main() -> None:
        succeeding = _core._spawn_joinable(succeed())
        failing = _core._spawn_joinable(fail())
        assert await succeeding.join() == "result"
        with pytest.raises(LookupError) as raised:
            await failing.join()
        assert raised.value is error
        # Joining again reads the same outcome.
        assert await succeeding.join() == "result"

    run(main())


def test_several_tasks_join_the_same_target() -> None:
    probe = _core._WaitProbe()

    async def target_body() -> int:
        await probe.wait()
        return 42

    async def joiner(target: "_core._JoinTarget[int]") -> int:
        return await target.join() + 1

    async def main() -> list[int]:
        target = _core._spawn_joinable(target_body())
        joiners = [_core._spawn_joinable(joiner(target)) for _ in range(3)]
        await wait_until(lambda: probe.waiting == 1)
        for _ in range(3):
            await checkpoint()
        probe.notify_all()
        return [await each.join() for each in joiners]

    assert run(main()) == [43, 43, 43]


def test_a_task_that_awaits_itself_gets_an_error() -> None:
    targets: list[_core._JoinTarget[object]] = []

    async def selfish() -> str:
        await wait_until(lambda: bool(targets))
        try:
            await targets[0].join()
        except RuntimeError as error:
            return str(error)
        return "joined itself"

    async def main() -> object:
        targets.append(_core._spawn_joinable(selfish()))
        return await targets[0].join()

    message = run(main())
    assert isinstance(message, str)
    assert "awaited itself" in message


# Notified


def test_a_notified_wait_resumes_on_notify() -> None:
    probe = _core._WaitProbe()

    async def waiter() -> str:
        await probe.wait()
        return "woken"

    async def main() -> None:
        task = _core._spawn_joinable(waiter())
        await wait_until(lambda: probe.waiting == 1)
        assert not task.done
        assert probe.notify_all() == 1
        assert await task.join() == "woken"

    run(main())


def test_trap_reprs_name_their_kind() -> None:
    assert repr(_core._WaitProbe().wait()) == "<porto._core._Trap notified fresh>"


# T4: a trap outside a porto task


def test_T4_a_trap_awaited_by_an_asyncio_task_is_refused_until_the_facade() -> None:
    # The asyncio facade (§22, M4) will handle it through `_asyncio_future_blocking`; until
    # then asyncio rejects the yielded trap instead of hanging.
    async def main() -> None:
        await _core._yield_trap()

    with pytest.raises(RuntimeError, match="bad yield"):
        asyncio.run(main())
