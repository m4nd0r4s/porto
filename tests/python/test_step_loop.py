"""The production step loop and the cooperative budget (11-task-model-and-trap-protocol.md §4,
§5.3 T2; 12-scheduler-and-lanes.md §10).

``_JoinTarget.steps`` counts how many times the driver resumed a task's coroutine (``send``
or ``throw``): each test pins the steps one branch of the loop takes. Tasks that must run
while another loops record what they see, so the tests read the lane's interleaving from it.
"""

import contextlib
import contextvars
import gc
import itertools
import types
import warnings
from collections.abc import Awaitable, Callable, Coroutine, Generator
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


async def steps_and_outcome(coro: Coroutine[Any, Any, object]) -> tuple[int, object]:
    """Runs ``coro`` as its own task: how many steps it took, and its result or exception."""
    target = _core._spawn_joinable(coro)
    try:
        outcome: object = await target.join()
    except BaseException as exception:
        outcome = exception
    return target.steps, outcome


class Unsupported:
    """An awaitable whose iterator yields an object porto cannot wait for."""

    def __await__(self) -> Generator[object, None, None]:
        yield "not a trap"


# One test per branch of the loop: PYGEN_RETURN, PYGEN_ERROR, PYGEN_NEXT (trap, None, other)


def test_a_coroutine_that_returns_at_once_takes_one_step() -> None:
    async def returns() -> int:
        return 7

    assert run(steps_and_outcome(returns())) == (1, 7)


def test_a_coroutine_that_raises_at_once_takes_one_step() -> None:
    async def raises() -> None:
        raise LookupError("raised")

    steps, outcome = run(steps_and_outcome(raises()))
    assert steps == 1
    assert isinstance(outcome, LookupError)
    assert outcome.args == ("raised",)


def test_a_yielded_trap_is_waited_for_then_resumed() -> None:
    async def yields_twice() -> str:
        await checkpoint()
        await checkpoint()
        return "done"

    assert run(steps_and_outcome(yields_twice())) == (3, "done")


def test_a_bare_yield_is_a_yield() -> None:
    @types.coroutine
    def bare_yield() -> Generator[None, None, None]:
        yield

    async def yields() -> str:
        await bare_yield()
        return "done"

    assert run(steps_and_outcome(yields())) == (2, "done")


def test_an_unsupported_object_is_thrown_back_as_type_error_in_the_same_poll() -> None:
    async def awaits_unsupported() -> str:
        try:
            await Unsupported()
        except TypeError as error:
            return str(error)
        return "no error"

    steps, message = run(steps_and_outcome(awaits_unsupported()))
    assert steps == 2, "one send, then one throw"
    assert isinstance(message, str)
    assert "porto cannot wait for" in message
    assert "of type str" in message
    assert "porto.asyncio" in message, "the error says what to do"


def test_a_thrown_error_that_is_not_caught_ends_the_task() -> None:
    async def awaits_unsupported() -> None:
        await Unsupported()

    steps, outcome = run(steps_and_outcome(awaits_unsupported()))
    assert steps == 2
    assert isinstance(outcome, TypeError)


def test_a_throw_the_coroutine_catches_and_returns_from_is_its_result() -> None:
    async def recovers() -> str:
        try:
            await Unsupported()
        except TypeError:
            return "recovered"
        return "no error"

    assert run(steps_and_outcome(recovers())) == (2, "recovered")


def test_a_throw_the_coroutine_catches_and_awaits_again_continues() -> None:
    async def recovers_then_yields() -> str:
        try:
            await Unsupported()
        except TypeError:
            await checkpoint()
        return "done"

    assert run(steps_and_outcome(recovers_then_yields())) == (3, "done")


# Synchronous continue: a wait that is already over costs a step, not a scheduler round trip


def test_waits_that_are_already_over_continue_without_a_round_trip() -> None:
    joins = 50

    async def seven() -> int:
        return 7

    async def main() -> None:
        finished = _core._spawn_joinable(seven())
        await wait_until(lambda: finished.done)
        ticks = [0]

        async def ticker() -> None:
            while ticks[0] >= 0:
                ticks[0] += 1
                await checkpoint()

        _core._spawn(ticker())

        async def joiner() -> int:
            before = ticks[0]
            for _ in range(joins):
                assert await finished.join() == 7
            return ticks[0] - before

        steps, ticks_meanwhile = await steps_and_outcome(joiner())
        ticks[0] = -1
        assert ticks_meanwhile == 0, "a join went through the scheduler"
        assert steps == 1 + joins, "one step per join, all in the first poll"

    run(main())


# The cooperative budget (T2, 12 §10)


def peer_samples(
    budget: int, operations: int, operation: Callable[[], Awaitable[object]]
) -> list[int]:
    """Runs a loop of ``operations`` awaits of ``operation()`` with a ``budget`` per poll next
    to a peer task: what the loop's progress was at each of the peer's turns."""
    done = [0]
    samples: list[int] = []

    async def peer() -> None:
        while done[0] < operations:
            samples.append(done[0])
            await checkpoint()

    async def main() -> None:
        _core._spawn(peer())
        await checkpoint()
        for _ in range(operations):
            await operation()
            done[0] += 1
        await checkpoint()

    porto.run(main, coop_budget=budget)
    return samples


@pytest.mark.parametrize("budget", [1, 10, 128])
def test_a_loop_of_ready_awaits_yields_every_coop_budget_awaits(budget: int) -> None:
    samples = peer_samples(budget, 1000, lambda: _core._checkpoint_if_needed(None))
    # The first sample is taken before the loop starts, the second after its first poll (which
    # also spent a unit on the step that started it); from there the peer runs once per
    # `budget` awaits of the loop.
    progress = [later - earlier for earlier, later in itertools.pairwise(samples[1:])]
    assert len(samples) >= 1000 // budget, f"the peer starved: {samples}"
    assert set(progress) == {budget}, progress


def test_a_loop_of_plain_ready_awaitables_never_yields() -> None:
    # `_Ready` is the raw fast path: only checkpoint_if_needed counts against the budget.
    samples = peer_samples(10, 1000, lambda: _core._make_ready(None))
    assert samples == [0]


def test_a_loop_over_unsupported_awaits_cannot_hang_the_lane() -> None:
    async def caught() -> None:
        with contextlib.suppress(TypeError):
            await Unsupported()

    samples = peer_samples(10, 1000, caught)
    assert len(samples) >= 1000 // 10, "each throw spends a unit of the budget"


def test_a_task_out_of_budget_goes_to_the_back_of_the_queue() -> None:
    budget = 8
    done = [0]
    turns: list[tuple[str, int]] = []

    async def peer(name: str) -> None:
        while done[0] < 100:
            turns.append((name, done[0]))
            await checkpoint()

    async def main() -> None:
        _core._spawn(peer("a"))
        _core._spawn(peer("b"))
        await checkpoint()
        for _ in range(100):
            await _core._checkpoint_if_needed(None)
            done[0] += 1

    porto.run(main, coop_budget=budget)
    # Both peers run between two polls of the loop: it was requeued behind them, not ahead.
    assert len(turns) >= 2 * (100 // budget)
    for first, second in zip(turns[::2], turns[1::2], strict=False):
        assert (first[0], second[0]) == ("a", "b")
        assert first[1] == second[1], turns


def test_the_budget_is_reset_at_each_poll() -> None:
    async def main() -> list[object]:
        await checkpoint()
        first = [_core._checkpoint_if_needed(index) for index in range(4)]
        await checkpoint()
        second = [_core._checkpoint_if_needed(index) for index in range(4)]
        return [*first, *second]

    awaitables = porto.run(main, coop_budget=4)
    kinds = [type(item).__name__ for item in awaitables]
    # Each poll spends a unit on the step that resumed it: three readies, then the trap.
    assert kinds == ["_Ready"] * 3 + ["_Trap"] + ["_Ready"] * 3 + ["_Trap"]


# A context that cannot be entered


def test_a_task_whose_context_cannot_be_entered_fails_without_a_never_awaited_warning() -> None:
    context = contextvars.copy_context()
    started = [False]

    async def child() -> None:
        started[0] = True

    async def main() -> BaseException | None:
        coro = child()
        # The lane runs inside `context.run`, so the context is already entered.
        target = _core._spawn_joinable(coro, context)
        try:
            await target.join()
        except RuntimeError as error:
            assert coro.cr_frame is None, "the coroutine was closed"
            return error
        return None

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        error = context.run(run, main())
        gc.collect()
    assert isinstance(error, RuntimeError)
    assert "already entered" in str(error)
    assert not started[0]
    never_awaited = [w for w in caught if "never awaited" in str(w.message)]
    assert never_awaited == []


def test_spawn_joinable_refuses_a_context_that_is_not_one() -> None:
    async def child() -> None:
        pass

    async def main() -> None:
        coro = child()
        with pytest.raises(TypeError, match=r"context must be a contextvars\.Context"):
            _core._spawn_joinable(coro, "not a context")  # pyright: ignore[reportArgumentType]
        coro.close()

    run(main())


def test_steps_cannot_be_read_by_the_task_itself() -> None:
    holder: list[_core._JoinTarget[None]] = []

    async def reads_itself() -> None:
        await wait_until(lambda: bool(holder))
        with pytest.raises(RuntimeError, match="another task"):
            _ = holder[0].steps

    async def main() -> None:
        target = _core._spawn_joinable(reads_itself())
        holder.append(target)
        await target.join()

    run(main())
