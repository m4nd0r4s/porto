"""The trap protocol's awaitables: _Trap and _Ready (11-task-model-and-trap-protocol.md §5-§6).

A minimal driver steps coroutines with ``coro.send()``, as the Rust step driver will with
``PyIter_Send``. ``_core._send`` calls ``PyIter_Send`` itself, to reach the ``am_send`` slots.
"""

import sys
import types
from collections.abc import Callable, Coroutine, Generator
from itertools import repeat
from typing import Any, TypeVar

import pytest

from porto import _core

ITERATIONS = 100_000
# Blocks a run may keep that do not scale with the awaits (a frame chunk, cached objects).
FIXED_BLOCKS = 16


def drive(coro: Coroutine[Any, Any, object], respond: Callable[[_core._Trap], object]) -> object:
    """Runs ``coro`` to completion, answering every trap it yields with ``respond(trap)``."""
    value: object = None
    while True:
        try:
            trap = coro.send(value)
        except StopIteration as stop:
            return stop.value
        assert isinstance(trap, _core._Trap)
        value = respond(trap)


def no_traps(trap: _core._Trap) -> object:
    raise AssertionError(f"the coroutine suspended on {trap!r}")


_T = TypeVar("_T")


def in_a_runtime(function: Callable[[], _T]) -> _T:
    """Calls ``function`` from a task of a fresh runtime: sleep traps read its clock."""

    async def main() -> _T:
        return function()

    return _core._run(main())


def test_the_driver_receives_the_trap_with_its_payload() -> None:
    def check() -> None:
        before = _core.current_time()
        trap = _core._sleep_trap(1.5)

        async def main() -> object:
            return await trap

        coro = main()
        assert coro.send(None) is trap
        assert trap.kind == "sleep"
        assert trap.deadline is not None
        assert before + 1.5 <= trap.deadline <= _core.current_time() + 1.5
        coro.close()

    in_a_runtime(check)


def test_a_yield_trap_has_no_deadline() -> None:
    trap = _core._yield_trap()
    assert trap.kind == "yield"
    assert trap.deadline is None
    assert repr(trap) == "<porto._core._Trap yield fresh>"


@pytest.mark.parametrize("delay", [-1.0, float("nan")])
def test_a_sleep_trap_rejects_a_negative_or_nan_delay(delay: float) -> None:
    with pytest.raises(ValueError, match="sleep delay must be a number of seconds >= 0"):
        _core._sleep_trap(delay)


def test_a_sleep_trap_outside_a_runtime_raises_and_says_what_to_do() -> None:
    with pytest.raises(RuntimeError, match=r"porto.sleep\(\) was called outside porto.run"):
        _core._sleep_trap(1)


def test_an_infinite_sleep_trap_never_ends() -> None:
    def check() -> None:
        trap = _core._sleep_trap(float("inf"))
        assert trap.deadline is not None
        assert trap.deadline > _core.current_time() + 100 * 365 * 24 * 3600

    in_a_runtime(check)


def test_a_trap_in_a_nested_chain_evaluates_to_the_value_the_driver_sends() -> None:
    async def leaf() -> object:
        return await _core._yield_trap()

    async def middle() -> tuple[object, object]:
        first = await leaf()
        second = await leaf()
        return first, second

    async def main() -> object:
        return await middle()

    answers = iter(["one", (2, "a tuple stays one value")])
    result = drive(main(), lambda trap: next(answers))
    assert result == ("one", (2, "a tuple stays one value"))


def test_a_trap_evaluates_to_none_when_resumed_with_next() -> None:
    trap = _core._yield_trap()
    assert next(trap) is trap
    with pytest.raises(StopIteration) as stop:
        next(trap)
    assert stop.value.value is None


def test_throw_reraises_inside_the_coroutine_at_the_await() -> None:
    async def main() -> str:
        try:
            await _core._yield_trap()
        except ValueError as error:
            return f"caught {error}"
        return "not thrown"

    coro = main()
    coro.send(None)
    with pytest.raises(StopIteration) as stop:
        coro.throw(ValueError("boom"))
    assert stop.value.value == "caught boom"


def test_an_uncaught_throw_propagates_out_of_the_chain() -> None:
    async def leaf() -> object:
        return await _core._yield_trap()

    async def main() -> object:
        return await leaf()

    coro = main()
    coro.send(None)
    with pytest.raises(KeyError, match="cancelled"):
        coro.throw(KeyError("cancelled"))


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        ((ValueError,), ValueError()),
        ((ValueError, "message"), ValueError("message")),
        ((ValueError, ("a", "b")), ValueError("a", "b")),
        ((ValueError, ValueError("instance")), ValueError("instance")),
    ],
)
def test_throw_accepts_the_legacy_signature(args: tuple[Any, ...], expected: BaseException) -> None:
    trap = _core._yield_trap()
    next(trap)
    with pytest.raises(type(expected)) as raised:
        trap.throw(*args)
    assert raised.value.args == expected.args


def test_throw_rejects_a_non_exception() -> None:
    trap: Any = _core._yield_trap()
    with pytest.raises(TypeError, match="deriving from BaseException, not int"):
        trap.throw(42)


def test_throw_rejects_an_instance_with_a_separate_value() -> None:
    trap: Any = _core._yield_trap()
    with pytest.raises(TypeError, match="takes no separate value"):
        trap.throw(ValueError("x"), "y")


def test_throw_attaches_the_given_traceback() -> None:
    try:
        raise ValueError("with a traceback")
    except ValueError as error:
        traceback = error.__traceback__
    assert traceback is not None
    trap = _core._yield_trap()
    with pytest.raises(ValueError, match="fresh") as raised:
        trap.throw(ValueError, "fresh", traceback)
    assert raised.value.__traceback__ is not None
    assert raised.value.__traceback__.tb_next is traceback


def test_awaiting_a_trap_twice_raises_trap_reused() -> None:
    trap = _core._yield_trap()

    async def main() -> None:
        await trap
        await trap

    coro = main()
    coro.send(None)
    with pytest.raises(RuntimeError, match=r"^trap reused"):
        coro.send("first answer")


RESUMES_WITH_NONE: dict[str, Callable[[_core._Trap], object]] = {
    "next": next,
    "send": lambda trap: trap.send(None),
    "am_send": lambda trap: _core._send(trap, None),
}


@pytest.mark.parametrize("resume", RESUMES_WITH_NONE.values(), ids=RESUMES_WITH_NONE.keys())
def test_resuming_a_completed_trap_raises_trap_reused(
    resume: Callable[[_core._Trap], object],
) -> None:
    trap = _core._yield_trap()
    assert _core._send(trap, None) == (False, trap)
    assert _core._send(trap, "answer") == (True, "answer")
    with pytest.raises(RuntimeError, match=r"^trap reused: .*create a new trap for each await"):
        resume(trap)


@pytest.mark.parametrize("finish", ["close", "throw"])
def test_a_closed_or_thrown_trap_cannot_be_resumed(finish: str) -> None:
    trap = _core._yield_trap()
    next(trap)
    if finish == "close":
        trap.close()
    else:
        with pytest.raises(ValueError, match="x"):
            trap.throw(ValueError("x"))
    with pytest.raises(RuntimeError, match=r"^trap reused"):
        trap.send(None)


EARLY_SENDS: dict[str, Callable[[_core._Trap], object]] = {
    "send": lambda trap: trap.send("too early"),
    "am_send": lambda trap: _core._send(trap, "too early"),
}


@pytest.mark.parametrize("send", EARLY_SENDS.values(), ids=EARLY_SENDS.keys())
def test_sending_a_value_into_a_fresh_trap_raises(send: Callable[[_core._Trap], object]) -> None:
    trap = _core._yield_trap()
    with pytest.raises(TypeError, match="first resume of an awaitable must be send"):
        send(trap)
    # The failed send did not consume the trap.
    assert next(trap) is trap


def test_a_trap_works_with_yield_from_in_generator_coroutines() -> None:
    @types.coroutine
    def legacy() -> Generator[Any, Any, object]:
        return (yield from _core._yield_trap())

    async def main() -> object:
        return await legacy()

    assert drive(main(), lambda trap: trap.kind * 2) == "yieldyield"


@pytest.mark.parametrize("value", [42, None, ("a", "tuple"), ValueError("an exception value")])
def test_awaiting_a_ready_returns_its_value_without_suspending(value: object) -> None:
    async def main() -> object:
        return await _core._make_ready(value)

    assert drive(main(), no_traps) is value


def test_a_ready_can_be_awaited_more_than_once() -> None:
    ready = _core._make_ready("same")

    async def main() -> tuple[str, str]:
        return await ready, await ready

    assert drive(main(), no_traps) == ("same", "same")


def test_a_ready_returns_its_value_through_am_send() -> None:
    value = object()
    assert _core._send(_core._make_ready(value), None) == (True, value)


def test_a_ready_raises_what_is_thrown_into_it() -> None:
    with pytest.raises(LookupError, match="thrown"):
        _core._make_ready(1).throw(LookupError("thrown"))


@pytest.mark.parametrize("cls", [_core._Trap, _core._Ready])
def test_the_awaitables_cannot_be_created_from_python(cls: type[object]) -> None:
    with pytest.raises(TypeError):
        cls()


def test_a_trap_fits_its_size_budget() -> None:
    # 25-cpython-integration-and-memory.md §5: a Trap is at most 48 bytes.
    assert sys.getsizeof(_core._yield_trap()) <= 48


# Allocations. `_count_allocations` counts every malloc, calloc and realloc CPython makes (MEM and
# OBJ domains) while the function runs; each measurement subtracts the same loop without awaits.


def per_await_allocations(measured: Callable[[], object], baseline: Callable[[], object]) -> float:
    measured()  # warm up: freelists, specialised bytecode
    baseline()
    extra = _core._count_allocations(measured) - _core._count_allocations(baseline)
    # Rounded so the fixed costs of a run (a few blocks per 100k awaits) do not show.
    return round(extra / ITERATIONS, 3)


def run(coro: Coroutine[Any, Any, object]) -> None:
    drive(coro, no_traps)


async def await_ready_in_a_loop(ready: "_core._Ready[object]") -> None:
    for _ in repeat(None, ITERATIONS):
        await ready


async def await_fresh_readies_in_a_loop() -> None:
    value = object()
    for _ in repeat(None, ITERATIONS):
        await _core._make_ready(value)


async def loop_without_awaits() -> None:
    for _ in repeat(None, ITERATIONS):
        pass


def test_a_ready_resumed_through_am_send_allocates_nothing() -> None:
    ready = _core._make_ready(object())

    def send_in_a_loop() -> None:
        for _ in repeat(None, ITERATIONS):
            _core._send(ready, None)

    def loop() -> None:
        for _ in repeat(None, ITERATIONS):
            pass

    assert per_await_allocations(send_in_a_loop, loop) == 0


# From 3.12 the SEND opcode resumes a non-generator awaitable with `tp_iternext` (for None) or its
# `send` method, never `am_send`, so the await ends in a StopIteration: one allocation. On 3.11
# SEND goes through PyIter_Send, hence am_send, and allocates nothing.
NESTED_AWAIT_ALLOCATIONS = 0 if sys.version_info < (3, 12) else 1


def test_a_nested_await_of_a_ready_allocates_at_most_the_stop_iteration() -> None:
    ready = _core._make_ready(object())
    allocations = per_await_allocations(
        lambda: run(await_ready_in_a_loop(ready)), lambda: run(loop_without_awaits())
    )
    assert allocations == NESTED_AWAIT_ALLOCATIONS


def test_a_fresh_ready_per_await_comes_from_the_freelist() -> None:
    allocations = per_await_allocations(
        lambda: run(await_fresh_readies_in_a_loop()), lambda: run(loop_without_awaits())
    )
    assert allocations == NESTED_AWAIT_ALLOCATIONS


def test_awaiting_a_ready_leaks_no_memory_blocks() -> None:
    ready = _core._make_ready(object())
    run(await_ready_in_a_loop(ready))
    before = sys.getallocatedblocks()
    run(await_ready_in_a_loop(ready))
    assert sys.getallocatedblocks() - before <= FIXED_BLOCKS
