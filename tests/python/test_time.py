"""Sleeps on the timer wheel (14-time-and-timers.md §3-§4, 20-python-api-reference.md Time).

``sleep(seconds <= 0)`` and ``sleep_until`` of a passed deadline are pure checkpoints: they
yield once and raise ``Cancelled`` in a cancelled scope. ``sleep_until`` never wakes before its
deadline on ``current_time()``. ``sleep_forever`` (and an infinite sleep) only ends by
cancellation, and a cancelled sleep leaves no entry in the wheel (counted against a baseline:
the runtime arms timers of its own, such as the lane's signal check). NaN is refused, saying what
to pass instead.
"""

import math
import warnings
from collections.abc import Awaitable, Callable
from typing import Any, NoReturn

import pytest

import porto
from porto import _core

#: The calls that wait until a cancellation: they never end on their own.
FOREVER: list[tuple[str, Callable[[], Awaitable[Any]]]] = [
    ("sleep_forever()", porto.sleep_forever),
    ("sleep(inf)", lambda: porto.sleep(math.inf)),
    ("sleep_until(inf)", lambda: porto.sleep_until(math.inf)),
    ("sleep_until(1e300)", lambda: porto.sleep_until(1e300)),
]

#: The calls whose deadline is already reached: checkpoints.
CHECKPOINTS: list[tuple[str, Callable[[], Awaitable[None]]]] = [
    ("sleep(0)", lambda: porto.sleep(0)),
    ("sleep(-0.0)", lambda: porto.sleep(-0.0)),
    ("sleep(-1)", lambda: porto.sleep(-1)),
    ("sleep(-inf)", lambda: porto.sleep(-math.inf)),
    ("sleep_until(0)", lambda: porto.sleep_until(0)),
    ("sleep_until(-inf)", lambda: porto.sleep_until(-math.inf)),
    ("sleep_until(now)", lambda: porto.sleep_until(porto.current_time())),
]


# A deadline already reached is a checkpoint


@pytest.mark.parametrize("make", [make for _, make in CHECKPOINTS], ids=[n for n, _ in CHECKPOINTS])
def test_a_reached_deadline_yields_to_the_other_tasks_once(
    make: Callable[[], Awaitable[None]],
) -> None:
    events: list[str] = []

    async def other() -> None:
        events.append("other")

    async def main() -> None:
        baseline = _core._armed_timers()
        async with porto.scope() as s:
            s.spawn(other)
            events.append("before")
            await make()
            events.append("after")
        assert _core._armed_timers() == baseline

    porto.run(main)
    # The spawned task ran during the await: it yielded.
    assert events == ["before", "other", "after"]


@pytest.mark.parametrize("make", [make for _, make in CHECKPOINTS], ids=[n for n, _ in CHECKPOINTS])
def test_a_reached_deadline_raises_cancelled_in_a_cancelled_scope(
    make: Callable[[], Awaitable[None]],
) -> None:
    async def main() -> None:
        async with porto.scope() as s:
            s.cancel()
            await make()
            pytest.fail("not reached: a checkpoint in a cancelled scope raises")
        assert s.cancelled_caught

    porto.run(main)


def test_a_negative_sleep_is_a_checkpoint_not_an_error() -> None:
    async def main() -> float:
        before = porto.current_time()
        await porto.sleep(-1)
        return porto.current_time() - before

    # Not a wait: well under a timer tick and a park.
    assert porto.run(main) < 0.5


# sleep_until never wakes early


def test_sleep_until_never_wakes_before_its_deadline() -> None:
    async def main() -> list[tuple[float, float]]:
        woken: list[tuple[float, float]] = []
        for offset in (0.05, 0.0013, 0.0004, 0.002_000_7):
            deadline = porto.current_time() + offset
            await porto.sleep_until(deadline)
            woken.append((deadline, porto.current_time()))
        return woken

    for deadline, now in porto.run(main):
        assert now >= deadline, f"woke at {now!r}, before {deadline!r}"


def test_sleep_until_does_not_wait_much_past_its_deadline() -> None:
    async def main() -> float:
        deadline = porto.current_time() + 0.05
        await porto.sleep_until(deadline)
        return porto.current_time() - deadline

    # Generous for CI: a missed deadline would be a hang, not a few ms.
    assert porto.run(main) < 1.0


def test_sleeps_until_the_same_deadline_wake_in_the_order_they_slept() -> None:
    order: list[int] = []

    async def sleeper(index: int, deadline: float) -> None:
        await porto.sleep_until(deadline)
        order.append(index)

    async def main() -> None:
        deadline = porto.current_time() + 0.02
        async with porto.scope() as s:
            for index in range(5):
                s.spawn(sleeper, index, deadline)

    porto.run(main)
    assert order == [0, 1, 2, 3, 4]


# Only a cancellation ends sleep_forever


@pytest.mark.parametrize("make", [make for _, make in FOREVER], ids=[n for n, _ in FOREVER])
def test_an_endless_sleep_only_ends_by_cancellation_and_leaves_no_timer(
    make: Callable[[], Awaitable[Any]],
) -> None:
    outcome: list[str] = []

    async def sleeper() -> None:
        try:
            await make()
        except porto.Cancelled:
            outcome.append("cancelled")
            raise
        outcome.append("woke up")

    async def main() -> None:
        baseline = _core._armed_timers()
        async with porto.scope() as s:
            s.spawn(sleeper)
            # Long enough for the lane to park with the sleeper's timer armed.
            await porto.sleep(0.05)
            assert _core._armed_timers() == baseline + 1
            assert outcome == []
            s.cancel()
        assert s.cancelled_caught
        assert _core._armed_timers() == baseline

    porto.run(main)
    assert outcome == ["cancelled"]


def test_sleep_forever_is_typed_no_return() -> None:
    async def wait() -> NoReturn:
        await porto.sleep_forever()

    async def main() -> None:
        async with porto.scope() as s:
            s.spawn(wait)
            await porto.checkpoint()
            s.cancel()

    porto.run(main)


def test_a_cancelled_sleep_until_leaves_no_timer() -> None:
    outcome: list[str] = []

    async def sleeper() -> None:
        try:
            await porto.sleep_until(porto.current_time() + 3600)
        except porto.Cancelled:
            outcome.append("cancelled")
            raise

    async def main() -> None:
        baseline = _core._armed_timers()
        async with porto.scope() as s:
            for _ in range(100):
                s.spawn(sleeper)
            await porto.sleep(0.01)
            assert _core._armed_timers() == baseline + 100
            s.cancel()
        assert _core._armed_timers() == baseline

    porto.run(main)
    assert outcome == ["cancelled"] * 100


def test_a_fired_sleep_leaves_no_timer() -> None:
    async def main() -> None:
        baseline = _core._armed_timers()
        await porto.sleep_until(porto.current_time() + 0.002)
        await porto.sleep(0.002)
        assert _core._armed_timers() == baseline

    porto.run(main)


# NaN, and calls outside a runtime


def test_a_nan_sleep_raises_and_says_what_to_do() -> None:
    async def main() -> None:
        with pytest.raises(ValueError, match=r"porto.sleep\(\) got NaN seconds.*sleep_forever"):
            await porto.sleep(math.nan)

    porto.run(main)


def test_a_nan_deadline_raises_and_says_what_to_do() -> None:
    async def main() -> None:
        with pytest.raises(
            ValueError, match=r"sleep_until\(\) got a NaN deadline.*porto.current_time\(\) \+ 5"
        ):
            await porto.sleep_until(math.nan)

    porto.run(main)


@pytest.mark.parametrize(
    ("name", "call"),
    [
        ("sleep_until", lambda: porto.sleep_until(1)),
        ("sleep_forever", porto.sleep_forever),
    ],
)
def test_outside_run_raises_and_says_what_to_do(name: str, call: Callable[[], object]) -> None:
    with pytest.raises(porto.NoRuntimeError, match=rf"porto.{name}\(\) was called outside"):
        call()


def test_the_armed_timer_hook_outside_run_raises() -> None:
    with pytest.raises(porto.NoRuntimeError, match="call it from a task"):
        _core._armed_timers()


@pytest.mark.parametrize(
    ("call", "make"),
    [
        ("porto.sleep_until(...)", lambda: porto.sleep_until(porto.current_time() + 1)),
        ("porto.sleep_forever()", porto.sleep_forever),
        ("porto.sleep(...)", lambda: porto.sleep(0)),
    ],
)
def test_a_sleep_never_awaited_warns_naming_its_call(
    call: str, make: Callable[[], Awaitable[Any]]
) -> None:
    async def main() -> list[str]:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            make()  # pyright: ignore[reportUnusedCallResult]
        return [str(warning.message) for warning in caught]

    assert porto.run(main) == [
        f"porto: {call} was never awaited, so it did nothing; add `await` in front of the call"
    ]


def test_sleep_until_and_sleep_forever_are_exported() -> None:
    assert porto.sleep_until is _core.sleep_until
    assert porto.sleep_forever is _core.sleep_forever
    assert {"sleep_until", "sleep_forever"} <= set(porto.__all__)
