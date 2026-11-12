"""Time: ``sleep``, ``sleep_until``, ``sleep_forever``, ``checkpoint`` and the runtime clock.

    uv run python examples/sleep_and_clock.py

The runtime clock counts seconds since the runtime started and is what sleep deadlines are
measured on. ``sleep(s)`` never resumes early; ``sleep_until(t)`` waits for a time on that clock;
``sleep_forever()`` waits until cancelled. ``sleep(0)``, a deadline already passed and
``checkpoint()`` all yield once so other ready tasks run. (Not named ``time.py``: that would
shadow the standard ``time`` module.)
"""

import porto


async def clock_basics() -> None:
    assert porto.monotonic is porto.current_time  # an alias, not a second clock
    first = porto.current_time()
    second = porto.monotonic()
    assert 0 <= first <= second, "the clock is monotonic and starts near 0"


async def sleep_never_early() -> None:
    for seconds in (0.001, 0.005, 0.02):
        started = porto.current_time()
        await porto.sleep(seconds)
        assert porto.current_time() - started >= seconds


async def sleeps_overlap() -> None:
    """Ten tasks sleeping 20 ms each take about 20 ms together, not 200 ms."""
    started = porto.current_time()
    async with porto.scope() as s:
        for _ in range(10):
            s.spawn(porto.sleep, 0.02)
    elapsed = porto.current_time() - started
    assert 0.02 <= elapsed < 0.15, f"sleeps did not overlap: {elapsed:.3f} s"


async def wake_order() -> list[str]:
    """Sleepers wake in deadline order, whatever order they started in."""
    woke: list[str] = []

    async def sleeper(name: str, seconds: float) -> None:
        await porto.sleep(seconds)
        woke.append(name)

    async with porto.scope() as s:
        s.spawn(sleeper, "slow", 0.03)
        s.spawn(sleeper, "fast", 0.01)
        s.spawn(sleeper, "medium", 0.02)
    return woke


async def sleep_until_a_time() -> None:
    """``sleep_until`` takes a time on the runtime clock: the right tool for a fixed schedule."""
    started = porto.current_time()
    tick = started
    for _ in range(3):
        tick += 0.01  # tick 10 ms after the previous *deadline*, not after the previous wake-up
        await porto.sleep_until(tick)  # so work done between ticks does not make them drift
        assert porto.current_time() >= tick
    assert porto.current_time() - started >= 0.03

    # A deadline already passed returns after one yield, like sleep(0).
    await porto.sleep_until(started)


async def sleep_forever_until_cancelled() -> None:
    """``sleep_forever()`` never returns: only a cancel ends it (``sleep(inf)`` is the same)."""
    log: list[str] = []

    async def parked() -> None:
        try:
            await porto.sleep_forever()
        finally:
            log.append("woken by the cancel")

    async with porto.move_on_after(0.01) as s:
        s.spawn(parked)  # a task that waits for work that may never come
    assert s.timed_out and log == ["woken by the cancel"]

    # The deadline of a sleep can be infinite, or a mistake: NaN is refused, with a hint.
    try:
        await porto.sleep(float("nan"))
    except ValueError as error:
        assert "sleep_forever" in str(error), error
    else:
        raise AssertionError("sleep(nan) was accepted")


async def zero_sleep_is_a_checkpoint() -> list[str]:
    """``sleep(0)`` is ``checkpoint()``: it yields once, so the other task gets a turn."""
    trace: list[str] = []

    async def chatter(name: str) -> None:
        for step in range(2):
            trace.append(f"{name}{step}")
            await porto.sleep(0)

    async with porto.scope() as s:
        s.spawn(chatter, "a")
        s.spawn(chatter, "b")
    return trace


async def checkpoints_interleave() -> list[str]:
    """A long loop that calls checkpoint() lets other tasks run between its steps."""
    trace: list[str] = []

    async def crunch(name: str, steps: int) -> None:
        for step in range(steps):
            trace.append(f"{name}{step}")
            await porto.checkpoint()

    async with porto.scope() as s:
        s.spawn(crunch, "a", 3)
        s.spawn(crunch, "b", 3)
    return trace


async def main() -> None:
    await clock_basics()
    await sleep_never_early()
    await sleeps_overlap()
    assert await wake_order() == ["fast", "medium", "slow"]
    # Without the checkpoints the trace would be a0 a1 a2 b0 b1 b2: b would wait for a.
    trace = await checkpoints_interleave()
    assert sorted(trace) == ["a0", "a1", "a2", "b0", "b1", "b2"], trace
    assert trace.index("b0") < trace.index("a2"), f"a ran without yielding: {trace}"
    await sleep_until_a_time()
    await sleep_forever_until_cancelled()
    assert await zero_sleep_is_a_checkpoint() == ["a0", "b0", "a1", "b1"]
    print("sleep_and_clock: ok")


if __name__ == "__main__":
    porto.run(main)
