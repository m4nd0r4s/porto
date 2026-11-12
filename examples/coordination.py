"""Coordinating tasks: ``porto.RwLock``, ``porto.Condition`` and ``porto.Barrier``.

    uv run python examples/coordination.py

Three ways to make tasks agree on when to go on. ``RwLock``: many readers, or one writer, for
data that is read much more than it changes. ``Condition``: a lock plus "wait until the state I
need is there" (``wait_for``). ``Barrier``: nobody starts the next phase until everyone has
finished this one.
"""

from collections import deque

import porto


async def readers_and_a_writer() -> None:
    """Readers share the lock; a writer has it alone, and a waiting writer is not starved."""
    lock = porto.RwLock()
    config = {"version": 1}
    versions_read: list[int] = []
    peak_readers = 0

    async def reader() -> None:
        nonlocal peak_readers
        async with lock.read():
            peak_readers = max(peak_readers, lock.readers)
            await porto.sleep(0.03)  # slow enough for the others to arrive
            versions_read.append(config["version"])

    async def writer() -> None:
        async with lock.write():
            assert lock.readers == 0 and lock.write_locked()  # alone
            config["version"] = 2

    async with porto.scope() as s:
        for _ in range(3):
            s.spawn(reader)
        await porto.sleep(0.001)
        assert lock.readers == 3  # all three hold it together
        s.spawn(writer)
    assert peak_readers == 3
    assert versions_read == [1, 1, 1]  # the writer waited for the readers to leave
    assert config == {"version": 2}


async def writers_are_preferred() -> None:
    """A reader arriving while a writer waits goes behind it, so writers cannot starve."""
    lock = porto.RwLock()
    order: list[str] = []

    async def writer() -> None:
        async with lock.write():
            order.append("writer")

    async def late_reader() -> None:
        async with lock.read():
            order.append("late reader")

    await lock.acquire_read()  # an early reader holds the lock
    async with porto.scope() as s:
        s.spawn(writer)
        await porto.checkpoint()  # the writer is waiting now
        assert not lock.try_acquire_read()  # a new reader may not jump the queue
        s.spawn(late_reader)
        await porto.checkpoint()
        order.append("early reader leaves")
        lock.release_read()
    assert order == ["early reader leaves", "writer", "late reader"], order


async def producer_and_consumer() -> list[int]:
    """A Condition guards the queue; consumers sleep until there is something to take."""
    queue: deque[int] = deque()
    condition = porto.Condition()
    taken: list[int] = []

    async def consumer(count: int) -> None:
        for _ in range(count):
            async with condition:
                # wait_for checks the predicate first, then after every notification, always
                # with the lock held, and returns what the predicate returned.
                await condition.wait_for(lambda: len(queue) > 0)
                taken.append(queue.popleft())

    async def producer() -> None:
        for item in range(6):
            await porto.sleep(0.002)
            async with condition:
                queue.append(item)
                condition.notify()  # wake one consumer; notify_all() would wake them all

    async with porto.scope() as s:
        s.spawn(consumer, 3)
        s.spawn(consumer, 3)
        s.spawn(producer)
    assert not condition.locked()
    return sorted(taken)


async def condition_misuse() -> None:
    """``wait`` and ``notify`` need the lock: using them without it is an error, not a hang."""
    condition = porto.Condition()
    try:
        condition.notify()
    except RuntimeError:
        pass
    else:
        raise AssertionError("notify() without the lock did not raise")


async def barrier_rounds() -> None:
    """Three workers run two phases; nobody starts phase 2 before everyone finished phase 1."""
    barrier = porto.Barrier(3)
    assert barrier.parties == 3
    log: list[str] = []
    leaders: list[str] = []

    async def worker(name: str, slowness: float) -> None:
        for phase in (1, 2):
            await porto.sleep(slowness)  # the work, which takes each worker a different time
            log.append(f"{name} finished phase {phase}")
            if await barrier.wait():  # True for the last to arrive, False for the others
                leaders.append(f"{name} led phase {phase}")  # e.g. merge the phase's results
            log.append(f"{name} starts phase {phase + 1}")

    async with porto.scope() as s:
        s.spawn(worker, "fast", 0.001)
        s.spawn(worker, "medium", 0.003)
        s.spawn(worker, "slow", 0.006)
    assert barrier.n_waiting == 0

    # Every "finished phase 1" came before every "starts phase 2" (and so on).
    last_finish_1 = max(i for i, line in enumerate(log) if line.endswith("finished phase 1"))
    first_start_2 = min(i for i, line in enumerate(log) if line.endswith("starts phase 2"))
    assert last_finish_1 < first_start_2, log
    # The slowest worker arrives last each round, so it leads both.
    assert leaders == ["slow led phase 1", "slow led phase 2"], leaders


async def main() -> None:
    await readers_and_a_writer()
    await writers_are_preferred()
    assert await producer_and_consumer() == list(range(6))
    await condition_misuse()
    await barrier_rounds()
    print("coordination: ok")


if __name__ == "__main__":
    porto.run(main)
