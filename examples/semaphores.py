"""Limiting concurrency: ``porto.Semaphore`` and ``porto.CapacityLimiter``.

    uv run python examples/semaphores.py

Spawning a task per job is cheap, but the thing the jobs talk to (a server, a connection pool, a
disk) often takes only a few at a time. Both primitives let at most N tasks into a section; the
rest wait in arrival order. A ``Semaphore`` counts permits that can be taken several at a time;
a ``CapacityLimiter`` (AnyIO's API) lends one token per borrower and can be resized while it
runs.
"""

import porto


class Gauge:
    """Counts how many tasks are inside a section at once, and the most there ever were."""

    def __init__(self) -> None:
        self.inside = 0
        self.peak = 0

    def enter(self) -> None:
        self.inside += 1
        self.peak = max(self.peak, self.inside)

    def leave(self) -> None:
        self.inside -= 1


async def rate_limited_fan_out() -> None:
    """Twelve downloads are spawned at once, but only three run at a time."""
    slots = porto.Semaphore(3)
    gauge = Gauge()
    done: list[int] = []

    async def download(job: int) -> None:
        async with slots:  # one permit; released when the block exits, even on an error
            gauge.enter()
            await porto.sleep(0.005)
            gauge.leave()
        done.append(job)

    started = porto.current_time()
    async with porto.scope() as s:
        for job in range(12):
            s.spawn(download, job)
    assert sorted(done) == list(range(12))
    assert gauge.peak == 3, gauge.peak  # never more than the three permits
    assert slots.available == 3  # everything was given back
    # Twelve jobs, three at a time: four rounds of 5 ms, not one and not twelve.
    assert porto.current_time() - started >= 0.02


async def several_permits() -> None:
    """``acquire(n)`` takes n permits at once: a big job can claim more of the capacity."""
    slots = porto.Semaphore(4)
    await slots.acquire(3)  # a "heavy" job
    assert slots.available == 1
    assert not slots.try_acquire(2)  # not enough left, and try_acquire never waits
    assert slots.try_acquire()  # one is
    assert slots.available == 0
    slots.release(3)
    slots.release()
    assert slots.available == 4

    # The semaphore is bounded: giving back more than was taken is a bug, reported at once.
    try:
        slots.release()
    except ValueError:
        pass
    else:
        raise AssertionError("released a permit that was never acquired")


async def capacity_limiter() -> None:
    """``CapacityLimiter``: a token per borrower, which is the running task by default."""
    limiter = porto.CapacityLimiter(2)
    gauge = Gauge()

    async def call_api() -> None:
        async with limiter:
            gauge.enter()
            await porto.sleep(0.03)  # long enough that the checks below see everyone in place
            gauge.leave()

    async with porto.scope() as s:
        for _ in range(6):
            s.spawn(call_api)
        await porto.sleep(0.002)  # two are inside, four are waiting
        assert limiter.borrowed_tokens == 2 and limiter.available_tokens == 0
    assert gauge.peak == 2
    assert limiter.borrowed_tokens == 0 and limiter.available_tokens == 2

    # Without a wait: acquire_nowait borrows now, or raises porto.WouldBlock if none is free.
    limiter.acquire_nowait()  # for this task
    assert limiter.borrowed_tokens == 1
    try:
        limiter.acquire_nowait()  # this task holds a token already: one per borrower
    except RuntimeError:
        pass
    else:
        raise AssertionError("borrowed two tokens for one task")
    limiter.release()


async def borrowing_for_someone_else() -> None:
    """``*_on_behalf_of`` lends a token to any object, for work that is not a task of its own."""
    limiter = porto.CapacityLimiter(1)
    connection_a, connection_b = object(), object()  # compared by identity
    await limiter.acquire_on_behalf_of(connection_a)
    try:
        limiter.acquire_on_behalf_of_nowait(connection_b)
    except porto.WouldBlock:
        pass
    else:
        raise AssertionError("a second token was lent")
    limiter.release_on_behalf_of(connection_a)
    limiter.acquire_on_behalf_of_nowait(connection_b)  # free again
    limiter.release_on_behalf_of(connection_b)

    # The limit can change while tasks run: raising it lets waiting tasks in at once.
    gauge = Gauge()

    async def worker() -> None:
        async with limiter:
            gauge.enter()
            await porto.sleep(0.05)
            gauge.leave()

    async with porto.scope() as s:
        for _ in range(4):
            s.spawn(worker)
        await porto.sleep(0.002)
        assert gauge.inside == 1  # the limit is 1
        limiter.total_tokens = 4
        await porto.sleep(0.002)
        assert gauge.inside == 4, gauge.inside  # the other three came in
    assert limiter.total_tokens == 4


async def main() -> None:
    await rate_limited_fan_out()
    await several_permits()
    await capacity_limiter()
    await borrowing_for_someone_else()
    print("semaphores: ok")


if __name__ == "__main__":
    porto.run(main)
