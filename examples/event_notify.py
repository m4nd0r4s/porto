"""Signalling between tasks: ``porto.Event`` and ``porto.Notify``.

    uv run python examples/event_notify.py

``Event`` is a one-shot flag: once set it stays set, and every waiter, current or future, goes
through. ``Notify`` is a wake-up call: ``notify_one()`` wakes the longest waiting task or stores
one permit for the next waiter, ``notify_all()`` wakes the tasks waiting now and stores nothing.
"""

from collections import deque

import porto


async def starting_gun(runners: int) -> list[float]:
    """Runners line up and wait for one Event; set() releases them all at once."""
    go = porto.Event()
    started_at: list[float] = []
    fired_at = 0.0  # bound before the block, so it can be read after it

    async def runner() -> None:
        await go.wait()
        started_at.append(porto.current_time())

    async with porto.scope() as s:
        for _ in range(runners):
            s.spawn(runner)
        await porto.sleep(0.01)  # everyone is waiting now
        assert started_at == [] and not go.is_set()
        fired_at = porto.current_time()
        go.set()
        go.set()  # setting it again does nothing
    assert go.is_set()
    await go.wait()  # a set event lets late waiters straight through
    return [moment - fired_at for moment in started_at]


async def work_queue(items: list[str]) -> list[str]:
    """A consumer sleeps on a Notify until the producer says a new item is queued."""
    queue: deque[str | None] = deque()
    ready = porto.Notify()
    handled: list[str] = []

    async def consumer() -> None:
        while True:
            while not queue:
                await ready.notified()
            item = queue.popleft()
            if item is None:  # the producer is done
                return
            handled.append(item.upper())

    async with porto.scope() as s:
        s.spawn(consumer)
        for item in [*items, None]:
            queue.append(item)
            ready.notify_one()  # a permit is stored if the consumer is busy
            await porto.checkpoint()
    return handled


async def permits() -> None:
    notify = porto.Notify()
    # notify_one() with nobody waiting stores one permit (at most one)...
    notify.notify_one()
    notify.notify_one()
    await notify.notified()  # ...which this await consumes without waiting.

    # notify_all() wakes only the tasks waiting now and stores nothing.
    woken: list[str] = []

    async def waiter(name: str) -> None:
        await notify.notified()
        woken.append(name)

    async with porto.scope() as s:
        s.spawn(waiter, "early-1")
        s.spawn(waiter, "early-2")
        await porto.sleep(0.005)
        notify.notify_all()
        await porto.sleep(0.005)
        assert sorted(woken) == ["early-1", "early-2"], woken

        # A waiter that arrives after notify_all() finds no permit (the second permit
        # above was not stored either) and waits for the next notify.
        late = s.spawn(waiter, "late")
        await porto.sleep(0.005)
        assert not late.done()
        notify.notify_one()
    assert woken[-1] == "late"


async def main() -> None:
    delays = await starting_gun(5)
    assert len(delays) == 5 and all(0 <= delay < 0.5 for delay in delays), delays
    assert await work_queue(["a", "b", "c"]) == ["A", "B", "C"]
    await permits()
    print("event_notify: ok")


if __name__ == "__main__":
    porto.run(main)
