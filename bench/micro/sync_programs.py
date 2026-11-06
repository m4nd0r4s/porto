"""Sync-primitive benchmarks: ``porto.Lock``, ``Mutex``, ``Semaphore``, ``Event`` and ``Notify``
against ``asyncio.Lock``, ``asyncio.Semaphore``, ``asyncio.Event`` and ``asyncio.Condition``
doing the same work.

53-benchmarks.md §2 "Lock contention 100 tasks" (at least 2 times asyncio+uvloop) and
16-sync-primitives-and-channels.md §1. Each porto program and its asyncio twin run the same
number of tasks through the same critical sections, with the same yields inside them.
"""

from __future__ import annotations

import asyncio
import time

import porto
from programs import Benchmark, variants

#: Tasks of the contended-lock row.
CONTENDERS = 100
#: Waiters of the event row.
WAITERS = 10_000


def _check(value: int, expected: int) -> None:
    if value != expected:
        raise RuntimeError(
            f"expected {expected} increments, got {value}: mutual exclusion broke or a task "
            "stopped early"
        )


class Counter:
    """What the critical sections protect."""

    def __init__(self) -> None:
        self.value = 0

    def check(self, expected: int) -> None:
        _check(self.value, expected)


# lock_uncontended: `async with lock` with nobody else around, per acquire+release -------------


async def porto_lock_uncontended(loops: int) -> float:
    lock = porto.Lock()
    counter = Counter()
    started = time.perf_counter()
    for _ in range(loops):
        async with lock:
            counter.value += 1
    elapsed = time.perf_counter() - started
    counter.check(loops)
    return elapsed


async def asyncio_lock_uncontended(loops: int) -> float:
    lock = asyncio.Lock()
    counter = Counter()
    started = time.perf_counter()
    for _ in range(loops):
        async with lock:
            counter.value += 1
    elapsed = time.perf_counter() - started
    counter.check(loops)
    return elapsed


# semaphore_uncontended: `async with semaphore` with permits to spare, per acquire+release -----

#: Permits of the uncontended-semaphore row: more than one, so it is not a lock in disguise.
SEMAPHORE_PERMITS = 4


async def porto_semaphore_uncontended(loops: int) -> float:
    semaphore = porto.Semaphore(SEMAPHORE_PERMITS)
    counter = Counter()
    started = time.perf_counter()
    for _ in range(loops):
        async with semaphore:
            counter.value += 1
    elapsed = time.perf_counter() - started
    counter.check(loops)
    return elapsed


async def asyncio_semaphore_uncontended(loops: int) -> float:
    semaphore = asyncio.Semaphore(SEMAPHORE_PERMITS)
    counter = Counter()
    started = time.perf_counter()
    for _ in range(loops):
        async with semaphore:
            counter.value += 1
    elapsed = time.perf_counter() - started
    counter.check(loops)
    return elapsed


# lock_contended: 100 tasks, each yields once inside the critical section, per acquisition -----
# The yield makes every other task find the lock held and queue up, so each release hands the
# lock to the first waiter (FIFO on both sides). One op is one acquire+release.


async def porto_contender(lock: porto.Lock, counter: Counter, rounds: int) -> None:
    checkpoint = porto.checkpoint
    for _ in range(rounds):
        async with lock:
            counter.value += 1
            await checkpoint()


async def porto_lock_contended(loops: int) -> float:
    lock = porto.Lock()
    counter = Counter()
    started = time.perf_counter()
    async with porto.scope() as s:
        for _ in range(CONTENDERS):
            s.spawn(porto_contender, lock, counter, loops)
    elapsed = time.perf_counter() - started
    counter.check(CONTENDERS * loops)
    return elapsed


async def asyncio_contender(lock: asyncio.Lock, counter: Counter, rounds: int) -> None:
    sleep = asyncio.sleep
    for _ in range(rounds):
        async with lock:
            counter.value += 1
            await sleep(0)


async def asyncio_lock_contended(loops: int) -> float:
    lock = asyncio.Lock()
    counter = Counter()
    started = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        for _ in range(CONTENDERS):
            group.create_task(asyncio_contender(lock, counter, loops))
    elapsed = time.perf_counter() - started
    counter.check(CONTENDERS * loops)
    return elapsed


# mutex_value: read and replace a value the lock owns, uncontended, per op ---------------------
# porto.Mutex[int] keeps the value behind the lock (`guard.value`, `guard.set`); asyncio has no
# such type, so its twin is a Lock beside the value.


async def porto_mutex_value(loops: int) -> float:
    mutex = porto.Mutex[int](0)
    started = time.perf_counter()
    for _ in range(loops):
        async with mutex as guard:
            guard.set(guard.value + 1)
    elapsed = time.perf_counter() - started
    async with mutex as guard:
        _check(guard.value, loops)
    return elapsed


async def asyncio_mutex_value(loops: int) -> float:
    lock = asyncio.Lock()
    counter = Counter()
    started = time.perf_counter()
    for _ in range(loops):
        async with lock:
            counter.value = counter.value + 1
    elapsed = time.perf_counter() - started
    counter.check(loops)
    return elapsed


# event_wakeup_10k: set() with 10k tasks waiting, until all of them ran, per waiter ------------
# Spawning the waiters and letting them block is outside the time; the time runs from `set()`
# to the scope (task group) exit, which needs every waiter to have resumed and finished.


async def porto_waiter(event: porto.Event, waiting: Counter) -> None:
    waiting.value += 1
    await event.wait()


async def porto_event_wakeup(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        event = porto.Event()
        waiting = Counter()
        started = time.perf_counter()
        async with porto.scope() as s:
            for _ in range(WAITERS):
                s.spawn(porto_waiter, event, waiting)
            while waiting.value < WAITERS:
                await porto.checkpoint()
            started = time.perf_counter()
            event.set()
        elapsed += time.perf_counter() - started
    return elapsed


async def asyncio_waiter(event: asyncio.Event, waiting: Counter) -> None:
    waiting.value += 1
    await event.wait()


async def asyncio_event_wakeup(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        event = asyncio.Event()
        waiting = Counter()
        async with asyncio.TaskGroup() as group:
            for _ in range(WAITERS):
                group.create_task(asyncio_waiter(event, waiting))
            while waiting.value < WAITERS:
                await asyncio.sleep(0)
            started = time.perf_counter()
            event.set()
        elapsed += time.perf_counter() - started
    return elapsed


# notify_handoff: two tasks waking each other in turn, per handoff (inner_loops=2) -------------
# porto: one Notify per direction (`notify_one` stores a permit if the other side is not
# waiting yet). asyncio's primitive for this is a Condition: the waker sets whose turn it is and
# notifies under the condition's lock, the woken side waits for its turn.


async def porto_notify_side(
    mine: porto.Notify, theirs: porto.Notify, rounds: int, first: bool
) -> None:
    for _ in range(rounds):
        if first:
            theirs.notify_one()
            await mine.notified()
        else:
            await mine.notified()
            theirs.notify_one()


async def porto_notify_handoff(loops: int) -> float:
    ping, pong = porto.Notify(), porto.Notify()
    started = time.perf_counter()
    async with porto.scope() as s:
        s.spawn(porto_notify_side, ping, pong, loops, True)
        s.spawn(porto_notify_side, pong, ping, loops, False)
    return time.perf_counter() - started


class Turn:
    def __init__(self) -> None:
        self.whose = "ping"


async def asyncio_condition_side(
    condition: asyncio.Condition, turn: Turn, me: str, other: str, rounds: int
) -> None:
    for _ in range(rounds):
        async with condition:
            await condition.wait_for(lambda: turn.whose == me)
            turn.whose = other
            condition.notify()


async def asyncio_notify_handoff(loops: int) -> float:
    condition = asyncio.Condition()
    turn = Turn()
    started = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        group.create_task(asyncio_condition_side(condition, turn, "ping", "pong", loops))
        group.create_task(asyncio_condition_side(condition, turn, "pong", "ping", loops))
    return time.perf_counter() - started


BENCHMARKS: list[Benchmark] = [
    *variants("lock_uncontended", porto_lock_uncontended, asyncio_lock_uncontended),
    *variants("semaphore_uncontended", porto_semaphore_uncontended, asyncio_semaphore_uncontended),
    *variants(
        "lock_contended", porto_lock_contended, asyncio_lock_contended, inner_loops=CONTENDERS
    ),
    *variants("mutex_value", porto_mutex_value, asyncio_mutex_value),
    # A handful of loop iterations per batch: the poll is noise, so no readyfd variant.
    *variants(
        "event_wakeup_10k",
        porto_event_wakeup,
        asyncio_event_wakeup,
        inner_loops=WAITERS,
        ready_fd=False,
    ),
    *variants("notify_handoff", porto_notify_handoff, asyncio_notify_handoff, inner_loops=2),
]
