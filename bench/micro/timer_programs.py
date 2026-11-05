"""Timer benchmarks: ``porto.sleep`` on the timer wheel against ``asyncio.sleep`` (the loop's
timer heap) doing the same work.

53-benchmarks.md §2 "sleep(0.001) x 100k tasks" (timers: at least 2 times asyncio+uvloop) and
14-time-and-timers.md.
"""

from __future__ import annotations

import asyncio
import time

import porto
from programs import Benchmark, variants

#: Sleeping tasks per batch of the sleep row.
SLEEPERS = 100_000
#: Sleeps armed and cancelled per batch of the churn row.
CHURN = 1_000
#: The sleep of the churn row: long enough never to fire.
NEVER = 3600.0


class Woken:
    def __init__(self) -> None:
        self.count = 0


def _check(count: int, expected: int, what: str) -> None:
    if count != expected:
        raise RuntimeError(f"expected {expected} {what}, got {count}: a task stopped early")


# sleep_100k: 100k tasks each sleeping 1 ms, spawned into one scope and joined, per batch ------
# The time includes spawning the 100k and the 1 ms itself; the wait is the same on both sides.


async def porto_sleeper(woken: Woken) -> None:
    await porto.sleep(0.001)
    woken.count += 1


async def porto_sleep_100k(loops: int) -> float:
    tasks = range(SLEEPERS)
    elapsed = 0.0
    for _ in range(loops):
        woken = Woken()
        started = time.perf_counter()
        async with porto.scope() as s:
            spawn = s.spawn
            for _ in tasks:
                spawn(porto_sleeper, woken)
        elapsed += time.perf_counter() - started
        _check(woken.count, SLEEPERS, "sleepers woken")
    return elapsed


async def asyncio_sleeper(woken: Woken) -> None:
    await asyncio.sleep(0.001)
    woken.count += 1


async def asyncio_sleep_100k(loops: int) -> float:
    tasks = range(SLEEPERS)
    elapsed = 0.0
    for _ in range(loops):
        woken = Woken()
        started = time.perf_counter()
        async with asyncio.TaskGroup() as group:
            create_task = group.create_task
            for _ in tasks:
                create_task(asyncio_sleeper(woken))
        elapsed += time.perf_counter() - started
        _check(woken.count, SLEEPERS, "sleepers woken")
    return elapsed


# sleep_cancel_churn: 1000 tasks arm a sleep that never fires and are cancelled, per sleep -----
# Spawn, arm the timer, cancel, unlink it and unwind: the life of a timeout that did not fire.
# porto cancels the scope; asyncio cancels each task (a TaskGroup cannot be cancelled).


async def porto_long_sleeper(armed: Woken) -> None:
    armed.count += 1
    await porto.sleep(NEVER)


async def porto_sleep_cancel_churn(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        armed = Woken()
        started = time.perf_counter()
        async with porto.scope() as s:
            for _ in range(CHURN):
                s.spawn(porto_long_sleeper, armed)
            while armed.count < CHURN:
                await porto.checkpoint()
            s.cancel()
        elapsed += time.perf_counter() - started
    return elapsed


async def asyncio_long_sleeper(armed: Woken) -> None:
    armed.count += 1
    await asyncio.sleep(NEVER)


async def asyncio_sleep_cancel_churn(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        armed = Woken()
        started = time.perf_counter()
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(asyncio_long_sleeper(armed)) for _ in range(CHURN)]
            while armed.count < CHURN:
                await asyncio.sleep(0)
            for task in tasks:
                task.cancel()
        elapsed += time.perf_counter() - started
    return elapsed


BENCHMARKS: list[Benchmark] = [
    # The batch sleeps 1 ms in the poll: no readyfd variant (it would spin instead).
    *variants("sleep_100k", porto_sleep_100k, asyncio_sleep_100k, inner_loops=1, ready_fd=False),
    *variants(
        "sleep_cancel_churn",
        porto_sleep_cancel_churn,
        asyncio_sleep_cancel_churn,
        inner_loops=CHURN,
    ),
]
