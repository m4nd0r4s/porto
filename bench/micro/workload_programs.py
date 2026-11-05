"""A realistic mix: a worker pool fed by a channel, reporting through another, sharing a Lock.

The porto program and its asyncio twin run the same pipeline: a producer puts jobs into a
capacity-64 queue, 8 workers take them, update shared statistics under one lock and put the
results into a second capacity-64 queue, and a collector sums them. One op is one job.
"""

from __future__ import annotations

import asyncio
import time

import porto
from programs import Benchmark, variants

JOBS = 10_000
WORKERS = 8
CAPACITY = 64


class Stats:
    """What the workers update under the shared lock."""

    def __init__(self) -> None:
        self.done = 0
        self.total = 0


def _check(stats: Stats, collected: int) -> None:
    expected = 2 * sum(range(JOBS))
    if stats.done != JOBS or collected != expected:
        raise RuntimeError(
            f"expected {JOBS} jobs summing to {expected}, got {stats.done} summing to "
            f"{collected}: a worker lost a job"
        )


async def porto_feed(jobs: porto.Sender[int]) -> None:
    async with jobs:
        send = jobs.send
        for job in range(JOBS):
            await send(job)


async def porto_worker(
    jobs: porto.Receiver[int], results: porto.Sender[int], lock: porto.Lock, stats: Stats
) -> None:
    async with results:
        send = results.send
        async for job in jobs:
            result = job * 2
            async with lock:
                stats.done += 1
                stats.total += result
            await send(result)


async def porto_collect(results: porto.Receiver[int], collected: list[int]) -> None:
    async for result in results:
        collected[0] += result


async def porto_worker_pool(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        jobs_tx, jobs_rx = porto.channel[int](CAPACITY)
        results_tx, results_rx = porto.channel[int](CAPACITY)
        lock, stats, collected = porto.Lock(), Stats(), [0]
        started = time.perf_counter()
        async with porto.scope() as s:
            s.spawn(porto_feed, jobs_tx)
            for _ in range(WORKERS):
                s.spawn(porto_worker, jobs_rx.clone(), results_tx.clone(), lock, stats)
            jobs_rx.close()
            results_tx.close()
            s.spawn(porto_collect, results_rx, collected)
        elapsed += time.perf_counter() - started
        _check(stats, collected[0])
    return elapsed


async def asyncio_feed(jobs: asyncio.Queue[int | None]) -> None:
    """``JOBS`` jobs, then one ``None`` per worker: asyncio's twin of closing the channel."""
    put = jobs.put
    for job in range(JOBS):
        await put(job)
    for _ in range(WORKERS):
        await put(None)


async def asyncio_worker(
    jobs: asyncio.Queue[int | None], results: asyncio.Queue[int], lock: asyncio.Lock, stats: Stats
) -> None:
    get, put = jobs.get, results.put
    while (job := await get()) is not None:
        result = job * 2
        async with lock:
            stats.done += 1
            stats.total += result
        await put(result)


async def asyncio_collect(results: asyncio.Queue[int], collected: list[int]) -> None:
    get = results.get
    for _ in range(JOBS):
        collected[0] += await get()


async def asyncio_worker_pool(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        jobs: asyncio.Queue[int | None] = asyncio.Queue(CAPACITY)
        results: asyncio.Queue[int] = asyncio.Queue(CAPACITY)
        lock, stats, collected = asyncio.Lock(), Stats(), [0]
        started = time.perf_counter()
        async with asyncio.TaskGroup() as group:
            group.create_task(asyncio_feed(jobs))
            for _ in range(WORKERS):
                group.create_task(asyncio_worker(jobs, results, lock, stats))
            group.create_task(asyncio_collect(results, collected))
        elapsed += time.perf_counter() - started
        _check(stats, collected[0])
    return elapsed


BENCHMARKS: list[Benchmark] = [
    *variants("worker_pool", porto_worker_pool, asyncio_worker_pool, inner_loops=JOBS),
]
