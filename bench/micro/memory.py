"""idle_tasks_memory: the resident memory each idle task costs, porto against asyncio.

    <venv>/bin/python bench/micro/memory.py porto|asyncio|uvloop [--tasks 100000]

Prints one JSON object. Run each backend in its own process (``bench/micro/run.py`` does): the
interpreter's allocator keeps freed memory, so a second measurement in the same process would
reuse the first one's pages.

The program starts the runtime, measures the process's peak resident set (RSS), spawns ``--tasks``
tasks that each wait on one event (``async def idle(event): await event.wait()``), lets all of
them reach the wait, collects garbage and measures RSS again; the difference over the task count
is the cost of one idle task: the coroutine frame, the task object and the runtime's own
bookkeeping (porto: the task header and its wait node). ST-11 budgets 2.5 GB for 1M sleeping
tasks (03-stress-tests.md, 53-benchmarks.md §2), 2.5 KB each.
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import json
import resource
import sys
from typing import Any

import porto

#: ST-11: RSS < 2.5 GB for 1M sleeping tasks.
ST11_BYTES_PER_TASK = 2.5e9 / 1_000_000


def rss_bytes() -> int:
    """The process's peak resident set size so far (``getrusage``: bytes on macOS, KiB on
    Linux). The programs only grow between their two readings, so the peak is the current size;
    asking ``ps`` instead would fork a child, which uvloop's SIGCHLD handler then reaps first."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


class Waiting:
    def __init__(self) -> None:
        self.count = 0


async def porto_idle(event: porto.Event, waiting: Waiting) -> None:
    waiting.count += 1
    await event.wait()


async def porto_main(tasks: int) -> tuple[int, int]:
    event, waiting = porto.Event(), Waiting()
    gc.collect()
    before = after = rss_bytes()
    async with porto.scope() as s:
        for _ in range(tasks):
            s.spawn(porto_idle, event, waiting)
        while waiting.count < tasks:
            await porto.checkpoint()
        gc.collect()
        after = rss_bytes()
        event.set()
    return before, after


async def asyncio_idle(event: asyncio.Event, waiting: Waiting) -> None:
    waiting.count += 1
    await event.wait()


async def asyncio_main(tasks: int) -> tuple[int, int]:
    event, waiting = asyncio.Event(), Waiting()
    gc.collect()
    before = rss_bytes()
    async with asyncio.TaskGroup() as group:
        for _ in range(tasks):
            group.create_task(asyncio_idle(event, waiting))
        while waiting.count < tasks:
            await asyncio.sleep(0)
        gc.collect()
        after = rss_bytes()
        event.set()
    return before, after


def run_backend(backend: str, tasks: int) -> tuple[int, int]:
    if backend == "porto":
        return porto.run(porto_main, tasks)
    if backend == "asyncio":
        return asyncio.run(asyncio_main(tasks))
    if backend == "uvloop":
        import uvloop

        return uvloop.run(asyncio_main(tasks))
    raise SystemExit(f"unknown backend {backend!r}: pass porto, asyncio or uvloop")


def main() -> None:
    parser = argparse.ArgumentParser(description="RSS per idle task, one backend per process.")
    parser.add_argument("backend", choices=["porto", "asyncio", "uvloop"])
    parser.add_argument("--tasks", type=int, default=100_000)
    options = parser.parse_args()
    tasks: int = options.tasks
    before, after = run_backend(options.backend, tasks)
    result: dict[str, Any] = {
        "backend": options.backend,
        "python": sys.version.split()[0],
        "tasks": tasks,
        "rss_before": before,
        "rss_after": after,
        "bytes_per_task": (after - before) / tasks,
        "st11_bytes_per_task": ST11_BYTES_PER_TASK,
    }
    print(json.dumps(result))


if __name__ == "__main__":
    main()
