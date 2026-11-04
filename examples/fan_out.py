"""Fan-out: spawn many tasks in one scope; each yields once and returns a value; sum them.

    uv run python examples/fan_out.py [--tasks 10000] [--backend porto|asyncio|uvloop]

The asyncio and uvloop backends run the same program with a TaskGroup, for a rough
comparison. The official benchmark is M0's spawn/switch suite.
"""

import argparse
import asyncio
import time
from collections.abc import Callable

import porto


async def porto_work(index: int) -> int:
    await porto.checkpoint()
    return index


async def porto_main(tasks: int) -> int:
    async with porto.scope() as s:
        spawned = [s.spawn(porto_work, index) for index in range(tasks)]
    return sum(task.result() for task in spawned)


async def asyncio_work(index: int) -> int:
    await asyncio.sleep(0)
    return index


async def asyncio_main(tasks: int) -> int:
    async with asyncio.TaskGroup() as group:
        spawned = [group.create_task(asyncio_work(index)) for index in range(tasks)]
    return sum(task.result() for task in spawned)


def runner(backend: str) -> Callable[[int], int]:
    if backend == "porto":
        return lambda tasks: porto.run(porto_main, tasks)
    if backend == "uvloop":
        import uvloop

        return lambda tasks: uvloop.run(asyncio_main(tasks))
    return lambda tasks: asyncio.run(asyncio_main(tasks))


def best_of(repeat: int, run: Callable[[int], int], tasks: int) -> tuple[int, float]:
    """The result, and the shortest time of ``repeat`` runs, in seconds."""
    times: list[float] = []
    result = 0
    for _ in range(repeat):
        started = time.perf_counter()
        result = run(tasks)
        times.append(time.perf_counter() - started)
    return result, min(times)


def main() -> None:
    parser = argparse.ArgumentParser(description="Spawn many tasks and sum their results.")
    parser.add_argument("--tasks", type=int, default=10_000)
    parser.add_argument("--repeat", type=int, default=5, help="report the best of N runs")
    parser.add_argument("--backend", choices=["porto", "asyncio", "uvloop"], default="porto")
    options = parser.parse_args()
    total, elapsed = best_of(options.repeat, runner(options.backend), options.tasks)
    assert total == options.tasks * (options.tasks - 1) // 2, total
    print(f"{options.backend}: {options.tasks} tasks, sum {total}, {elapsed * 1000:.1f} ms")


if __name__ == "__main__":
    main()
