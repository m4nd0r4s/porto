"""Scope and cancellation benchmarks: ``porto.scope`` against ``asyncio.TaskGroup`` (and
``task.cancel()``) doing the same work; trio nurseries and anyio task groups (on asyncio+uvloop)
as informative rows.

12-structured-concurrency-and-cancellation.md. The porto, asyncio, trio and anyio programs open
the same scopes, spawn the same children and cancel or fail them the same way.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

import porto
from programs import Benchmark, Main, run_anyio_uvloop, run_trio, variants

#: Children of the cancel fan-out and fail-fast rows.
CHILDREN = 10_000
#: Depth of the nested-scope row.
DEPTH = 10


class Boom(Exception):
    """What the failing child of the fail-fast row raises."""


class Waiting:
    """How many children reached their wait."""

    def __init__(self) -> None:
        self.count = 0


def _check_one_boom(group: BaseExceptionGroup[BaseException]) -> None:
    if len(group.exceptions) != 1 or not isinstance(group.exceptions[0], Boom):
        raise RuntimeError(f"expected the scope to raise just the failing child's Boom: {group!r}")


def _never_reached() -> RuntimeError:
    return RuntimeError("the scope exited without raising: the failing child's error was lost")


def _informative(
    bench: str, trio_main: Main, anyio_main: Main, inner_loops: int
) -> list[Benchmark]:
    return [
        Benchmark(
            bench,
            "trio",
            "informative",
            lambda loops: run_trio(trio_main, loops),
            inner_loops,
            requires=("trio",),
        ),
        Benchmark(
            bench,
            "anyio-uvloop",
            "informative",
            lambda loops: run_anyio_uvloop(anyio_main, loops),
            inner_loops,
            requires=("anyio", "uvloop"),
        ),
    ]


# scope_enter_exit: an empty scope, per scope ---------------------------------------------------


async def porto_scope_enter_exit(loops: int) -> float:
    scope = porto.scope
    started = time.perf_counter()
    for _ in range(loops):
        async with scope():
            pass
    return time.perf_counter() - started


async def asyncio_scope_enter_exit(loops: int) -> float:
    task_group = asyncio.TaskGroup
    started = time.perf_counter()
    for _ in range(loops):
        async with task_group():
            pass
    return time.perf_counter() - started


async def trio_scope_enter_exit(loops: int) -> float:
    import trio

    open_nursery = trio.open_nursery
    started = time.perf_counter()
    for _ in range(loops):
        async with open_nursery():
            pass
    return time.perf_counter() - started


async def anyio_scope_enter_exit(loops: int) -> float:
    import anyio

    create_task_group = anyio.create_task_group
    started = time.perf_counter()
    for _ in range(loops):
        async with create_task_group():
            pass
    return time.perf_counter() - started


# scope_nested_10: ten empty scopes nested in each other, per nest of ten -----------------------


async def porto_nest(depth: int) -> None:
    async with porto.scope():
        if depth > 1:
            await porto_nest(depth - 1)


async def asyncio_nest(depth: int) -> None:
    async with asyncio.TaskGroup():
        if depth > 1:
            await asyncio_nest(depth - 1)


async def trio_nest(depth: int) -> None:
    import trio

    async with trio.open_nursery():
        if depth > 1:
            await trio_nest(depth - 1)


async def anyio_nest(depth: int) -> None:
    import anyio

    async with anyio.create_task_group():
        if depth > 1:
            await anyio_nest(depth - 1)


def nested(nest: Callable[[int], Awaitable[None]]) -> Main:
    async def main(loops: int) -> float:
        started = time.perf_counter()
        for _ in range(loops):
            await nest(DEPTH)
        return time.perf_counter() - started

    return main


# cancel_fanout_10k: cancel a scope of 10k waiting children, until it exited, per child --------
# Spawning the children and letting them block on an event is outside the time; the time runs
# from the cancel to the scope exit, which needs every child to have unwound (the event is
# never set, so the exit itself shows they were all cancelled). asyncio has no
# cancellable TaskGroup: its twin cancels each task (`task.cancel()`), which a TaskGroup does
# not count as a failure.


async def porto_wait(event: porto.Event, waiting: Waiting) -> None:
    waiting.count += 1
    await event.wait()


async def porto_cancel_fanout(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        event, waiting = porto.Event(), Waiting()
        started = time.perf_counter()
        async with porto.scope() as s:
            for _ in range(CHILDREN):
                s.spawn(porto_wait, event, waiting)
            while waiting.count < CHILDREN:
                await porto.checkpoint()
            started = time.perf_counter()
            s.cancel()
        elapsed += time.perf_counter() - started
    return elapsed


async def asyncio_wait(event: asyncio.Event, waiting: Waiting) -> None:
    waiting.count += 1
    await event.wait()


async def asyncio_cancel_fanout(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        event, waiting = asyncio.Event(), Waiting()
        async with asyncio.TaskGroup() as group:
            tasks = [group.create_task(asyncio_wait(event, waiting)) for _ in range(CHILDREN)]
            while waiting.count < CHILDREN:
                await asyncio.sleep(0)
            started = time.perf_counter()
            for task in tasks:
                task.cancel()
        elapsed += time.perf_counter() - started
    return elapsed


async def trio_cancel_fanout(loops: int) -> float:
    import trio

    async def wait(event: trio.Event, waiting: Waiting) -> None:
        waiting.count += 1
        await event.wait()

    elapsed = 0.0
    for _ in range(loops):
        event, waiting = trio.Event(), Waiting()
        started = time.perf_counter()
        async with trio.open_nursery() as nursery:
            for _ in range(CHILDREN):
                nursery.start_soon(wait, event, waiting)
            while waiting.count < CHILDREN:
                await trio.lowlevel.checkpoint()
            started = time.perf_counter()
            nursery.cancel_scope.cancel()
        elapsed += time.perf_counter() - started
    return elapsed


async def anyio_cancel_fanout(loops: int) -> float:
    import anyio
    import anyio.lowlevel

    async def wait(event: anyio.Event, waiting: Waiting) -> None:
        waiting.count += 1
        await event.wait()

    elapsed = 0.0
    for _ in range(loops):
        event, waiting = anyio.Event(), Waiting()
        started = time.perf_counter()
        async with anyio.create_task_group() as group:
            for _ in range(CHILDREN):
                group.start_soon(wait, event, waiting)
            while waiting.count < CHILDREN:
                await anyio.lowlevel.checkpoint()
            started = time.perf_counter()
            group.cancel_scope.cancel()
        elapsed += time.perf_counter() - started
    return elapsed


# fail_fast_10k: one child raises among 10k waiting ones, until the error group is caught, per
# child. Spawning the 10k and letting them block is outside the time; the time runs from
# spawning the failing child to catching the scope's ExceptionGroup, which needs the failure to
# cancel the 10k and every one of them to unwind.


async def porto_fail() -> None:
    raise Boom


async def porto_fail_fast(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        event, waiting = porto.Event(), Waiting()
        started = 0.0
        try:
            async with porto.scope() as s:
                for _ in range(CHILDREN):
                    s.spawn(porto_wait, event, waiting)
                while waiting.count < CHILDREN:
                    await porto.checkpoint()
                started = time.perf_counter()
                s.spawn(porto_fail)
            raise _never_reached()
        except BaseExceptionGroup as group:
            elapsed += time.perf_counter() - started
            _check_one_boom(group)
    return elapsed


async def asyncio_fail() -> None:
    raise Boom


async def asyncio_fail_fast(loops: int) -> float:
    elapsed = 0.0
    for _ in range(loops):
        event, waiting = asyncio.Event(), Waiting()
        started = 0.0
        try:
            async with asyncio.TaskGroup() as group:
                for _ in range(CHILDREN):
                    group.create_task(asyncio_wait(event, waiting))
                while waiting.count < CHILDREN:
                    await asyncio.sleep(0)
                started = time.perf_counter()
                group.create_task(asyncio_fail())
            raise _never_reached()
        except BaseExceptionGroup as group:
            elapsed += time.perf_counter() - started
            _check_one_boom(group)
    return elapsed


async def trio_fail_fast(loops: int) -> float:
    import trio

    async def wait(event: trio.Event, waiting: Waiting) -> None:
        waiting.count += 1
        await event.wait()

    async def fail() -> None:
        raise Boom

    elapsed = 0.0
    for _ in range(loops):
        event, waiting = trio.Event(), Waiting()
        started = 0.0
        try:
            async with trio.open_nursery() as nursery:
                for _ in range(CHILDREN):
                    nursery.start_soon(wait, event, waiting)
                while waiting.count < CHILDREN:
                    await trio.lowlevel.checkpoint()
                started = time.perf_counter()
                nursery.start_soon(fail)
            raise _never_reached()
        except BaseExceptionGroup as group:
            elapsed += time.perf_counter() - started
            _check_one_boom(group)
    return elapsed


async def anyio_fail_fast(loops: int) -> float:
    import anyio
    import anyio.lowlevel

    async def wait(event: anyio.Event, waiting: Waiting) -> None:
        waiting.count += 1
        await event.wait()

    async def fail() -> None:
        raise Boom

    elapsed = 0.0
    for _ in range(loops):
        event, waiting = anyio.Event(), Waiting()
        started = 0.0
        try:
            async with anyio.create_task_group() as task_group:
                for _ in range(CHILDREN):
                    task_group.start_soon(wait, event, waiting)
                while waiting.count < CHILDREN:
                    await anyio.lowlevel.checkpoint()
                started = time.perf_counter()
                task_group.start_soon(fail)
            raise _never_reached()
        except BaseExceptionGroup as group:
            elapsed += time.perf_counter() - started
            _check_one_boom(group)
    return elapsed


BENCHMARKS: list[Benchmark] = [
    *variants("scope_enter_exit", porto_scope_enter_exit, asyncio_scope_enter_exit),
    *_informative("scope_enter_exit", trio_scope_enter_exit, anyio_scope_enter_exit, 1),
    *variants("scope_nested_10", nested(porto_nest), nested(asyncio_nest)),
    *_informative("scope_nested_10", nested(trio_nest), nested(anyio_nest), 1),
    # A handful of loop iterations per batch: the poll is noise, so no readyfd variants.
    *variants(
        "cancel_fanout_10k",
        porto_cancel_fanout,
        asyncio_cancel_fanout,
        inner_loops=CHILDREN,
        ready_fd=False,
    ),
    *_informative("cancel_fanout_10k", trio_cancel_fanout, anyio_cancel_fanout, CHILDREN),
    *variants(
        "fail_fast_10k",
        porto_fail_fast,
        asyncio_fail_fast,
        inner_loops=CHILDREN,
        ready_fd=False,
    ),
    *_informative("fail_fast_10k", trio_fail_fast, anyio_fail_fast, CHILDREN),
]
