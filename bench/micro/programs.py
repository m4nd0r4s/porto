"""The micro-benchmark programs: the same program for porto and for asyncio on each event loop.

Every program is a pyperf time function, ``(loops) -> seconds``: it starts a runtime, runs
``loops`` operations inside one main task, and returns the time measured *inside* that task, so
runtime start-up and shutdown are not part of the number. The programs follow
docs/spec/50-engineering/53-benchmarks.md §2 and the §11 table of
docs/spec/10-porto-runtime/11-task-model-and-trap-protocol.md.

asyncio backends ("loops"):

- ``asyncio``: the default event loop, ``asyncio.new_event_loop()`` (what ``asyncio.run`` uses;
  on macOS a selector loop over kqueue).
- ``uvloop``: ``uvloop.new_event_loop()``.
- ``asyncio-nopoll``: the default selector loop over a selector that never makes a system call
  (:class:`NoPollSelector`). Informative only: porto M0 has no I/O polling, while every asyncio
  loop iteration polls the OS for I/O; this variant shows how much of the gap that poll is.
- ``uvloop-readyfd``: uvloop with a pipe that is always readable registered (with a no-op
  reader), so its per-iteration ``kevent`` returns an event at once. Informative only: on macOS
  a zero-timeout ``kevent`` that finds nothing costs ~12 µs on the reference machine, against
  ~0.3 µs when an event is ready (the runner records this probe); this variant takes that
  kernel path out for uvloop, at the price of one extra callback per iteration.

Task-start variants: ``eager`` sets ``asyncio.eager_task_factory`` (3.12+): a new task runs
synchronously inside ``create_task`` until its first suspension. Informative only: it changes
the semantics (the child runs before the spawner continues) and porto's spawn does not do it.
"""

from __future__ import annotations

import asyncio
import os
import selectors
import sys
import time
from collections.abc import Callable, Coroutine, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

import porto

if TYPE_CHECKING:
    from _typeshed import FileDescriptorLike

#: The batch size of the "spawn+join 100k trivial tasks" row (53 §2).
SPAWN_JOIN_TASKS = 100_000

Role = Literal["porto", "baseline", "informative"]
TimeFunction = Callable[[int], float]
Main = Callable[[int], Coroutine[Any, Any, float]]


# The no-poll selector ------------------------------------------------------------------------


class NoPollSelector(selectors.BaseSelector):
    """A selector that keeps its registrations but never asks the OS about them.

    ``select(0)``, which is what the loop calls while callbacks are ready, returns at once. A
    positive timeout (a pending timer) sleeps. ``select(None)`` would wait for I/O forever: the
    benchmarks never get there, so it raises instead of hanging.
    """

    def __init__(self) -> None:
        self._keys: dict[FileDescriptorLike, selectors.SelectorKey] = {}

    def register(
        self, fileobj: FileDescriptorLike, events: int, data: Any = None
    ) -> selectors.SelectorKey:
        key = selectors.SelectorKey(fileobj, _fd(fileobj), events, data)
        self._keys[key.fd] = key
        return key

    def unregister(self, fileobj: FileDescriptorLike) -> selectors.SelectorKey:
        return self._keys.pop(_fd(fileobj))

    def select(self, timeout: float | None = None) -> list[tuple[selectors.SelectorKey, int]]:
        if timeout is None:
            raise RuntimeError(
                "NoPollSelector cannot wait for I/O: run only programs that never block on I/O "
                "under the asyncio-nopoll loop"
            )
        if timeout > 0:
            time.sleep(timeout)
        return []

    def get_map(self) -> Mapping[FileDescriptorLike, selectors.SelectorKey]:
        return self._keys


def _fd(fileobj: FileDescriptorLike) -> int:
    return fileobj if isinstance(fileobj, int) else fileobj.fileno()


# Runners -------------------------------------------------------------------------------------


def _uvloop_factory() -> asyncio.AbstractEventLoop:
    import uvloop

    return uvloop.new_event_loop()


def _nopoll_factory() -> asyncio.AbstractEventLoop:
    return asyncio.SelectorEventLoop(NoPollSelector())


LOOP_FACTORIES: dict[str, Callable[[], asyncio.AbstractEventLoop]] = {
    "asyncio": asyncio.new_event_loop,
    "uvloop": _uvloop_factory,
    "asyncio-nopoll": _nopoll_factory,
}


def run_porto(main: Main, loops: int) -> float:
    return porto.run(main, loops)


def runasyncio_program(
    loop: str, main: Main, loops: int, *, eager: bool = False, ready_fd: bool = False
) -> float:
    with asyncio.Runner(loop_factory=LOOP_FACTORIES[loop]) as runner:
        event_loop = runner.get_loop()
        if eager:
            if sys.version_info < (3, 12):
                raise RuntimeError("eager_task_factory needs Python 3.12+: skip eager variants")
            event_loop.set_task_factory(asyncio.eager_task_factory)
        if not ready_fd:
            return runner.run(main(loops))
        read_end, write_end = os.pipe()
        try:
            os.write(write_end, b"x")
            event_loop.add_reader(read_end, _ignore)
            try:
                return runner.run(main(loops))
            finally:
                event_loop.remove_reader(read_end)
        finally:
            os.close(read_end)
            os.close(write_end)


def _ignore() -> None:
    """The ready pipe's reader: leaves the byte in the pipe so it stays readable."""


def run_trio(main: Main, loops: int) -> float:
    import trio

    return trio.run(main, loops)


def run_anyio_uvloop(main: Main, loops: int) -> float:
    """anyio on asyncio+uvloop: the cost of anyio's task groups and cancel scopes over the same
    loop as the ``uvloop`` baseline."""
    import anyio

    return anyio.run(main, loops, backend="asyncio", backend_options={"use_uvloop": True})


# 1. checkpoint: one task yielding to the scheduler, per operation ----------------------------


async def porto_checkpoint(loops: int) -> float:
    checkpoint = porto.checkpoint
    started = time.perf_counter()
    for _ in range(loops):
        await checkpoint()
    return time.perf_counter() - started


async def asyncio_checkpoint(loops: int) -> float:
    sleep = asyncio.sleep
    started = time.perf_counter()
    for _ in range(loops):
        await sleep(0)
    return time.perf_counter() - started


# 2. switch: two tasks yielding in turn, per switch (inner_loops=2) ---------------------------


async def porto_yielder(rounds: int) -> None:
    checkpoint = porto.checkpoint
    for _ in range(rounds):
        await checkpoint()


async def porto_switch(loops: int) -> float:
    started = time.perf_counter()
    async with porto.scope() as s:
        s.spawn(porto_yielder, loops)
        s.spawn(porto_yielder, loops)
    return time.perf_counter() - started


async def asyncio_yielder(rounds: int) -> None:
    sleep = asyncio.sleep
    for _ in range(rounds):
        await sleep(0)


async def asyncio_switch(loops: int) -> float:
    started = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        group.create_task(asyncio_yielder(loops))
        group.create_task(asyncio_yielder(loops))
    return time.perf_counter() - started


# 3. spawn_one: spawn `async def f(): return 1` and wait for it, one at a time, per task ------


async def one() -> int:
    return 1


async def porto_spawn_one(loops: int) -> float:
    scope = porto.scope
    started = time.perf_counter()
    for _ in range(loops):
        async with scope() as s:
            s.spawn(one)
    return time.perf_counter() - started


async def asyncio_spawn_one(loops: int) -> float:
    """The §11 comparison row: ``create_task`` + ``await``."""
    create_task = asyncio.get_running_loop().create_task
    started = time.perf_counter()
    for _ in range(loops):
        await create_task(one())
    return time.perf_counter() - started


async def asyncio_spawn_one_taskgroup(loops: int) -> float:
    """The same shape as porto's: a nursery per task."""
    task_group = asyncio.TaskGroup
    started = time.perf_counter()
    for _ in range(loops):
        async with task_group() as group:
            group.create_task(one())
    return time.perf_counter() - started


# 4. spawn_join_100k: spawn 100k trivial tasks into one nursery and join them, per batch ------


async def porto_spawn_join(loops: int) -> float:
    scope = porto.scope
    tasks = range(SPAWN_JOIN_TASKS)
    started = time.perf_counter()
    for _ in range(loops):
        async with scope() as s:
            spawn = s.spawn
            for _ in tasks:
                spawn(one)
    return time.perf_counter() - started


async def asyncio_spawn_join_taskgroup(loops: int) -> float:
    task_group = asyncio.TaskGroup
    tasks = range(SPAWN_JOIN_TASKS)
    started = time.perf_counter()
    for _ in range(loops):
        async with task_group() as group:
            create_task = group.create_task
            for _ in tasks:
                create_task(one())
    return time.perf_counter() - started


async def asyncio_spawn_join_gather(loops: int) -> float:
    gather = asyncio.gather
    tasks = range(SPAWN_JOIN_TASKS)
    started = time.perf_counter()
    for _ in range(loops):
        await gather(*[one() for _ in tasks])
    return time.perf_counter() - started


# 5. ping_pong: two players hitting a ball in turn (53 §2 "yield ping-pong"), per round trip --


class Table:
    """Whose turn it is; a player checks it is theirs before hitting."""

    def __init__(self) -> None:
        self.turn = "ping"
        self.hits = 0


def _out_of_turn(me: str) -> RuntimeError:
    return RuntimeError(f"{me} played out of turn: the scheduler did not alternate the players")


def _check_hits(table: Table, loops: int) -> None:
    if table.hits != 2 * loops:
        raise RuntimeError(f"expected {2 * loops} hits, got {table.hits}: a player stopped early")


async def porto_player(table: Table, me: str, other: str, rounds: int) -> None:
    checkpoint = porto.checkpoint
    for _ in range(rounds):
        if table.turn != me:
            raise _out_of_turn(me)
        table.turn = other
        table.hits += 1
        await checkpoint()


async def porto_ping_pong(loops: int) -> float:
    table = Table()
    started = time.perf_counter()
    async with porto.scope() as s:
        s.spawn(porto_player, table, "ping", "pong", loops)
        s.spawn(porto_player, table, "pong", "ping", loops)
    elapsed = time.perf_counter() - started
    _check_hits(table, loops)
    return elapsed


async def asyncio_player(table: Table, me: str, other: str, rounds: int) -> None:
    sleep = asyncio.sleep
    for _ in range(rounds):
        if table.turn != me:
            raise _out_of_turn(me)
        table.turn = other
        table.hits += 1
        await sleep(0)


async def asyncio_ping_pong(loops: int) -> float:
    table = Table()
    started = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        group.create_task(asyncio_player(table, "ping", "pong", loops))
        group.create_task(asyncio_player(table, "pong", "ping", loops))
    elapsed = time.perf_counter() - started
    _check_hits(table, loops)
    return elapsed


# The registry --------------------------------------------------------------------------------


@dataclass(frozen=True)
class Benchmark:
    """One pyperf benchmark: a program on one backend.

    ``role`` says how the report uses it: ``porto`` is measured, ``baseline`` is what porto is
    gated against, ``informative`` is reported but never gates.
    """

    bench: str
    variant: str
    role: Role
    function: TimeFunction
    inner_loops: int = 1
    min_python: tuple[int, int] = (3, 11)
    #: The modules the program imports besides porto and asyncio (``uvloop``, ``trio``): the
    #: suite skips it on a Python where one is missing.
    requires: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return f"{self.bench}/{self.variant}"


def porto_program(main: Main) -> TimeFunction:
    return lambda loops: run_porto(main, loops)


def asyncio_program(
    loop: str, main: Main, *, eager: bool = False, ready_fd: bool = False
) -> TimeFunction:
    return lambda loops: runasyncio_program(loop, main, loops, eager=eager, ready_fd=ready_fd)


def variants(
    bench: str,
    porto_main: Main | None,
    asyncio_main: Main,
    *,
    inner_loops: int = 1,
    suffix: str = "",
    baseline: bool = True,
    ready_fd: bool = True,
    nopoll: bool = True,
) -> list[Benchmark]:
    """porto, then asyncio and uvloop (baselines, or informative), then asyncio-nopoll and
    uvloop-readyfd (informative; ``ready_fd=False`` and ``nopoll=False`` leave them out:
    ``asyncio-nopoll`` cannot run a program whose loop waits for a wake-up from another
    thread)."""
    role: Role = "baseline" if baseline else "informative"
    found: list[Benchmark] = []
    if porto_main is not None:
        found.append(Benchmark(bench, "porto", "porto", porto_program(porto_main), inner_loops))
    found += [
        Benchmark(
            bench, f"asyncio{suffix}", role, asyncio_program("asyncio", asyncio_main), inner_loops
        ),
        Benchmark(
            bench,
            f"uvloop{suffix}",
            role,
            asyncio_program("uvloop", asyncio_main),
            inner_loops,
            requires=("uvloop",),
        ),
    ]
    if nopoll:
        found.append(
            Benchmark(
                bench,
                f"asyncio-nopoll{suffix}",
                "informative",
                asyncio_program("asyncio-nopoll", asyncio_main),
                inner_loops,
            )
        )
    if ready_fd:
        found.append(
            Benchmark(
                bench,
                f"uvloop-readyfd{suffix}",
                "informative",
                asyncio_program("uvloop", asyncio_main, ready_fd=True),
                inner_loops,
                requires=("uvloop",),
            )
        )
    return found


def eager_variants(bench: str, asyncio_main: Main, suffix: str = "") -> list[Benchmark]:
    return [
        Benchmark(
            bench,
            f"asyncio-eager{suffix}",
            "informative",
            asyncio_program("asyncio", asyncio_main, eager=True),
            min_python=(3, 12),
        ),
        Benchmark(
            bench,
            f"uvloop-eager{suffix}",
            "informative",
            asyncio_program("uvloop", asyncio_main, eager=True),
            min_python=(3, 12),
            requires=("uvloop",),
        ),
    ]


#: Every benchmark, in report order. The gating baseline of a row is its fastest ``baseline``
#: variant on the loop the gate names (asyncio+uvloop).
BENCHMARKS: list[Benchmark] = [
    *variants("checkpoint", porto_checkpoint, asyncio_checkpoint),
    *variants("switch", porto_switch, asyncio_switch, inner_loops=2),
    *variants("spawn_one", porto_spawn_one, asyncio_spawn_one),
    *variants(
        "spawn_one",
        None,
        asyncio_spawn_one_taskgroup,
        suffix="-taskgroup",
        baseline=False,
        ready_fd=False,
    ),
    *eager_variants("spawn_one", asyncio_spawn_one),
    # One batch is a handful of loop iterations, so the poll is noise here: no readyfd variant.
    *variants(
        "spawn_join_100k",
        porto_spawn_join,
        asyncio_spawn_join_taskgroup,
        suffix="-taskgroup",
        ready_fd=False,
    ),
    *variants("spawn_join_100k", None, asyncio_spawn_join_gather, suffix="-gather", ready_fd=False),
    *eager_variants("spawn_join_100k", asyncio_spawn_join_taskgroup, "-taskgroup"),
    *variants("ping_pong", porto_ping_pong, asyncio_ping_pong),
]
