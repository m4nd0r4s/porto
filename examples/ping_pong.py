"""Ping-pong: two tasks pass a ball back and forth, switching with sleep(0) on every hit.

    uv run python examples/ping_pong.py [--rounds 100000] [--backend porto|asyncio|uvloop]

The asyncio and uvloop backends run the same program, for a rough comparison. The official
benchmark is M0's spawn/switch suite.
"""

import argparse
import asyncio
import time
from collections.abc import Callable

import porto


class Table:
    """Whose turn it is; each player checks it is theirs before hitting."""

    def __init__(self) -> None:
        self.turn = "ping"
        self.hits = 0


async def porto_player(table: Table, me: str, other: str, rounds: int) -> None:
    for _ in range(rounds):
        assert table.turn == me, f"{me} played out of turn"
        table.turn = other
        table.hits += 1
        await porto.sleep(0)


async def porto_main(rounds: int) -> int:
    table = Table()
    async with porto.scope() as s:
        s.spawn(porto_player, table, "ping", "pong", rounds)
        s.spawn(porto_player, table, "pong", "ping", rounds)
    return table.hits


async def asyncio_player(table: Table, me: str, other: str, rounds: int) -> None:
    for _ in range(rounds):
        assert table.turn == me, f"{me} played out of turn"
        table.turn = other
        table.hits += 1
        await asyncio.sleep(0)


async def asyncio_main(rounds: int) -> int:
    table = Table()
    async with asyncio.TaskGroup() as group:
        group.create_task(asyncio_player(table, "ping", "pong", rounds))
        group.create_task(asyncio_player(table, "pong", "ping", rounds))
    return table.hits


def runner(backend: str) -> Callable[[int], int]:
    if backend == "porto":
        return lambda rounds: porto.run(porto_main, rounds)
    if backend == "uvloop":
        import uvloop

        return lambda rounds: uvloop.run(asyncio_main(rounds))
    return lambda rounds: asyncio.run(asyncio_main(rounds))


def best_of(repeat: int, run: Callable[[int], int], rounds: int) -> tuple[int, float]:
    """The result, and the shortest time of ``repeat`` runs, in seconds."""
    times: list[float] = []
    result = 0
    for _ in range(repeat):
        started = time.perf_counter()
        result = run(rounds)
        times.append(time.perf_counter() - started)
    return result, min(times)


def main() -> None:
    parser = argparse.ArgumentParser(description="Two tasks alternating with sleep(0).")
    parser.add_argument("--rounds", type=int, default=100_000)
    parser.add_argument("--repeat", type=int, default=5, help="report the best of N runs")
    parser.add_argument("--backend", choices=["porto", "asyncio", "uvloop"], default="porto")
    options = parser.parse_args()
    hits, elapsed = best_of(options.repeat, runner(options.backend), options.rounds)
    assert hits == 2 * options.rounds, hits
    switches_per_second = hits / elapsed
    print(
        f"{options.backend}: {hits} hits in {elapsed * 1000:.1f} ms, "
        f"{switches_per_second / 1e6:.2f} M switches/s"
    )


if __name__ == "__main__":
    main()
