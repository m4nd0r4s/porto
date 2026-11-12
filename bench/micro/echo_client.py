"""The load generator of the TCP echo benchmark (``net_programs.py``): a script, run in its own
processes so that it does not share the GIL with the server it measures.

    python bench/micro/echo_client.py PORT --connections 100 --rounds 1000

It opens ``--connections`` connections to 127.0.0.1:PORT, split over ``--workers`` processes (each
runs one uvloop loop, or the default asyncio loop without uvloop, with ``asyncio.Protocol``
clients: the cheapest way Python has to drive a socket, so that the generator is not what limits
the server). Every connection then does ``--rounds`` round trips: send ``--size`` bytes, wait for
the same bytes back. The workers connect first and start together, so the time covers
only the round trips. It prints one JSON line: ``{"seconds": ..., "round_trips": ...}``, the
wall-clock time from the first worker's start to the last worker's end.

The same generator drives every server of the benchmark (porto's, asyncio streams, ...), so the
ratio of their times is the ratio of the servers.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing
import sys
import time
from multiprocessing.synchronize import Barrier
from typing import Any, cast

LOOPBACK = "127.0.0.1"


class Client(asyncio.Protocol):
    """One connection: ``rounds`` times, send the payload and wait for all of it back."""

    def __init__(self, payload: bytes, rounds: int, done: asyncio.Future[None]) -> None:
        self._payload = payload
        self._remaining = rounds
        self._done = done
        self._partial = b""
        self._transport: asyncio.Transport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        # uvloop's transports are not asyncio.Transport subclasses, but have its methods.
        self._transport = cast(asyncio.Transport, transport)

    def start(self) -> None:
        assert self._transport is not None
        self._transport.write(self._payload)

    def data_received(self, data: bytes) -> None:
        if self._partial:
            data = self._partial + data
        if len(data) < len(self._payload):
            self._partial = data
            return
        if data != self._payload:
            self._fail(f"the echo differs from the message ({len(data)} bytes back)")
            return
        self._partial = b""
        self._remaining -= 1
        assert self._transport is not None
        if self._remaining:
            self._transport.write(self._payload)
        else:
            self._done.set_result(None)

    def connection_lost(self, exc: Exception | None) -> None:
        if not self._done.done():
            self._fail(f"the server closed the connection early ({exc!r})")

    def _fail(self, message: str) -> None:
        if not self._done.done():
            self._done.set_exception(RuntimeError(message))


def new_loop() -> asyncio.AbstractEventLoop:
    try:
        import uvloop
    except ImportError:
        return asyncio.new_event_loop()
    return uvloop.new_event_loop()


async def drive(
    port: int, connections: int, rounds: int, size: int, barrier: Barrier
) -> tuple[float, float]:
    loop = asyncio.get_running_loop()
    payload = bytes(range(256)) * (size // 256) + bytes(size % 256)
    clients: list[Client] = []
    futures: list[asyncio.Future[None]] = []
    for _ in range(connections):
        done: asyncio.Future[None] = loop.create_future()
        _, client = await loop.create_connection(
            lambda done=done: Client(payload, rounds, done), LOOPBACK, port
        )
        clients.append(client)
        futures.append(done)
    # Everyone is connected: start together (a blocking wait is fine, the loop has no other work).
    barrier.wait()
    started = time.monotonic()
    for client in clients:
        client.start()
    await asyncio.gather(*futures)
    return started, time.monotonic()


def worker(
    port: int,
    connections: int,
    rounds: int,
    size: int,
    barrier: Barrier,
    results: Any,
) -> None:
    try:
        with asyncio.Runner(loop_factory=new_loop) as runner:
            results.put(runner.run(drive(port, connections, rounds, size, barrier)))
    except BaseException as error:  # reported to the parent: the benchmark must not hang
        barrier.abort()
        results.put(f"{type(error).__name__}: {error}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("port", type=int)
    parser.add_argument("--connections", type=int, default=100)
    parser.add_argument("--rounds", type=int, default=1000)
    parser.add_argument("--size", type=int, default=1024)
    parser.add_argument("--workers", type=int, default=4)
    options = parser.parse_args()
    workers = max(1, min(options.workers, options.connections))
    # `spawn` everywhere: a fork of the benchmark's parent would copy its runtime.
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(workers)
    results = context.Queue()
    shares = [
        options.connections // workers + (n < options.connections % workers) for n in range(workers)
    ]
    processes = [
        context.Process(
            target=worker,
            args=(options.port, share, options.rounds, options.size, barrier, results),
        )
        for share in shares
    ]
    for process in processes:
        process.start()
    outcomes = [results.get() for _ in processes]
    for process in processes:
        process.join()
    failures = [outcome for outcome in outcomes if isinstance(outcome, str)]
    if failures:
        sys.exit("echo_client: " + "; ".join(failures))
    spans = [outcome for outcome in outcomes if not isinstance(outcome, str)]
    seconds = max(end for _, end in spans) - min(start for start, _ in spans)
    round_trips = options.connections * options.rounds
    print(json.dumps({"seconds": seconds, "round_trips": round_trips}))


if __name__ == "__main__":
    main()
