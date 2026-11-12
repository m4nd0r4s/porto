"""The TCP echo gate (53-benchmarks.md §2: 1 KiB, 100 connections, at least 1.5x uvloop streams).

One op is one 1 KiB round trip on one of 100 persistent connections: the clients send a message
and wait for all of it back, ``loops`` times each, so a sample is ``100 * loops`` round trips.

**The server is the thing measured, in this process; the clients are not.** The load comes from
``echo_client.py``, run as a subprocess (4 worker processes, uvloop, ``asyncio.Protocol``
clients), the same one for every server, started once the server listens and timed by itself,
from the moment all connections are open to the moment the last echo arrived. So the server has
its own GIL and its own core, nothing but the server's speed limits the number as long as the
generator is faster than the server (it is, by a wide margin: see the results), and the ratio of
two servers' times is the ratio of their throughputs. The alternatives were worse: porto clients
(``porto.net._connect_tcp_numeric``) in the same run would make the number half client; asyncio
clients on the same loop would charge uvloop for its own client side, and no client at all in the
server's process, in threads, would fight it for the GIL. Clients that are *not* in the server's
run also keep the comparison honest where porto has no public ``connect_tcp`` yet (porto-szb.69).

The servers, all echoing 1 KiB messages with the idiom of their library:

- ``porto``: ``porto.net.serve_tcp`` with ``read_exact(1024)`` + ``write`` (a scope per
  connection);
- ``asyncio``, ``uvloop`` (the gate's baseline): ``asyncio.start_server`` with
  ``readexactly(1024)`` + ``write`` + ``drain`` (streams);
- ``uvloop-protocol`` (informative, never gates): an ``asyncio.Protocol`` that writes the bytes
  from ``data_received``, uvloop's ceiling for this work.

On macOS an empty zero-timeout ``kevent`` costs ~12 µs (programs.py), but a loaded echo server
rarely polls an empty queue, so this row is not the poll-bound one the switch rows are; the
poll-free and ``asyncio-nopoll`` variants do not exist here (a socket needs real readiness).
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import threading
from pathlib import Path
from typing import cast

import porto
from porto import net
from programs import Benchmark, asyncio_program, variants

CONNECTIONS = 100
MESSAGE_SIZE = 1024
LOOPBACK = "127.0.0.1"
CLIENT_WORKERS = 4
CLIENT = Path(__file__).with_name("echo_client.py")


def run_clients(port: int, rounds: int) -> float:
    """Blocking: the load generator's own measurement of ``rounds`` round trips on each of the
    connections, in seconds. Run it off the server's lane (a thread)."""
    completed = subprocess.run(
        [
            sys.executable,
            str(CLIENT),
            str(port),
            "--connections",
            str(CONNECTIONS),
            "--rounds",
            str(rounds),
            "--size",
            str(MESSAGE_SIZE),
            "--workers",
            str(CLIENT_WORKERS),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"the echo load generator failed: {completed.stderr.strip()}")
    result = json.loads(completed.stdout)
    if result["round_trips"] != CONNECTIONS * rounds:
        raise RuntimeError(f"the load generator reports {result['round_trips']} round trips")
    return float(result["seconds"])


# porto ---------------------------------------------------------------------------------------


async def porto_echo(stream: net.TcpStream) -> None:
    read_exact, write = stream.read_exact, stream.write
    try:
        while True:
            await write(await read_exact(MESSAGE_SIZE))
    except porto.IncompleteReadError:
        pass  # the client closed


async def porto_tcp_echo(rounds: int) -> float:
    finished = porto.Event()
    outcome: list[float | BaseException] = []

    def generate(port: int) -> None:
        try:
            outcome.append(run_clients(port, rounds))
        except BaseException as error:
            outcome.append(error)
        finally:
            finished.set()  # safe from any thread

    async with porto.scope() as s:
        port = await s.start(net.serve_tcp, porto_echo, LOOPBACK)
        threading.Thread(target=generate, args=(port,), daemon=True).start()
        await finished.wait()
        s.cancel()  # serve_tcp runs forever
    [seconds] = outcome
    if isinstance(seconds, BaseException):
        raise seconds
    return seconds


# asyncio streams (asyncio and uvloop) ---------------------------------------------------------


async def streams_echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    readexactly, write, drain = reader.readexactly, writer.write, writer.drain
    try:
        while True:
            write(await readexactly(MESSAGE_SIZE))
            await drain()
    except asyncio.IncompleteReadError:
        pass  # the client closed
    finally:
        writer.close()


async def asyncio_tcp_echo(rounds: int) -> float:
    server = await asyncio.start_server(streams_echo, LOOPBACK, 0, backlog=1024)
    port: int = server.sockets[0].getsockname()[1]
    try:
        return await asyncio.get_running_loop().run_in_executor(None, run_clients, port, rounds)
    finally:
        server.close()


# uvloop's ceiling: a Protocol, no streams ---------------------------------------------------------


class EchoProtocol(asyncio.Protocol):
    def __init__(self) -> None:
        self._transport: asyncio.Transport | None = None
        self._partial = b""

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        # uvloop's transports are not asyncio.Transport subclasses, but have its methods.
        self._transport = cast(asyncio.Transport, transport)

    def data_received(self, data: bytes) -> None:
        # The streams programs echo whole 1 KiB messages; so does this, to be the same work.
        if self._partial:
            data = self._partial + data
        whole = len(data) - len(data) % MESSAGE_SIZE
        self._partial = data[whole:]
        if whole and self._transport is not None:
            self._transport.write(data[:whole])


async def protocol_tcp_echo(rounds: int) -> float:
    loop = asyncio.get_running_loop()
    server = await loop.create_server(EchoProtocol, LOOPBACK, 0, backlog=1024)
    port: int = server.sockets[0].getsockname()[1]
    try:
        return await loop.run_in_executor(None, run_clients, port, rounds)
    finally:
        server.close()


BENCHMARKS: list[Benchmark] = [
    *variants(
        "tcp_echo",
        porto_tcp_echo,
        asyncio_tcp_echo,
        inner_loops=CONNECTIONS,
        ready_fd=False,
        nopoll=False,
    ),
    Benchmark(
        "tcp_echo",
        "uvloop-protocol",
        "informative",
        asyncio_program("uvloop", protocol_tcp_echo),
        CONNECTIONS,
        requires=("uvloop",),
    ),
]
