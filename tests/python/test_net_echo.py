"""The TCP echo gate's functional side (53-benchmarks.md §2, 54-roadmap.md M1): a ``serve_tcp``
echo server handles 100 connections that each send and read back 1 KiB messages, with clients
that are porto streams and with clients that know nothing about porto (blocking sockets in OS
threads). The throughput side is ``bench/micro/net_programs.py``; this file keeps the program it
measures correct, on every CI platform that can run sockets.
"""

import socket
import sys
import threading
from collections.abc import Coroutine
from typing import Any

import pytest

import porto
from porto import net

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="registering raw sockets needs the AFD backend (Windows)"
)

LOOPBACK = "127.0.0.1"
CONNECTIONS = 100
MESSAGE_SIZE = 1024
ROUNDS = 20


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return porto.run(coro)


async def echo(stream: net.TcpStream) -> None:
    """The measured handler: read a 1 KiB message, write it back, until the client closes."""
    while True:
        try:
            message = await stream.read_exact(MESSAGE_SIZE)
        except porto.IncompleteReadError as error:
            if error.partial:
                raise AssertionError("the client closed in the middle of a message") from error
            return
        await stream.write(message)


def message(connection: int, round_number: int) -> bytes:
    """1 KiB that differs per connection and round, so a mixed-up echo is noticed."""
    head = b"connection %d round %d:" % (connection, round_number)
    return (head + bytes(MESSAGE_SIZE))[:MESSAGE_SIZE]


def test_a_hundred_porto_clients_get_their_1_kib_messages_back() -> None:
    async def client(port: int, connection: int) -> int:
        async with await net._connect_tcp_numeric(LOOPBACK, port) as stream:  # pyright: ignore[reportPrivateUsage]
            for round_number in range(ROUNDS):
                sent = message(connection, round_number)
                await stream.write(sent)
                assert await stream.read_exact(MESSAGE_SIZE) == sent
        return ROUNDS

    async def main() -> int:
        async with porto.scope() as s:
            port = await s.start(net.serve_tcp, echo, LOOPBACK)
            async with porto.scope() as clients:
                tasks = [clients.spawn(client(port, n)) for n in range(CONNECTIONS)]
            s.cancel()
        return sum(task.result() for task in tasks)

    assert run(main()) == CONNECTIONS * ROUNDS


def test_a_hundred_blocking_socket_clients_in_threads_get_their_messages_back() -> None:
    """Clients that are not porto, the way a foreign program would connect."""
    failures: list[str] = []

    def client(port: int, connection: int, done: porto.Event) -> None:
        try:
            with socket.create_connection((LOOPBACK, port)) as sock:
                for round_number in range(ROUNDS):
                    sent = message(connection, round_number)
                    sock.sendall(sent)
                    received = b""
                    while len(received) < MESSAGE_SIZE:
                        chunk = sock.recv(MESSAGE_SIZE - len(received))
                        if not chunk:
                            failures.append(f"connection {connection} ended early")
                            return
                        received += chunk
                    if received != sent:
                        failures.append(f"connection {connection} round {round_number} differs")
                        return
        except OSError as error:
            failures.append(f"connection {connection}: {error!r}")
        finally:
            done.set()

    async def main() -> None:
        async with porto.scope() as s:
            port = await s.start(net.serve_tcp, echo, LOOPBACK)
            finished = [porto.Event() for _ in range(CONNECTIONS)]
            threads = [
                threading.Thread(target=client, args=(port, n, finished[n]))
                for n in range(CONNECTIONS)
            ]
            for thread in threads:
                thread.start()
            for event in finished:
                await event.wait()
            for thread in threads:
                thread.join()
            s.cancel()

    run(main())
    assert failures == []


def test_messages_that_arrive_in_pieces_or_glued_together_are_echoed_whole() -> None:
    """The server's ``read_exact`` reassembles a message sent in 1-byte pieces, and answers
    two messages written back to back one by one."""

    async def main() -> tuple[bytes, bytes]:
        async with porto.scope() as s:
            port = await s.start(net.serve_tcp, echo, LOOPBACK)
            async with await net._connect_tcp_numeric(LOOPBACK, port) as stream:  # pyright: ignore[reportPrivateUsage]
                first, second = message(1, 1), message(2, 2)
                async with porto.scope() as sending:
                    sending.spawn(_dribble, stream, first)
                    echoed_first = await stream.read_exact(MESSAGE_SIZE)
                await stream.write(second + second)  # glued together
                echoed_second = await stream.read_exact(MESSAGE_SIZE)
                assert await stream.read_exact(MESSAGE_SIZE) == second
                s.cancel()
                assert echoed_first == first
                return echoed_first, echoed_second

    first, second = run(main())
    assert first == message(1, 1)
    assert second == message(2, 2)


async def _dribble(stream: net.TcpStream, data: bytes) -> None:
    for offset in range(64):  # the first 64 bytes one at a time, then the rest at once
        await stream.write(data[offset : offset + 1])
        await porto.sleep(0)
    await stream.write(data[64:])
