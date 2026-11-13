"""The fast path attempts an operation only when it can succeed (13-io-driver.md §5, porto-szb.148):
not after a ``WouldBlock`` while nothing has arrived, and not after a short receive proved the
socket empty; always on a registration the driver has not reported on yet. None of that may lose
a byte, hang a read whose data arrived between two attempts, or break cancel safety (T3).
Registering raw sockets needs the AFD backend on Windows, which is not written yet, so these
tests run on Unix."""

import sys
from collections.abc import Coroutine
from typing import Any

import pytest

import porto
from porto import net

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="registering raw sockets needs the AFD backend (Windows)"
)

LOOPBACK = "127.0.0.1"


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return porto.run(coro)


async def pair() -> tuple[net.TcpStream, net.TcpStream]:
    listener = await net.listen_tcp(LOOPBACK)
    async with listener:
        client = await net.connect_tcp(LOOPBACK, listener.port)
        server = await listener.accept()
    return client, server


def test_data_queued_before_the_first_read_of_a_fresh_stream_is_read_at_once() -> None:
    async def main() -> bytes:
        client, server = await pair()
        async with client, server:
            await client.write(b"queued")
            await porto.sleep(0.05)  # it is in the server's socket, whatever the driver saw
            with porto.move_on_after(10) as deadline:
                data = await server.read()
            assert not deadline.cancelled_caught
            return data

    assert run(main()) == b"queued"


def test_a_request_response_loop_never_loses_or_hangs_on_a_byte() -> None:
    rounds = 2000

    async def echo(stream: net.TcpStream) -> None:
        while chunk := await stream.read(4096):
            await stream.write(chunk)

    async def main() -> int:
        client, server = await pair()
        total = 0
        async with client, server, porto.scope() as s:
            s.spawn(echo, server)
            with porto.move_on_after(60) as deadline:
                for number in range(rounds):
                    message = b"%06d" % number * 20
                    await client.write(message)
                    assert await client.read_exact(len(message)) == message
                    total += 1
            assert not deadline.cancelled_caught
            await client.shutdown_write()
            s.cancel()
        return total

    assert run(main()) == rounds


def test_data_that_arrives_right_after_a_short_read_wakes_the_next_read() -> None:
    async def main() -> list[bytes]:
        client, server = await pair()
        seen: list[bytes] = []
        async with client, server:
            for number in range(200):
                await client.write(b"a%d" % number)
                first = await server.read()  # short: the socket is empty after it
                await client.write(b"b%d" % number)  # arrives while no read waits
                with porto.move_on_after(10) as deadline:
                    second = await server.read()
                assert not deadline.cancelled_caught, "the second read hung"
                seen.append(first + second)
        return seen

    seen = run(main())
    assert len(seen) == 200
    assert all(item.startswith(b"a") and b"b" in item for item in seen)


def test_a_cancelled_read_after_a_short_read_loses_nothing() -> None:
    async def main() -> bytes:
        client, server = await pair()
        async with client, server:
            await client.write(b"one")
            assert await server.read() == b"one"  # short read: readiness forgotten
            with porto.move_on_after(0.05):
                await server.read()  # waits, then is cancelled (T3)
            await client.write(b"two")
            with porto.move_on_after(10) as deadline:
                data = await server.read()
            assert not deadline.cancelled_caught
            return data

    assert run(main()) == b"two"
