"""The buffered reads of ``porto.net.TcpStream`` (17-networking-tls-dns.md §1.1): ``read_exact``,
``read_until``, ``readline`` and ``read_to_end``, and how the plain reads take buffered bytes
first. Over loopback, like ``test_net_tcp.py``; I/O needs the Unix backends.
"""

import socket
import sys
import threading
import time
from collections.abc import Coroutine
from typing import Any

import pytest

import porto
from porto import _core, net

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="registering raw sockets needs the AFD backend (Windows)"
)

LOOPBACK = "127.0.0.1"


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return porto.run(coro)


async def connected() -> tuple[net.TcpStream, net.TcpStream, net.TcpListener]:
    """A client, the stream the listener accepted for it, and the listener."""
    listener = await net.listen_tcp(LOOPBACK)
    client = await net.connect_tcp(LOOPBACK, listener.port)
    server = await listener.accept()
    return client, server, listener


# read_exact


def test_read_exact_returns_exactly_n_bytes_and_leaves_the_rest_for_the_next_read() -> None:
    async def main() -> list[bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"0123456789")
            return [
                await server.read_exact(4),
                await server.read_exact(0),
                await server.read_exact(6),
            ]

    assert run(main()) == [b"0123", b"", b"456789"]


def test_read_exact_collects_bytes_that_arrive_in_pieces() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener, porto.scope() as s:
            reading = s.spawn(server.read_exact, 12)
            for piece in (b"abc", b"d", b"efgh", b"ijkl"):
                await client.write(piece)
                await porto.sleep(0.002)
            return await reading

    assert run(main()) == b"abcdefghijkl"


def test_read_exact_of_a_megabyte_grows_the_buffer_as_the_bytes_come() -> None:
    size = 1 << 20

    async def main() -> tuple[int, bytes]:
        client, server, listener = await connected()
        async with client, server, listener, porto.scope() as s:
            s.spawn(client.write, bytes(range(256)) * (size // 256))
            data = await server.read_exact(size)
            return len(data), data[-256:]

    assert run(main()) == (1 << 20, bytes(range(256)))


def test_the_end_of_the_stream_in_read_exact_raises_incomplete_read_with_the_partial() -> None:
    async def main() -> porto.IncompleteReadError:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"short")
            await client.aclose()
            with pytest.raises(porto.IncompleteReadError) as caught:
                await server.read_exact(10)
            # The partial bytes were consumed: the stream is at its end.
            assert await server.read() == b""
            return caught.value

    error = run(main())
    assert error.partial == b"short"
    assert error.expected == 10
    assert isinstance(error, EOFError)
    assert isinstance(error, porto.PortoError)
    assert "ended after 5 bytes" in str(error)


def test_read_exact_validates_its_argument() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            with pytest.raises(ValueError, match=r"read_exact\(\)'s n must be an int in 0\.\."):
                server.read_exact(-1)
            with pytest.raises(ValueError, match="n must be an int"):
                server.read_exact(2**70)
            with pytest.raises(TypeError):
                server.read_exact("3")  # pyright: ignore[reportArgumentType]

    run(main())


# read_until and readline


def test_read_until_includes_the_separator_and_returns_each_message_in_order() -> None:
    async def main() -> list[bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"one\r\ntwo\r\n\r\nthree\r\nrest")
            messages = [await server.read_until(b"\r\n") for _ in range(4)]
            # What follows the last separator is buffered, not lost.
            await client.aclose()
            return [*messages, await server.read_to_end(limit=10)]

    assert run(main()) == [b"one\r\n", b"two\r\n", b"\r\n", b"three\r\n", b"rest"]


def test_read_until_finds_a_separator_split_over_many_small_segments() -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await connected()
        async with client, server, listener, porto.scope() as s:
            reading = s.spawn(server.read_until, b"\r\n\r\n")
            for piece in (b"GET / HT", b"TP/1.1", b"\r", b"\n", b"\r", b"\nbody"):
                await client.write(piece)
                await porto.sleep(0.003)
            head = await reading
            return head, await server.read_exact(4)

    assert run(main()) == (b"GET / HTTP/1.1\r\n\r\n", b"body")


def test_read_until_takes_any_bytes_like_separator() -> None:
    async def main() -> list[bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"a;b;c;")
            return [
                await server.read_until(b";"),
                await server.read_until(bytearray(b";")),
                await server.read_until(memoryview(b";")),
            ]

    assert run(main()) == [b"a;", b"b;", b"c;"]


def test_read_until_without_the_separator_within_the_limit_raises_and_keeps_the_bytes() -> None:
    async def main() -> tuple[str, bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"x" * 100)
            with pytest.raises(porto.LimitOverrunError) as caught:
                await server.read_until(b"\n", limit=10)
            # Nothing was consumed: a plain read gets them all.
            return str(caught.value), await server.read_exact(100)

    message, data = run(main())
    assert data == b"x" * 100
    assert "read_until() found no separator in the first 10 bytes" in message
    assert "larger limit" in message


def test_a_separator_that_ends_past_the_limit_is_an_overrun_not_a_short_line() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"abcdefgh\nnext\n")
            with pytest.raises(porto.LimitOverrunError):
                await server.read_until(b"\n", limit=8)  # the newline is the 9th byte
            return await server.read_until(b"\n", limit=9)

    assert run(main()) == b"abcdefgh\n"


def test_the_end_of_the_stream_before_the_separator_raises_incomplete_read() -> None:
    async def main() -> porto.IncompleteReadError:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"no newline")
            await client.aclose()
            with pytest.raises(porto.IncompleteReadError) as caught:
                await server.read_until(b"\n")
            return caught.value

    error = run(main())
    assert error.partial == b"no newline"
    assert error.expected is None


def test_readline_reads_lines_and_ends_with_incomplete_read_at_the_end_of_the_stream() -> None:
    async def main() -> tuple[list[bytes], bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"first\nsecond\nlast without newline")
            await client.aclose()
            lines: list[bytes] = []
            while True:
                try:
                    lines.append(await server.readline())
                except porto.IncompleteReadError as error:
                    return lines, error.partial

    assert run(main()) == ([b"first\n", b"second\n"], b"last without newline")


def test_readline_at_the_end_of_an_empty_stream_raises_incomplete_read_with_nothing() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.aclose()
            with pytest.raises(porto.IncompleteReadError) as caught:
                await server.readline()
            return caught.value.partial

    assert run(main()) == b""


def test_readline_has_a_default_limit_of_64_kib_and_takes_another() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener, porto.scope() as s:
            s.spawn(client.write, b"y" * 70_000)
            with pytest.raises(porto.LimitOverrunError, match=r"readline\(\) found no separator"):
                await server.readline()
            with pytest.raises(porto.LimitOverrunError, match="first 5 bytes"):
                await server.readline(5)
            with pytest.raises(porto.LimitOverrunError):
                await server.readline(limit=70_000)

    run(main())


def test_read_until_and_readline_validate_their_arguments() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            with pytest.raises(ValueError, match="sep is empty"):
                server.read_until(b"")
            with pytest.raises(TypeError):
                server.read_until("\n")  # pyright: ignore[reportArgumentType]
            with pytest.raises(ValueError, match=r"limit must be an int in 2\.\..*2 bytes"):
                server.read_until(b"\r\n", limit=1)
            with pytest.raises(ValueError, match=r"readline\(\)'s limit must be an int in 1\.\."):
                server.readline(0)
            with pytest.raises(ValueError, match="limit must be an int"):
                server.readline(limit=2**70)
            with pytest.raises(TypeError):
                server.read_until(b"\n", 10)  # pyright: ignore[reportCallIssue]

    run(main())


# read_to_end


def test_read_to_end_returns_everything_until_the_end_of_the_stream() -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"all of it")
            await client.aclose()
            everything = await server.read_to_end(limit=100)
            return everything, await server.read_to_end(limit=0)

    assert run(main()) == (b"all of it", b"")


def test_read_to_end_takes_the_buffered_bytes_first() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"header\nbody")
            assert await server.readline() == b"header\n"
            await client.aclose()
            return await server.read_to_end(limit=100)

    assert run(main()) == b"body"


def test_read_to_end_over_its_limit_raises_and_keeps_the_bytes() -> None:
    async def main() -> tuple[str, bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"z" * 50)
            await client.aclose()
            with pytest.raises(porto.LimitOverrunError) as caught:
                await server.read_to_end(limit=20)
            return str(caught.value), await server.read_to_end(limit=50)

    message, data = run(main())
    assert data == b"z" * 50
    assert "read_to_end() reached its limit of 20 bytes" in message


def test_read_to_end_needs_a_limit_and_validates_it() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            with pytest.raises(TypeError, match="limit"):
                server.read_to_end()  # pyright: ignore[reportCallIssue]
            with pytest.raises(
                ValueError, match=r"read_to_end\(\)'s limit must be an int in 0\.\."
            ):
                server.read_to_end(limit=-1)

    run(main())


# Cancellation loses nothing (T3)


def test_cancelling_read_until_midway_and_retrying_returns_all_the_bytes() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"par")
            async with porto.move_on_after(0.02):
                await server.read_until(b"\n")  # received "par", cancelled waiting for the rest
            await client.write(b"tial\nnext")
            return await server.read_until(b"\n")

    assert run(main()) == b"partial\n"


def test_cancelling_read_exact_midway_and_retrying_returns_all_the_bytes() -> None:
    async def main() -> list[bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"abc")
            async with porto.move_on_after(0.02):
                await server.read_exact(8)
            await client.write(b"defghij")
            return [await server.read_exact(8), await server.read_exact(2)]

    assert run(main()) == [b"abcdefgh", b"ij"]


def test_cancelling_read_to_end_midway_and_retrying_returns_all_the_bytes() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"one ")
            async with porto.move_on_after(0.02):
                await server.read_to_end(limit=100)
            await client.write(b"two")
            await client.aclose()
            return await server.read_to_end(limit=100)

    assert run(main()) == b"one two"


def test_a_cancelled_buffered_read_leaves_the_stream_usable_for_every_kind_of_read() -> None:
    async def main() -> list[bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"buffered")
            async with porto.move_on_after(0.02):
                await server.read_exact(100)
            return [await server.read(3), await server.read_exact(5)]

    assert run(main()) == [b"buf", b"fered"]


# Plain reads take buffered bytes first


def test_read_returns_buffered_bytes_without_waiting_for_the_socket() -> None:
    async def main() -> list[bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"line\nleftover")
            assert await server.readline() == b"line\n"
            # "leftover" is buffered; the socket has nothing more, so this must not wait.
            async with porto.move_on_after(1.0):  # a wait here would be cancelled: the asserts fail
                first = await server.read(4)
                second = await server.read()
            return [first, second]

    assert run(main()) == [b"left", b"over"]


def test_read_buffer_and_read_into_take_buffered_bytes_first() -> None:
    async def main() -> tuple[bytes, int, bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"head\n0123456789")
            assert await server.readline() == b"head\n"
            async with porto.move_on_after(1.0):  # a wait here would be cancelled: the asserts fail
                buffer = await server.read_buffer(4)
                target = bytearray(16)
                count = await server.read_into(target)
            return bytes(buffer), count, bytes(target[:count])  # pyright: ignore[reportArgumentType]

    assert run(main()) == (b"0123", 6, b"456789")


def test_after_the_buffer_is_drained_reads_go_to_the_socket_again() -> None:
    async def main() -> list[bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"a\nb")
            out = [await server.readline(), await server.read()]
            await client.write(b"c")
            out.append(await server.read())
            await client.aclose()
            out.append(await server.read())
            return out

    assert run(main()) == [b"a\n", b"b", b"c", b""]


# One reader at a time, closing


@pytest.mark.parametrize(
    "start",
    [
        lambda s: s.read_exact(5),
        lambda s: s.read_until(b"\n"),
        lambda s: s.readline(),
        lambda s: s.read_to_end(limit=10),
        lambda s: s.read(5),
    ],
    ids=["read_exact", "read_until", "readline", "read_to_end", "read"],
)
def test_a_second_reader_is_refused_while_a_buffered_read_waits(start: Any) -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener, porto.scope() as s:
            first = s.spawn(server.read_exact, 3)
            await porto.sleep(0.01)
            with pytest.raises(porto.BusyResourceError, match="reading from"):
                start(server)
            await client.write(b"abc")
            assert await first == b"abc"
            # The claim is free again.
            await client.write(b"x\ny\n12345")
            await client.aclose()
            await server.readline()

    run(main())


def test_every_buffered_read_is_refused_while_a_plain_read_waits() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener, porto.scope() as s:
            first = s.spawn(server.read, 8)
            await porto.sleep(0.01)
            for call in (
                lambda: server.read_exact(1),
                lambda: server.read_until(b"\n"),
                lambda: server.readline(),
                lambda: server.read_to_end(limit=1),
            ):
                with pytest.raises(porto.BusyResourceError, match="one reader and one writer"):
                    call()
            await client.write(b"go")
            await first

    run(main())


def test_closing_the_stream_ends_a_waiting_buffered_read_and_every_later_one() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, listener:
            await client.write(b"line\nbuffered")
            await server.readline()  # "buffered" is now in the receive buffer

            async def closer() -> None:
                await porto.sleep(0.01)
                await server.aclose()

            async with porto.scope() as s:
                s.spawn(closer())
                with pytest.raises(porto.ClosedResourceError):
                    await server.read_exact(100)
            for call in (
                lambda: server.read_exact(1),
                lambda: server.readline(),
                lambda: server.read(1),
                lambda: server.read_to_end(limit=10),
            ):
                with pytest.raises(porto.ClosedResourceError):
                    await call()

    run(main())


def test_a_peer_reset_is_an_os_error_not_an_incomplete_read() -> None:
    import socket
    import struct

    async def main() -> None:
        listener = await net.listen_tcp(LOOPBACK)
        raw = socket.create_connection((LOOPBACK, listener.port))
        server = await listener.accept()
        async with server, listener:
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            raw.close()  # an RST, not a FIN
            with pytest.raises(ConnectionResetError):
                await server.read_exact(4)

    run(main())


# Large reads cost linear time and do not starve the lane


MEBIBYTE = 1 << 20


def sending_thread(port: int, size: int, tail: bytes = b"") -> threading.Thread:
    """A peer that is not porto: sends `size` bytes as fast as it can, then `tail`, and closes."""

    def send() -> None:
        with socket.create_connection((LOOPBACK, port)) as sock:
            chunk = b"x" * MEBIBYTE
            for _ in range(size // MEBIBYTE):
                sock.sendall(chunk)
            sock.sendall(b"x" * (size % MEBIBYTE) + tail)

    thread = threading.Thread(target=send, daemon=True)
    thread.start()
    return thread


async def accept_from_sender(size: int, tail: bytes = b"") -> tuple[net.TcpStream, net.TcpListener]:
    listener = await net.listen_tcp(LOOPBACK)
    sending_thread(listener.port, size, tail)
    return await listener.accept(), listener


@pytest.mark.parametrize("call", ["read_to_end", "read_until", "read_exact"])
def test_reading_64_mib_takes_linear_time(call: str) -> None:
    size = 64 * MEBIBYTE

    async def main() -> tuple[int, float]:
        stream, listener = await accept_from_sender(size - 1, b"\n")
        async with stream, listener:
            started = time.perf_counter()
            if call == "read_to_end":
                data = await stream.read_to_end(limit=size)
            elif call == "read_until":
                data = await stream.read_until(b"\n", limit=size)
            else:
                data = await stream.read_exact(size)
            return len(data), time.perf_counter() - started

    length, seconds = run(main())
    assert length == size
    # Quadratic growth took 4 to 40 seconds; this is generous for a slow CI machine.
    assert seconds < 5.0, f"{call} of 64 MiB took {seconds:.1f} s"


def test_a_large_read_from_a_fast_peer_lets_other_tasks_run() -> None:
    size = 64 * MEBIBYTE

    async def main() -> int:
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                await porto.checkpoint()
                ticks += 1

        stream, listener = await accept_from_sender(size)
        async with stream, listener, porto.scope() as s:
            s.spawn(ticker())
            await stream.read_to_end(limit=size)
            s.cancel()
        return ticks

    # The lane is yielded every MiB received: dozens of turns for the ticker (without it, none).
    assert run(main()) >= 20


# An idle stream holds no receive storage


def test_reads_that_waited_and_gave_up_leave_no_storage_with_the_stream() -> None:
    async def main() -> tuple[int, int]:
        listener = await net.listen_tcp(LOOPBACK)
        async with listener:
            clients = [await net.connect_tcp(LOOPBACK, listener.port) for _ in range(50)]
            servers = [await listener.accept() for _ in range(50)]
            before = sum(row[3] for row in _core._buffer_pool_stats())  # pyright: ignore[reportPrivateUsage]

            async def give_up(stream: net.TcpStream, size: int) -> None:
                async with porto.move_on_after(0.02):
                    if size:
                        await stream.read_exact(size)
                    else:
                        await stream.readline()

            async with porto.scope() as s:
                for number, stream in enumerate(servers):
                    s.spawn(give_up(stream, 0 if number % 2 else 1 << 20))
            after = sum(row[3] for row in _core._buffer_pool_stats())  # pyright: ignore[reportPrivateUsage]
            # Each stream can still read: nothing was lost, nothing is held.
            await clients[0].write(b"hi\n")
            assert await servers[0].readline() == b"hi\n"
            for stream in [*clients, *servers]:
                await stream.aclose()
            return before, after

    before, after = run(main())
    # 100 reads that waited, 50 of them for 1 MiB: if each kept its storage, each would have
    # taken one from the pool (a miss); returned at once, one buffer is reused by all.
    assert after - before <= 4, f"{after - before} pool misses: idle streams hold storage"


# LimitOverrunError.consumed


def test_limit_overrun_says_how_many_bytes_are_buffered() -> None:
    async def main() -> list[tuple[int, bytes]]:
        client, server, listener = await connected()
        async with client, server, listener:
            found: list[tuple[int, bytes]] = []
            await client.write(b"x" * 100)
            with pytest.raises(porto.LimitOverrunError) as until:
                await server.read_until(b"\n", limit=10)
            found.append((until.value.consumed, await server.read_exact(until.value.consumed)))
            assert await server.read_exact(90) == b"x" * 90  # what the first read left unread
            await client.write(b"y" * 50)
            await client.aclose()
            with pytest.raises(porto.LimitOverrunError) as to_end:
                await server.read_to_end(limit=20)
            found.append((to_end.value.consumed, await server.read_exact(to_end.value.consumed)))
            return found

    assert run(main()) == [(10, b"x" * 10), (21, b"y" * 21)]


def test_limit_overrun_survives_pickling() -> None:
    import pickle

    error = pickle.loads(pickle.dumps(porto.LimitOverrunError("too much", 7)))
    assert (str(error), error.consumed) == ("too much", 7)
