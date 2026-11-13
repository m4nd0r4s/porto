"""The ``porto.abc.ByteStream`` contract (17-networking-tls-dns.md §1.1), checked on every stream
type: each test runs once for ``TcpStream`` and once for ``UnixStream`` (and ``TlsStream`` joins the
list with its bead). A stream type that passes this suite is a ``ByteStream`` the way the spec
says; what only one type has (TCP's options, a Unix socket's path and file mode) is tested in its
own module, and the TCP-only wire behavior (resets, refused connects) in ``test_net_tcp.py``.
Over loopback; I/O needs the Unix backends.
"""

import errno
import hashlib
import sys
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, TypeAlias

import pytest

import porto
from porto import abc, net

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="registering raw sockets needs the AFD backend (Windows)"
)

LOOPBACK = "127.0.0.1"
KINDS = ["tcp", "unix"]

Stream: TypeAlias = net.TcpStream | net.UnixStream
Listener: TypeAlias = net.TcpListener | net.UnixListener
Pair: TypeAlias = Callable[[], Awaitable[tuple[Stream, Stream, Listener]]]


TEST_DEADLINE = 60  # seconds a test may take: none comes near it


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    """Runs ``coro`` in ``porto.run`` under a deadline: a test that would wait forever (a datagram
    or connection that never comes) fails after a minute with ``TimeoutError`` instead of
    hanging the suite."""

    async def bounded() -> Any:
        async with porto.timeout(TEST_DEADLINE):
            return await coro

    return porto.run(bounded())


async def connected(kind: str, unix_path: str) -> tuple[Stream, Stream, Listener]:
    """A client, the stream the listener accepted for it, and the listener."""
    if kind == "tcp":
        tcp_listener = await net.listen_tcp(LOOPBACK)
        return (
            await net.connect_tcp(LOOPBACK, tcp_listener.port),
            await tcp_listener.accept(),
            tcp_listener,
        )
    unix_listener = await net.listen_unix(unix_path)
    return await net.connect_unix(unix_path), await unix_listener.accept(), unix_listener


async def read_exactly(stream: abc.ByteStream, count: int) -> bytes:
    # A bytearray: `data += chunk` on bytes copies everything received so far, which over a
    # socket that delivers 8 KiB at a time (macOS Unix sockets) made a 64 MiB read take 30 s.
    data = bytearray()
    while len(data) < count:
        chunk = await stream.read(count - len(data))
        assert chunk, "the stream ended early"
        data += chunk
    return bytes(data)


@pytest.fixture(params=KINDS)
def pair(request: pytest.FixtureRequest, unix_socket_path: str) -> Pair:
    kind: str = request.param
    return lambda: connected(kind, unix_socket_path)


def test_a_stream_reads_and_writes_both_ways(pair: Pair) -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await pair()
        async with client, server, listener:
            await client.write(b"ping")
            ping = await read_exactly(server, 4)
            await server.write(b"pong")
            return ping, await read_exactly(client, 4)

    assert run(main()) == (b"ping", b"pong")


def test_a_stream_is_a_byte_stream(pair: Pair) -> None:
    async def main() -> None:
        client, server, listener = await pair()
        async with client, server, listener:
            assert isinstance(client, abc.ByteStream)
            assert isinstance(server, abc.ByteStream)
            assert type(client) is type(server)

    run(main())


def test_the_file_descriptor_is_an_int_until_the_stream_is_closed(pair: Pair) -> None:
    async def main() -> None:
        client, server, listener = await pair()
        async with server, listener:
            assert isinstance(client.fileno(), int)
            await client.aclose()
            with pytest.raises(porto.ClosedResourceError):
                client.fileno()

    run(main())


def test_read_returns_empty_bytes_at_the_end_of_the_stream(pair: Pair) -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await pair()
        async with listener:
            await client.write(b"last")
            await client.aclose()
            first = await server.read()
            second = await server.read()
            await server.aclose()
            return first, second

    assert run(main()) == (b"last", b"")


def test_read_buffer_and_read_into(pair: Pair) -> None:
    async def main() -> tuple[bytes, int, bytes, int, bytes]:
        client, server, listener = await pair()
        async with client, server, listener:
            await client.write(b"zero-copy")
            buffer = await server.read_buffer(64)
            assert type(buffer) is porto.Buffer
            await client.write(b"into")
            target = bytearray(b"-" * 8)
            count = await server.read_into(target)
            await client.shutdown_write()
            eof = await server.read_buffer(8)
            return bytes(buffer), buffer.capacity, bytes(target), count, bytes(eof)

    data, capacity, target, count, eof = run(main())
    assert data == b"zero-copy"
    assert capacity >= len(data)
    assert target[:count] == b"into"[:count]
    assert count > 0
    assert eof == b""


def test_a_large_write_completes_under_a_slow_reader(pair: Pair) -> None:
    total = 12 * 1024 * 1024
    payload = bytes(range(256)) * (total // 256)

    async def slow_reader(server: abc.ByteStream) -> tuple[int, str]:
        digest = hashlib.sha256()
        received = 0
        while True:
            chunk = await server.read(32 * 1024)
            if not chunk:
                return received, digest.hexdigest()
            digest.update(chunk)
            received += len(chunk)
            if received % (1024 * 1024) < len(chunk):
                await porto.sleep(0.002)

    async def main() -> tuple[int, str]:
        client, server, listener = await pair()
        async with listener, server:
            async with porto.scope() as s:
                reader = s.spawn(slow_reader(server))
                async with client:
                    await client.write(payload)  # waits whenever the reader is behind
            return await reader

    received, digest = run(main())
    assert received == total
    assert digest == hashlib.sha256(payload).hexdigest()


def test_writev_sends_every_buffer_in_order(pair: Pair) -> None:
    chunks = [b"head ", bytearray(b"middle "), memoryview(b"tail"), b"", b"!"]
    bulk = [bytes([index % 251]) * 100_000 for index in range(40)]

    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await pair()
        async with client, server, listener:
            await client.writev(chunks)
            small = await read_exactly(server, 17)
            async with porto.scope() as s:
                reader = s.spawn(read_exactly, server, sum(map(len, bulk)))
                await client.writev(bulk)  # more than the socket buffers hold
            return small, reader.result()

    small, large = run(main())
    assert small == b"head middle tail!"
    assert large == b"".join(bulk)


def test_half_close_is_the_end_of_the_stream_for_the_peer(pair: Pair) -> None:
    async def main() -> tuple[bytes, bytes, bytes]:
        client, server, listener = await pair()
        async with client, server, listener:
            await client.write(b"request")
            await client.shutdown_write()
            request = await read_exactly(server, 7)
            at_eof = await server.read()
            # The other direction is still open.
            await server.write(b"response")
            await server.shutdown_write()
            response = await read_exactly(client, 8)
            return request, at_eof, response

    assert run(main()) == (b"request", b"", b"response")


def test_writing_after_shutdown_write_is_a_broken_pipe(pair: Pair) -> None:
    async def main() -> None:
        client, server, listener = await pair()
        async with client, server, listener:
            await client.shutdown_write()
            with pytest.raises(BrokenPipeError) as caught:
                await client.write(b"too late")
            assert caught.value.errno == errno.EPIPE

    run(main())


def test_writing_to_a_peer_that_closed_is_an_error_not_a_signal(pair: Pair) -> None:
    """A write to a stream whose peer is gone raises ``BrokenPipeError`` (or a reset), and the
    process is not killed by ``SIGPIPE``."""

    async def write_until_it_fails(client: Stream) -> None:
        for _ in range(100):  # the first write may still be accepted by the kernel
            await client.write(b"x" * 65536)

    async def main() -> type[BaseException]:
        client, server, listener = await pair()
        async with client, listener:
            await server.aclose()
            with pytest.raises(OSError, match="Errno") as caught:
                await write_until_it_fails(client)
            return type(caught.value)

    assert issubclass(run(main()), (BrokenPipeError, ConnectionResetError))


def test_closing_a_stream_fails_a_task_reading_from_it(pair: Pair) -> None:
    async def main() -> None:
        client, server, listener = await pair()
        async with client, server, listener:

            async def close_later() -> None:
                await porto.sleep(0.01)
                await server.aclose()

            async with porto.scope() as s:
                s.spawn(close_later())
                with pytest.raises(porto.ClosedResourceError) as caught:
                    await server.read()
            assert isinstance(caught.value, porto.PortoError)
            assert isinstance(caught.value, OSError)
            assert caught.value.errno == errno.EBADF

    run(main())


def test_every_operation_on_a_closed_stream_fails_and_closing_again_is_fine(pair: Pair) -> None:
    async def main() -> None:
        client, server, listener = await pair()
        async with listener, server:
            await client.aclose()
            await client.aclose()
            calls: list[Callable[[], Awaitable[object]]] = [
                client.read,
                client.read_buffer,
                lambda: client.read_into(bytearray(4)),
                lambda: client.read_exact(1),
                lambda: client.readline(),
                lambda: client.read_to_end(limit=10),
                lambda: client.write(b"x"),
                lambda: client.writev([b"x"]),
                client.shutdown_write,
            ]
            for call in calls:
                with pytest.raises(porto.ClosedResourceError):
                    await call()
            assert await server.read() == b"", "the peer sees the end of the stream"

    run(main())


def test_async_with_closes_the_stream_on_exit_even_for_an_exception(pair: Pair) -> None:
    async def main() -> None:
        client, server, listener = await pair()
        async with listener, server:
            with pytest.raises(KeyError):
                async with client:
                    raise KeyError("boom")
            with pytest.raises(porto.ClosedResourceError):
                await client.read()

    run(main())


@pytest.mark.parametrize("max_bytes", [2**63, 2**64, 2**70, -(2**70)])
def test_read_sizes_out_of_range_are_value_errors(pair: Pair, max_bytes: int) -> None:
    async def main() -> None:
        client, server, listener = await pair()
        async with client, server, listener:
            for call in (client.read, client.read_buffer):
                with pytest.raises(ValueError, match=r"max must be an int in 1\.\.="):
                    call(max_bytes)
            with pytest.raises(TypeError):
                client.read("10")  # pyright: ignore[reportArgumentType]

    run(main())


def test_a_read_of_no_bytes_is_refused_because_it_would_look_like_the_end(pair: Pair) -> None:
    async def main() -> None:
        client, server, listener = await pair()
        async with client, server, listener:
            for call in (client.read, client.read_buffer):
                for bad in (0, -1):
                    with pytest.raises(ValueError, match=r"max must be an int in 1\.\."):
                        call(bad)
            with pytest.raises(ValueError, match="buffer is empty"):
                client.read_into(bytearray())

    run(main())


def test_wrong_kinds_of_data_say_what_to_pass(pair: Pair) -> None:
    async def main() -> None:
        client, server, listener = await pair()
        async with client, server, listener:
            with pytest.raises(TypeError, match="bytes-like object"):
                client.write("text")  # pyright: ignore[reportArgumentType]
            with pytest.raises(TypeError, match=r"writev\(\)'s buffers\[1\]"):
                client.writev([b"fine", "text"])  # pyright: ignore[reportArgumentType]
            with pytest.raises(TypeError, match="writable bytes-like object"):
                client.read_into(b"immutable")  # pyright: ignore[reportArgumentType]

    run(main())


def test_a_stream_of_an_earlier_run_is_refused_at_the_call(pair: Pair) -> None:
    holder: list[Stream] = []

    async def first() -> None:
        client, _server, _listener = await pair()
        holder.append(client)

    run(first())

    async def second() -> None:
        with pytest.raises(porto.RuntimeClosed, match="runtime that stopped"):
            holder[0].read()

    run(second())


# A write cancelled after part of its data went out (13-io-driver.md §5)


def test_a_write_cancelled_midway_breaks_the_stream(pair: Pair) -> None:
    payload = b"x" * (64 * 1024 * 1024)  # far more than the socket buffers hold

    async def main() -> None:
        client, server, listener = await pair()
        async with client, server, listener:
            async with porto.move_on_after(0.05) as scope:
                await client.write(payload)  # nobody reads: it waits, having sent a part
            assert scope.timed_out
            for call in (lambda: client.write(b"more"), lambda: client.writev([b"more"])):
                with pytest.raises(porto.BrokenResourceError, match="close the stream") as caught:
                    await call()
                assert "left mid-write" in str(caught.value)
            # Reading is unaffected, and so is the other side.
            await server.write(b"reply")
            assert await read_exactly(client, 5) == b"reply"

    run(main())


def test_a_write_cancelled_before_it_sent_anything_leaves_the_stream_usable(pair: Pair) -> None:
    async def main() -> bytes:
        client, server, listener = await pair()
        async with client, server, listener:
            async with porto.move_on_after(0) as scope:
                await porto.checkpoint()
                await client.write(b"never sent")
            assert scope.timed_out
            await client.write(b"fine")
            return await read_exactly(server, 4)

    assert run(main()) == b"fine"


# One reader and one writer at a time (13-io-driver.md §5)


def test_a_second_writer_is_refused_while_a_write_waits(pair: Pair) -> None:
    payload = b"w" * (64 * 1024 * 1024)

    async def main() -> bytes:
        client, server, listener = await pair()
        async with client, server, listener:
            async with porto.scope() as s:
                first = s.spawn(client.write, payload)
                await porto.sleep(0.02)  # the first write has sent a part and waits
                for call in (lambda: client.write(b"intruder"), lambda: client.writev([b"i"])):
                    with pytest.raises(porto.BusyResourceError, match=r"porto\.Lock") as caught:
                        call()
                    assert isinstance(caught.value, porto.PortoError)
                received = s.spawn(read_exactly, server, len(payload))
                await first
            data = await received
            return data[:4] + data[-4:]

    assert run(main()) == b"wwwwwwww"


def test_a_second_reader_is_refused_while_a_read_waits(pair: Pair) -> None:
    async def main() -> bytes:
        client, server, listener = await pair()
        async with client, server, listener, porto.scope() as s:
            first = s.spawn(server.read, 16)
            await porto.sleep(0.01)  # waiting for data
            for call in (
                lambda: server.read(8),
                lambda: server.read_buffer(8),
                lambda: server.read_into(bytearray(8)),
                lambda: server.readline(),
            ):
                with pytest.raises(porto.BusyResourceError, match="reading from"):
                    call()
            await client.write(b"for the first")
            return await first

    assert run(main()) == b"for the first"


def test_a_reader_and_a_writer_may_use_a_stream_at_once(pair: Pair) -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await pair()
        async with client, server, listener, porto.scope() as s:
            reading = s.spawn(client.read, 8)
            await porto.sleep(0.01)
            await client.write(b"out")  # a write while a read waits
            await server.write(b"in")
            reply = await reading
            return reply, await read_exactly(server, 3)

    assert run(main()) == (b"in", b"out")


def test_cancelling_a_read_loses_no_bytes(pair: Pair) -> None:
    async def main() -> bytes:
        client, server, listener = await pair()
        async with client, server, listener:
            async with porto.move_on_after(0.02):
                await server.read()
            await client.write(b"after")
            return await read_exactly(server, 5)

    assert run(main()) == b"after"


# The buffered reads (17-networking-tls-dns.md §1.1)


def test_read_exact_collects_pieces_and_leaves_the_rest(pair: Pair) -> None:
    async def main() -> list[bytes]:
        client, server, listener = await pair()
        async with client, server, listener, porto.scope() as s:
            reading = s.spawn(server.read_exact, 12)
            for piece in (b"abc", b"d", b"efgh", b"ijkl", b"rest"):
                await client.write(piece)
                await porto.sleep(0.001)
            return [await reading, await server.read_exact(0), await server.read_exact(4)]

    assert run(main()) == [b"abcdefghijkl", b"", b"rest"]


def test_read_exact_at_the_end_of_the_stream_raises_with_what_arrived(pair: Pair) -> None:
    async def main() -> porto.IncompleteReadError:
        client, server, listener = await pair()
        async with server, listener:
            await client.write(b"short")
            await client.aclose()
            with pytest.raises(porto.IncompleteReadError) as caught:
                await server.read_exact(10)
            return caught.value

    error = run(main())
    assert error.partial == b"short"
    assert error.expected == 10


def test_readline_and_read_until_split_what_arrives_glued_together(pair: Pair) -> None:
    async def main() -> list[bytes]:
        client, server, listener = await pair()
        async with client, server, listener:
            await client.write(b"one\ntwo\r\nthree")
            lines = [await server.readline(), await server.read_until(b"\r\n")]
            await client.aclose()
            with pytest.raises(porto.IncompleteReadError) as caught:
                await server.readline()
            lines.append(caught.value.partial)
            with pytest.raises(porto.IncompleteReadError) as at_end:
                await server.readline()
            lines.append(at_end.value.partial)
            return lines

    assert run(main()) == [b"one\n", b"two\r\n", b"three", b""]


def test_read_until_past_the_limit_consumes_nothing(pair: Pair) -> None:
    async def main() -> tuple[int, bytes]:
        client, server, listener = await pair()
        async with client, server, listener:
            await client.write(b"x" * 100)
            with pytest.raises(porto.LimitOverrunError) as caught:
                await server.read_until(b"\n", limit=10)
            consumed = caught.value.consumed
            return consumed, await server.read_exact(consumed)

    consumed, data = run(main())
    assert consumed >= 10
    assert data == b"x" * consumed


def test_read_to_end_reads_until_the_peer_closes_and_needs_a_limit(pair: Pair) -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await pair()
        async with server, listener:
            await client.write(b"all of it")
            await client.shutdown_write()
            everything = await server.read_to_end(limit=100)
            with pytest.raises(TypeError):
                server.read_to_end()  # pyright: ignore[reportCallIssue]
            await client.aclose()
            return everything, await server.read_to_end(limit=100)

    assert run(main()) == (b"all of it", b"")


def test_a_cancelled_buffered_read_keeps_what_it_received(pair: Pair) -> None:
    async def main() -> bytes:
        client, server, listener = await pair()
        async with client, server, listener:
            await client.write(b"par")
            async with porto.move_on_after(0.02):
                await server.read_exact(10)  # receives "par", then waits
            await client.write(b"tial-and-more")
            return await server.read_exact(10)

    assert run(main()) == b"partial-an"
