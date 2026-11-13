"""``porto.net.TcpStream`` (17-networking-tls-dns.md §1.1, 13-io-driver.md §5) over loopback.

Clients are made with ``porto.net.connect_tcp`` or with
the standard library's sockets, the way a foreign peer would connect. Registering raw sockets
needs the AFD backend on Windows, which is not written yet, so these tests run on Unix.
"""

import errno
import hashlib
import socket
import struct
import sys
import threading
import tracemalloc
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import pytest

import porto
from porto import _core, abc, net

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


async def read_exactly(stream: net.TcpStream, count: int) -> bytes:
    data = b""
    while len(data) < count:
        chunk = await stream.read(count - len(data))
        assert chunk, "the stream ended early"
        data += chunk
    return data


def test_a_stream_reads_and_writes_both_ways() -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"ping")
            ping = await read_exactly(server, 4)
            await server.write(b"pong")
            return ping, await read_exactly(client, 4)

    assert run(main()) == (b"ping", b"pong")


def test_a_stream_is_a_byte_stream() -> None:
    async def main() -> bool:
        client, server, listener = await connected()
        async with client, server, listener:
            return isinstance(client, abc.ByteStream) and isinstance(server, abc.ByteStream)

    assert run(main())


def test_addresses_and_the_file_descriptor() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            assert client.peername == (LOOPBACK, listener.port)
            assert server.sockname == (LOOPBACK, listener.port)
            assert server.peername == client.sockname
            assert listener.sockname == (LOOPBACK, listener.port)
            fd = client.fileno()
            assert isinstance(fd, int)
            with socket.socket(fileno=socket.dup(fd)) as duplicate:
                assert duplicate.getpeername() == (LOOPBACK, listener.port)
        # A closed stream still reports the addresses it had, but has no descriptor.
        assert server.peername[1] == client.sockname[1]
        with pytest.raises(porto.ClosedResourceError):
            client.fileno()

    run(main())


def test_the_peer_name_of_an_accepted_stream_survives_a_reset() -> None:
    async def main() -> tuple[Any, Any]:
        listener = await net.listen_tcp(LOOPBACK)
        async with listener:
            foreign = socket.create_connection((LOOPBACK, listener.port))
            server = await listener.accept()
            name = foreign.getsockname()
            foreign.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            foreign.close()
            with pytest.raises(ConnectionResetError):
                await server.read()
            peer = server.peername
            await server.aclose()
            return name, peer

    name, peer = run(main())
    assert peer == name


def test_read_returns_empty_bytes_at_the_end_of_the_stream() -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await connected()
        async with listener:
            await client.write(b"last")
            await client.aclose()
            first = await server.read()
            second = await server.read()
            await server.aclose()
            return first, second

    assert run(main()) == (b"last", b"")


def test_read_buffer_and_read_into() -> None:
    async def main() -> tuple[bytes, int, bytes, int, bytes]:
        client, server, listener = await connected()
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
    assert capacity == 4096, "a buffer of the lane's pool"
    assert (target, count) == (b"into----", 4)
    assert eof == b""


def test_read_buffer_copies_no_bytes_and_reuses_the_pool() -> None:
    """The payload never becomes a ``bytes`` object: tracemalloc sees no allocation of its
    size, where ``read`` allocates one, and a freed buffer's storage serves the next read."""
    size = 200 * 1024

    async def arrived(client: net.TcpStream) -> None:
        await client.write(b"z" * size)
        await porto.sleep(0.05)  # all of it has arrived

    async def main() -> tuple[int, int, int, int, int]:
        client, server, listener = await connected()
        async with client, server, listener:
            tracemalloc.start()
            try:
                await arrived(client)
                tracemalloc.reset_peak()
                before = tracemalloc.get_traced_memory()[0]
                buffer = await server.read_buffer(size)
                peak_buffer = tracemalloc.get_traced_memory()[1] - before
                held = len(buffer)
                del buffer  # its storage goes back to the lane's pool
                await arrived(client)
                # Each read takes what has arrived, so the sizes differ (bytes the first
                # round left in the kernel come first); only the storage reuse matters here.
                again = await server.read_buffer(size)
                assert len(again) >= 64 * 1024
                del again
                await arrived(client)
                tracemalloc.reset_peak()
                before = tracemalloc.get_traced_memory()[0]
                data = await server.read(size)
                peak_bytes = tracemalloc.get_traced_memory()[1] - before
            finally:
                tracemalloc.stop()
            row = next(r for r in _core._buffer_pool_stats() if r[0] == 256 * 1024)
            return peak_buffer, peak_bytes, min(held, len(data)), row[2], row[3]

    peak_buffer, peak_bytes, held, hits, misses = run(main())
    assert held >= 64 * 1024
    assert peak_buffer < size // 4, "read_buffer allocated no bytes object of the payload's size"
    assert peak_bytes >= held, "while read() allocates one"
    assert (hits, misses) == (1, 1), "the second read reused the first one's storage"


def test_a_large_write_completes_under_a_slow_reader() -> None:
    total = 12 * 1024 * 1024
    payload = bytes(range(256)) * (total // 256)

    async def slow_reader(server: net.TcpStream) -> tuple[int, str]:
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
        client, server, listener = await connected()
        async with listener, server:
            async with porto.scope() as s:
                reader = s.spawn(slow_reader(server))
                async with client:
                    await client.write(payload)  # waits whenever the reader is behind
            return await reader

    received, digest = run(main())
    assert received == total
    assert digest == hashlib.sha256(payload).hexdigest()


def test_writev_sends_every_buffer_in_order() -> None:
    chunks = [b"head ", bytearray(b"middle "), memoryview(b"tail"), b"", b"!"]
    bulk = [bytes([index % 251]) * 100_000 for index in range(40)]

    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await connected()
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


def test_writev_resumes_inside_a_buffer_past_the_vector_limit_and_empty_buffers() -> None:
    """More buffers than one system call takes (64), empty ones among them, and a payload the
    socket buffers cannot hold, so sends stop inside a buffer and go on from there."""
    buffers = [bytes([index % 251]) * (0 if index % 7 == 0 else 20_011) for index in range(400)]
    expected = b"".join(buffers)

    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener, porto.scope() as s:
            reader = s.spawn(read_exactly, server, len(expected))
            await client.writev(buffers)
            return await reader

    assert run(main()) == expected


def test_half_close_is_the_end_of_the_stream_for_the_peer() -> None:
    async def main() -> tuple[bytes, bytes, bytes]:
        client, server, listener = await connected()
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


def test_writing_after_shutdown_write_is_a_broken_pipe() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.shutdown_write()
            with pytest.raises(BrokenPipeError) as caught:
                await client.write(b"too late")
            assert caught.value.errno == errno.EPIPE

    run(main())


def test_a_reset_is_a_connection_reset_error_with_its_errno() -> None:
    async def main() -> None:
        listener = await net.listen_tcp(LOOPBACK)
        async with listener:
            foreign = socket.create_connection((LOOPBACK, listener.port))
            server = await listener.accept()
            async with server:
                # SO_LINGER 0 makes close() send RST instead of FIN.
                foreign.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                foreign.close()
                with pytest.raises(ConnectionResetError) as caught:
                    await server.read()
                assert caught.value.errno == errno.ECONNRESET

    run(main())


def test_a_refused_connection_is_a_connection_refused_error() -> None:
    async def main() -> None:
        listener = await net.listen_tcp(LOOPBACK)
        port = listener.port
        await listener.aclose()
        with pytest.raises(ConnectionRefusedError) as caught:
            await net.connect_tcp(LOOPBACK, port)
        assert caught.value.errno == errno.ECONNREFUSED

    run(main())


def test_closing_a_stream_fails_a_task_reading_from_it() -> None:
    async def main() -> None:
        client, server, listener = await connected()
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


def test_every_operation_on_a_closed_stream_fails_and_closing_again_is_fine() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with listener, server:
            await client.aclose()
            await client.aclose()
            calls: list[Callable[[], Awaitable[object]]] = [
                client.read,
                client.read_buffer,
                lambda: client.read_into(bytearray(4)),
                lambda: client.write(b"x"),
                lambda: client.writev([b"x"]),
                client.shutdown_write,
            ]
            for call in calls:
                with pytest.raises(porto.ClosedResourceError):
                    await call()
            with pytest.raises(porto.ClosedResourceError):
                client.set_nodelay(True)
            assert await server.read() == b"", "the peer sees the end of the stream"

    run(main())


def test_async_with_closes_the_stream_on_exit_even_for_an_exception() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with listener, server:
            with pytest.raises(KeyError):
                async with client:
                    raise KeyError("boom")
            with pytest.raises(porto.ClosedResourceError):
                await client.read()

    run(main())


def test_nodelay_and_keepalive_are_set_on_the_socket() -> None:
    async def main() -> tuple[int, int, int]:
        client, server, listener = await connected()
        async with client, server, listener:
            client.set_nodelay(True)
            client.set_keepalive(30.5, 10, 4)
            with socket.socket(fileno=socket.dup(client.fileno())) as view:
                nodelay = view.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
                keepalive = view.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
                interval = view.getsockopt(socket.IPPROTO_TCP, _keepintvl())
            client.set_nodelay(False)
            return nodelay, keepalive, interval

    nodelay, keepalive, interval = run(main())
    assert nodelay != 0
    assert keepalive != 0
    assert interval == 10


def _keepintvl() -> int:
    return getattr(socket, "TCP_KEEPINTVL", 0x101)


@pytest.mark.parametrize(
    ("idle", "interval", "count"),
    [(0, 1, 1), (1, 0, 1), (-1, 1, 1), (float("nan"), 1, 1), (float("inf"), 1, 1)],
)
def test_keepalive_needs_positive_numbers(idle: float, interval: float, count: int) -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            with pytest.raises(ValueError, match="positive finite numbers of seconds"):
                client.set_keepalive(idle, interval, count)

    run(main())


@pytest.mark.parametrize("count", [0, -1, 2**32, 2**70])
def test_keepalive_count_out_of_range_is_a_value_error(count: int) -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            with pytest.raises(ValueError, match=r"count must be an int in 1\.\.=4294967295"):
                client.set_keepalive(1, 1, count)

    run(main())


@pytest.mark.parametrize("max_bytes", [2**63, 2**64, 2**70, -(2**70)])
def test_read_sizes_out_of_range_are_value_errors(max_bytes: int) -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            for call in (client.read, client.read_buffer):
                with pytest.raises(ValueError, match=r"max must be an int in 1\.\.="):
                    call(max_bytes)
            with pytest.raises(TypeError):
                client.read("10")  # pyright: ignore[reportArgumentType]

    run(main())


def test_none_means_the_default_read_size() -> None:
    async def main() -> tuple[bytes, int]:
        client, server, listener = await connected()
        async with client, server, listener:
            await client.write(b"x" * 100_000)
            await porto.sleep(0.05)
            first = await server.read(None)
            buffer = await server.read_buffer(None)
            return first, len(buffer)

    first, buffered = run(main())
    assert 0 < len(first) <= 65536
    assert 0 <= buffered <= 65536


def test_a_read_of_no_bytes_is_refused_because_it_would_look_like_the_end() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            for call in (client.read, client.read_buffer):
                for bad in (0, -1):
                    with pytest.raises(ValueError, match=r"max must be an int in 1\.\."):
                        call(bad)
            with pytest.raises(ValueError, match="buffer is empty"):
                client.read_into(bytearray())

    run(main())


def test_wrong_kinds_of_data_say_what_to_pass() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            with pytest.raises(TypeError, match="bytes-like object"):
                client.write("text")  # pyright: ignore[reportArgumentType]
            with pytest.raises(TypeError, match=r"writev\(\)'s buffers\[1\]"):
                client.writev([b"fine", "text"])  # pyright: ignore[reportArgumentType]
            with pytest.raises(TypeError, match="writable bytes-like object"):
                client.read_into(b"immutable")  # pyright: ignore[reportArgumentType]

    run(main())


def test_a_stream_of_an_earlier_run_is_refused_at_the_call() -> None:
    left, right = socket.socketpair()
    with left, right:
        holder: list[net.TcpStream] = []

        async def first() -> None:
            listener = await net.listen_tcp(LOOPBACK)
            holder.append(await net.connect_tcp(LOOPBACK, listener.port))

        run(first())

        async def second() -> None:
            with pytest.raises(porto.RuntimeClosed, match="runtime that stopped"):
                holder[0].read()

        run(second())


def test_a_stream_of_a_run_on_another_thread_is_refused_at_the_call() -> None:
    refused: list[str] = []

    async def other_run(stream: net.TcpStream) -> None:
        try:
            stream.write(b"x")
        except RuntimeError as error:
            refused.append(str(error))

    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            # `_run`, not `porto.run`: I1 refuses a second public run while one runs.
            thread = threading.Thread(target=_core._run, args=(other_run(client),))
            thread.start()
            thread.join()
            await client.write(b"still works here")
            assert await read_exactly(server, 16) == b"still works here"

    run(main())
    assert len(refused) == 1
    assert "open the socket in the run that uses it" in refused[0]


# A write cancelled after part of its data went out (13-io-driver.md §5)


def test_a_write_cancelled_midway_breaks_the_stream() -> None:
    payload = b"x" * (64 * 1024 * 1024)  # far more than the socket buffers hold

    async def main() -> None:
        client, server, listener = await connected()
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
            await client.aclose()

    run(main())


def test_a_vectored_write_cancelled_midway_breaks_the_stream() -> None:
    buffers = [b"y" * (4 * 1024 * 1024)] * 16

    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            async with porto.move_on_after(0.05) as scope:
                await client.writev(buffers)
            assert scope.timed_out
            with pytest.raises(porto.BrokenResourceError, match="close the stream"):
                await client.write(b"more")

    run(main())


def test_a_write_cancelled_before_it_sent_anything_leaves_the_stream_usable() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener:
            async with porto.move_on_after(0) as scope:
                await porto.checkpoint()
                await client.write(b"never sent")
            assert scope.timed_out
            await client.write(b"fine")
            return await read_exactly(server, 4)

    assert run(main()) == b"fine"


def test_a_completed_write_leaves_the_stream_usable_after_a_cancel() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener:
            async with porto.move_on_after(1) as scope:
                await client.write(b"complete")
                scope.cancel()
            await client.write(b"again")
            return await read_exactly(server, 13)

    assert run(main()) == b"completeagain"


def test_a_write_that_fails_with_an_os_error_after_a_partial_send_does_not_blame_a_cancel() -> None:
    """A reset does not break the stream (other errors after a partial send do): after a reset
    the stream's own errors are the answer, not a ``BrokenResourceError``."""
    payload = b"x" * (64 * 1024 * 1024)

    async def main() -> tuple[type[BaseException], type[BaseException]]:
        listener = await net.listen_tcp(LOOPBACK)
        async with listener:
            foreign = socket.create_connection((LOOPBACK, listener.port))
            server = await listener.accept()
            async with server:

                async def reset_later() -> None:
                    await porto.sleep(0.05)  # the write has sent a part and waits
                    foreign.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                    foreign.close()

                async with porto.scope() as s:
                    s.spawn(reset_later())
                    with pytest.raises(OSError, match="Errno") as first:
                        await server.write(payload)
                with pytest.raises(OSError, match="Errno") as later:
                    await server.write(b"more")
                assert not isinstance(later.value, porto.BrokenResourceError)
                return type(first.value), type(later.value)

    first, later = run(main())
    assert issubclass(first, (ConnectionResetError, BrokenPipeError))
    assert issubclass(later, (ConnectionResetError, BrokenPipeError))


# One reader and one writer at a time (13-io-driver.md §5)


def test_a_second_writer_is_refused_while_a_write_waits() -> None:
    payload = b"w" * (64 * 1024 * 1024)

    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener:
            async with porto.scope() as s:
                first = s.spawn(client.write, payload)
                await porto.sleep(0.02)  # the first write has sent a part and waits
                for call in (lambda: client.write(b"intruder"), lambda: client.writev([b"i"])):
                    with pytest.raises(porto.BusyResourceError, match=r"porto\.Lock") as caught:
                        call()
                    assert isinstance(caught.value, porto.PortoError)
                    assert "one reader and one writer at a time" in str(caught.value)
                # The refused calls changed nothing: the first write goes on and completes.
                received = s.spawn(read_exactly, server, len(payload))
                await first
            data = await received
            return data[:4] + data[-4:]

    assert run(main()) == b"wwwwwwww"


def test_a_second_reader_is_refused_while_a_read_waits() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener, porto.scope() as s:
            first = s.spawn(server.read, 16)
            await porto.sleep(0.01)  # waiting for data
            for call in (
                lambda: server.read(8),
                lambda: server.read_buffer(8),
                lambda: server.read_into(bytearray(8)),
            ):
                with pytest.raises(porto.BusyResourceError, match="reading from"):
                    call()
            await client.write(b"for the first")
            return await first

    assert run(main()) == b"for the first"


def test_a_reader_and_a_writer_may_use_a_stream_at_once() -> None:
    async def main() -> tuple[bytes, bytes]:
        client, server, listener = await connected()
        async with client, server, listener, porto.scope() as s:
            reading = s.spawn(client.read, 8)
            await porto.sleep(0.01)
            await client.write(b"out")  # a write while a read waits
            await server.write(b"in")
            reply = await reading
            return reply, await read_exactly(server, 3)

    assert run(main()) == (b"in", b"out")


def test_the_claim_is_released_when_the_operation_ends_however_it_ends() -> None:
    async def main() -> None:
        client, server, listener = await connected()
        async with client, server, listener:
            # Completed (fast path and slow path), failed, cancelled: each frees the stream.
            await client.write(b"a")
            await client.write(b"b")
            assert await read_exactly(server, 2) == b"ab"
            async with porto.move_on_after(0.01):
                await server.read()  # cancelled while waiting
            await client.write(b"c")
            assert await server.read(4) == b"c"
            with pytest.raises(TypeError):
                server.read_into(b"immutable")  # pyright: ignore[reportArgumentType]
            await client.write(b"d")
            assert await server.read(4) == b"d"
            await client.aclose()
            assert await server.read() == b""
            await server.shutdown_write()
            with pytest.raises(BrokenPipeError):
                await server.write(b"e")  # failed
            with pytest.raises(BrokenPipeError):
                await server.write(b"f")  # and the claim was free again to fail the same way

    run(main())


def test_cancelling_a_read_loses_no_bytes() -> None:
    async def main() -> bytes:
        client, server, listener = await connected()
        async with client, server, listener:
            async with porto.move_on_after(0.02):
                await server.read()
            await client.write(b"after")
            return await read_exactly(server, 5)

    assert run(main()) == b"after"
