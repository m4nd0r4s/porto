"""The ``Io`` trap and its operations (13-io-driver.md §5, 10-architecture-overview.md §4,
11-task-model-and-trap-protocol.md T1, T3).

``_core._IoHandle(fd)`` registers a duplicate of a socket with the running lane's driver; its
methods are the ``IoOp`` set. An operation that can complete at once returns a ``_Ready`` (the
scheduler never sees it); one that would block returns exactly one ``Io`` trap, which waits for
readiness and attempts the operation again. Registering raw sockets needs the AFD backend on
Windows, which is not written yet, so these tests run on Unix.
"""

import os
import socket
import sys
import tempfile
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

import porto
from porto import _core

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="registering raw sockets needs the AFD backend (Windows)"
)


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return porto.run(coro)


async def wait_until(condition: Callable[[], bool]) -> None:
    while not condition():
        await porto.checkpoint()


def socketpair() -> tuple[socket.socket, socket.socket]:
    left, right = socket.socketpair()
    left.setblocking(False)
    return left, right


def test_a_fast_path_read_returns_a_ready_awaitable() -> None:
    async def main() -> bytes:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            left.send(b"ready")
            awaitable = handle.read(64)
            assert type(awaitable) is _core._Ready
            data = await awaitable
            handle.close()
            return data

    assert run(main()) == b"ready"


def test_a_slow_path_read_yields_exactly_one_io_trap() -> None:
    async def reader(handle: Any) -> bytes:
        return await handle.read(64)

    async def main() -> tuple[str, int, bytes]:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            awaitable = handle.read(64)
            assert type(awaitable) is _core._Trap
            kind = awaitable.kind
            awaitable.close()
            target = _core._spawn_joinable(reader(handle))
            # Let it start and park on the trap, then make the socket readable.
            for _ in range(3):
                await porto.checkpoint()
            assert not target.done
            left.send(b"later")
            data = await target.join()
            steps = target.steps
            handle.close()
            return kind, steps, data

    kind, steps, data = run(main())
    assert kind == "io"
    # Started once, resumed once by the readiness: one trap, no other round trip.
    assert steps == 2
    assert data == b"later"


def test_T3_trap_cancel_safe_read() -> None:
    """Cancelling a pending read loses no bytes: what the cancelled reader did not get (a wait
    that is already over wins over the cancel) the next read gets."""

    async def main() -> tuple[bytes, int]:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            received = b""
            cancelled_reads = 0
            for round_ in range(20):
                started: list[bool] = []

                async def read_once(started: list[bool]) -> None:
                    nonlocal received, cancelled_reads
                    started.append(True)
                    try:
                        received += await handle.read(64)
                    except porto.Cancelled:
                        cancelled_reads += 1
                        raise

                async with porto.scope() as s:
                    s.spawn(read_once(started))
                    await wait_until(lambda: bool(started))  # noqa: B023 (awaited here)
                    await porto.checkpoint()
                    # The data and the cancel land before the reader runs again.
                    left.send(b"%02d" % round_)
                    s.cancel()
            while len(received) < 40:
                received += await handle.read(64)
            handle.close()
            return received, cancelled_reads

    received, _ = run(main())
    assert received == b"".join(b"%02d" % n for n in range(20))


def test_T3_a_cancelled_read_with_no_data_consumes_nothing() -> None:
    async def main() -> bytes:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            outcome: list[str] = []

            async def read_once() -> None:
                try:
                    await handle.read(64)
                    outcome.append("read")
                except porto.Cancelled:
                    outcome.append("cancelled")
                    raise

            async with porto.scope() as s:
                s.spawn(read_once())
                await porto.sleep(0.01)
                s.cancel()
            assert outcome == ["cancelled"]
            left.send(b"kept")
            data = await handle.read(64)
            handle.close()
            return data

    assert run(main()) == b"kept"


def test_T3_a_read_cancelled_once_readable_delivers_the_bytes_or_leaves_them() -> None:
    """The readiness wakes the reader and the cancel lands before it runs: the reader either
    gets the bytes (a wait that is over wins) or leaves them on the socket."""

    async def main() -> tuple[bytes, bytes]:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            got = b""
            started = False

            async def read_once() -> None:
                nonlocal got, started
                started = True
                got = await handle.read(64)
                await porto.checkpoint()

            async with porto.scope() as s:
                s.spawn(read_once())
                await wait_until(lambda: started)
                await porto.checkpoint()
                left.send(b"payload")
                # Give the driver a turn so the readiness is dispatched, then cancel.
                await porto.sleep(0.01)
                s.cancel()
            rest = b"" if got else await handle.read(64)
            handle.close()
            return got, rest

    got, rest = run(main())
    assert got + rest == b"payload"


def test_read_into_fills_a_writable_buffer() -> None:
    async def main() -> tuple[int, bytes]:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            buffer = bytearray(8)

            async def later() -> None:
                await porto.sleep(0.01)
                left.send(b"abc")

            async with porto.scope() as s:
                s.spawn(later())
                count = await handle.read_into(buffer)
            handle.close()
            return count, bytes(buffer)

    assert run(main()) == (3, b"abc\0\0\0\0\0")


def test_read_into_rejects_a_read_only_buffer() -> None:
    async def main() -> None:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            with pytest.raises(TypeError, match="writable bytes-like object"):
                handle.read_into(b"immutable")
            handle.close()

    run(main())


def test_write_rejects_a_non_buffer() -> None:
    async def main() -> None:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            with pytest.raises(TypeError, match="bytes-like object"):
                handle.write("text")
            handle.close()

    run(main())


def test_a_write_larger_than_the_socket_buffer_completes_across_readiness() -> None:
    payload = bytes(range(256)) * 8192  # 2 MiB: far more than a socket buffer

    async def main() -> bytes:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            received = bytearray()

            async def drain() -> None:
                while len(received) < len(payload):
                    try:
                        chunk = left.recv(65536)
                    except BlockingIOError:
                        await porto.sleep(0.001)
                        continue
                    received.extend(chunk)

            async with porto.scope() as s:
                s.spawn(drain())
                await handle.write(memoryview(payload))
            handle.close()
            return bytes(received)

    assert run(main()) == payload


def test_a_bytearray_cannot_be_resized_while_a_write_holds_it() -> None:
    async def main() -> None:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            data = bytearray(b"x" * (4 << 20))
            awaitable = handle.write(data)
            with pytest.raises(BufferError):
                data.extend(b"more")
            # The trap, never awaited, releases the view when it is dropped.
            del awaitable
            data.extend(b"more")
            handle.close()

    run(main())


def test_accept_and_connect_over_tcp() -> None:
    async def main() -> tuple[bytes, bool]:
        with socket.socket() as listener, socket.socket() as client:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            server = _core._IoHandle(listener.fileno())
            connector = _core._IoHandle(client.fileno())
            accepted: list[Any] = []

            async def accept() -> None:
                accepted.append(await server.accept())

            async with porto.scope() as s:
                s.spawn(accept())
                await connector.connect(listener.getsockname())
            connection, address = accepted[0]
            await connector.write(b"hello")
            data = await connection.read(64)
            peer_ok = address[0] == "127.0.0.1" and isinstance(address[1], int)
            for handle in (connection, connector, server):
                handle.close()
            return data, peer_ok

    assert run(main()) == (b"hello", True)


def test_connect_reports_a_refused_connection() -> None:
    async def main() -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            address = probe.getsockname()
        # Nothing listens on that port.
        with socket.socket() as client:
            handle = _core._IoHandle(client.fileno())
            with pytest.raises(ConnectionRefusedError):
                await handle.connect(address)
            handle.close()

    run(main())


def test_a_cancelled_task_does_not_start_a_connect() -> None:
    async def main() -> None:
        with socket.socket() as listener, socket.socket() as client:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            handle = _core._IoHandle(client.fileno())
            async with porto.scope() as s:
                s.cancel()
                with pytest.raises(porto.Cancelled):
                    handle.connect(listener.getsockname())
            with pytest.raises(OSError, match="not connected"):
                client.getpeername()
            handle.close()

    run(main())


def test_unix_datagrams_to_str_and_bytes_paths() -> None:
    # A short directory: pytest's tmp_path is too long for sun_path on macOS.
    directory = tempfile.TemporaryDirectory(dir="/tmp")

    async def main() -> list[Any]:
        path = os.path.join(directory.name, "porto.sock")
        with (
            socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as a,
            socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as b,
        ):
            a.bind(path)
            receiver = _core._IoHandle(a.fileno())
            sender = _core._IoHandle(b.fileno())
            await sender.send_to(b"to str", path)
            await sender.send_to(b"to bytes", path.encode())
            got = [await receiver.recv_from(64), await receiver.recv_from(64)]
            receiver.close()
            sender.close()
            return got

    with directory:
        got = run(main())
    # The sender is unbound: its address is ''.
    assert got == [(b"to str", ""), (b"to bytes", "")]


@pytest.mark.skipif(sys.platform != "linux", reason="abstract Unix names are Linux-only")
def test_unix_datagrams_to_an_abstract_name() -> None:
    async def main() -> bytes:
        name = b"\0porto-test-%d" % id(main)
        with (
            socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as a,
            socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as b,
        ):
            a.bind(name)
            receiver = _core._IoHandle(a.fileno())
            sender = _core._IoHandle(b.fileno())
            await sender.send_to(b"abstract", name)
            data, _ = await receiver.recv_from(64)
            receiver.close()
            sender.close()
            return data

    assert run(main()) == b"abstract"


def test_a_unix_path_that_is_too_long_says_what_to_do() -> None:
    async def main() -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as a:
            handle = _core._IoHandle(a.fileno())
            with pytest.raises(ValueError, match="not a valid Unix socket address"):
                handle.send_to(b"x", b"/" + b"x" * 200)
            handle.close()

    run(main())


def test_a_connect_address_must_be_numeric() -> None:
    async def main() -> None:
        with socket.socket() as client:
            handle = _core._IoHandle(client.fileno())
            with pytest.raises(ValueError, match="not a numeric IP address"):
                handle.connect(("localhost", 80))
            with pytest.raises(TypeError, match=r"\(host, port\)"):
                handle.connect(80)
            handle.close()

    run(main())


def test_recv_from_and_send_to_over_udp() -> None:
    async def main() -> tuple[bytes, Any, Any]:
        with (
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as a,
            socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as b,
        ):
            a.bind(("127.0.0.1", 0))
            b.bind(("127.0.0.1", 0))
            receiver = _core._IoHandle(a.fileno())
            sender = _core._IoHandle(b.fileno())
            received: list[Any] = []

            async def receive() -> None:
                received.append(await receiver.recv_from(1024))

            async with porto.scope() as s:
                s.spawn(receive())
                await porto.checkpoint()
                await sender.send_to(b"datagram", a.getsockname())
            receiver.close()
            sender.close()
            data, address = received[0]
            return data, address, b.getsockname()

    data, address, expected = run(main())
    assert data == b"datagram"
    assert address == expected


def test_wait_readable_and_wait_writable() -> None:
    async def main() -> None:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())
            assert type(handle.wait_readable()) is _core._Trap
            await handle.wait_writable()

            async def later() -> None:
                await porto.sleep(0.01)
                left.send(b"!")

            async with porto.scope() as s:
                s.spawn(later())
                await handle.wait_readable()
            # Readiness only: the byte is still there.
            assert await handle.read(8) == b"!"
            handle.close()

    run(main())


def test_closing_a_handle_fails_its_pending_read_with_ebadf() -> None:
    async def main() -> None:
        left, right = socketpair()
        with left, right:
            handle = _core._IoHandle(right.fileno())

            async def close_later() -> None:
                await porto.sleep(0.01)
                handle.close()

            async with porto.scope() as s:
                s.spawn(close_later())
                with pytest.raises(OSError, match="was closed") as caught:
                    await handle.read(8)
            assert caught.value.errno == 9
            assert handle.closed
            with pytest.raises(OSError, match="was closed"):
                handle.read(8)
            handle.close()

    run(main())


def test_end_of_stream_reads_empty_bytes() -> None:
    async def main() -> bytes:
        left, right = socketpair()
        with right:
            handle = _core._IoHandle(right.fileno())
            left.close()
            data = await handle.read(8)
            handle.close()
            return data

    assert run(main()) == b""


def test_an_io_handle_needs_a_running_runtime() -> None:
    left, right = socket.socketpair()
    with left, right, pytest.raises(RuntimeError, match="inside a running porto runtime"):
        _core._IoHandle(right.fileno())


def test_an_io_handle_needs_an_open_socket() -> None:
    async def main() -> None:
        with pytest.raises(OSError, match="Bad file descriptor") as caught:
            _core._IoHandle(1_000_000)
        assert caught.value.errno == 9
        with pytest.raises(ValueError, match="not an open socket"):
            _core._IoHandle(-1)

    run(main())
