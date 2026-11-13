"""``porto.net.serve_tcp`` (17-networking-tls-dns.md §1): one scope per connection, ``started``,
``max_connections``, handler errors that do not stop the server, and cancelling it. Over loopback
with porto clients (``connect_tcp``); I/O needs the Unix backends.
"""

import errno
import functools
import logging
import os
import socket
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


async def connect(port: int) -> net.TcpStream:
    return await net.connect_tcp(LOOPBACK, port)


class Serving:
    """``async with Serving(handler, **options) as port``: ``serve_tcp`` runs in a scope of its
    own for the block and is cancelled at its end (it would run forever)."""

    def __init__(self, handler: Any, **options: Any) -> None:
        self._server = functools.partial(net.serve_tcp, handler, LOOPBACK, 0, **options)
        self._scope = porto.scope()

    async def __aenter__(self) -> int:
        await self._scope.__aenter__()
        return await self._scope.start(self._server)  # type: ignore[no-any-return]

    async def __aexit__(self, *exception: Any) -> None:
        self._scope.cancel()
        await self._scope.__aexit__(*exception)


async def echo(stream: net.TcpStream) -> None:
    while True:
        try:
            line = await stream.readline()
        except porto.IncompleteReadError:
            return
        await stream.write(line)


async def at_eof(stream: net.TcpStream, within: float = 2.0) -> bool:
    """Whether the stream reads its end (the server closed it) before `within` seconds."""
    async with porto.move_on_after(within) as scope:
        try:
            return await stream.read() == b""
        except ConnectionResetError:
            return True  # closed with unread data: the OS resets
    assert scope.timed_out
    return False


def test_start_returns_the_port_and_the_server_echoes_for_many_clients() -> None:
    async def main() -> list[bytes]:
        async with Serving(echo) as port:
            assert isinstance(port, int)
            assert port != 0

            async def client(number: int) -> bytes:
                async with await connect(port) as stream:
                    await stream.write(b"hello %d\n" % number)
                    return await stream.readline()

            async with porto.scope() as clients:
                tasks = [clients.spawn(client(n)) for n in range(20)]
            return [task.result() for task in tasks]

    assert run(main()) == [b"hello %d\n" % n for n in range(20)]


def test_started_is_set_with_the_port_the_listener_is_bound_to() -> None:
    async def main() -> tuple[int, tuple[str, int]]:
        async with Serving(echo) as port, await connect(port) as stream:
            return port, stream.peername  # type: ignore[return-value]

    port, peer = run(main())
    assert peer == (LOOPBACK, port)


def test_a_server_can_be_a_plain_task_when_the_port_is_known() -> None:
    async def main() -> bytes:
        async with await net.listen_tcp(LOOPBACK) as probe:
            port = probe.port  # a free port, handed over to the server below
        async with porto.scope() as s:
            s.spawn(functools.partial(net.serve_tcp, echo, LOOPBACK, port))
            await porto.sleep(0.01)
            async with await connect(port) as stream:
                await stream.write(b"plain\n")
                reply = await stream.readline()
            s.cancel()
            return reply

    assert run(main()) == b"plain\n"


def test_a_handler_that_returns_closes_its_connection() -> None:
    async def returning(stream: net.TcpStream) -> None:
        await stream.write(b"bye\n")

    async def main() -> bool:
        async with Serving(returning) as port, await connect(port) as stream:
            assert await stream.readline() == b"bye\n"
            return await at_eof(stream)

    assert run(main())


def test_a_failing_handler_is_logged_and_the_server_keeps_serving(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(stream: net.TcpStream) -> None:
        line = await stream.readline()
        if line == b"boom\n":
            raise RuntimeError("the handler is broken")
        await stream.write(line)

    async def main() -> tuple[bool, bytes]:
        async with Serving(handler) as port:
            async with await connect(port) as bad:
                await bad.write(b"boom\n")
                closed = await at_eof(bad)
            async with await connect(port) as good:
                await good.write(b"fine\n")
                return closed, await good.readline()

    with caplog.at_level(logging.ERROR, logger="porto.net"):
        closed, reply = run(main())
    assert closed
    assert reply == b"fine\n"
    [record] = [r for r in caplog.records if r.name == "porto.net"]
    assert record.levelno == logging.ERROR
    assert "serve_tcp's handler" in record.getMessage()
    assert "the connection was closed and the server keeps serving" in record.getMessage()
    assert record.exc_info is not None
    assert isinstance(record.exc_info[1], RuntimeError)


def test_a_child_task_of_a_handler_that_fails_fails_the_handler_not_the_server(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def handler(stream: net.TcpStream) -> None:
        async def child() -> None:
            raise ValueError("a child failed")

        async with porto.scope() as inner:  # a scope of the handler's own
            inner.spawn(child())
            await porto.sleep_forever()

    async def main() -> bool:
        async with Serving(handler) as port:
            async with await connect(port) as stream:
                first = await at_eof(stream)
            async with await connect(port) as stream:  # the server is alive
                return first and await at_eof(stream)

    with caplog.at_level(logging.ERROR, logger="porto.net"):
        assert run(main())
    assert sum("serve_tcp's handler" in r.getMessage() for r in caplog.records) == 2


def test_tasks_a_handler_spawns_end_with_its_connection() -> None:
    finished: list[str] = []

    async def handler(stream: net.TcpStream) -> None:
        async def helper() -> None:
            try:
                await porto.sleep_forever()
            finally:
                finished.append("helper")

        async with porto.scope() as inner:
            inner.spawn(helper())
            await stream.read()  # until the client closes
            inner.cancel()

    async def main() -> None:
        async with Serving(handler) as port:
            async with await connect(port):
                await porto.sleep(0.01)
            await porto.sleep(0.02)
            assert finished == ["helper"]

    run(main())


def test_max_connections_keeps_the_next_client_in_the_backlog_until_one_closes() -> None:
    active = 0
    peak = 0

    async def handler(stream: net.TcpStream) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        try:
            await echo(stream)
        finally:
            active -= 1

    async def main() -> tuple[bytes, int]:
        async with Serving(handler, max_connections=2) as port:
            first, second, third = [await connect(port) for _ in range(3)]  # all connect
            async with first, second, third:
                for stream in (first, second):
                    await stream.write(b"x\n")
                    assert await stream.readline() == b"x\n"
                await third.write(b"third\n")
                async with porto.move_on_after(0.05) as waiting:
                    await third.readline()  # not accepted yet: no echo
                assert waiting.timed_out
                assert active == 2
                await first.aclose()  # a slot frees up
                reply = await third.readline()
                return reply, peak

    assert run(main()) == (b"third\n", 2)


def test_a_slot_is_given_back_when_the_handler_fails() -> None:
    async def handler(stream: net.TcpStream) -> None:
        raise RuntimeError("fails at once")

    async def main() -> list[bool]:
        async with Serving(handler, max_connections=1) as port:
            results: list[bool] = []
            for _ in range(3):  # each needs the one slot
                async with await connect(port) as stream:
                    results.append(await at_eof(stream))
            return results

    assert run(main()) == [True, True, True]


def test_cancelling_the_scope_stops_the_server_and_closes_every_connection() -> None:
    async def main() -> tuple[list[bool], bool]:
        async with porto.scope() as s:
            port = await s.start(functools.partial(net.serve_tcp, echo, LOOPBACK, 0))
            streams = [await connect(port) for _ in range(3)]
            for stream in streams:
                await stream.write(b"hi\n")
                assert await stream.readline() == b"hi\n"
            s.cancel()
        # The scope exited (the server task and its handlers ended): the clients see it.
        closed = [await at_eof(stream) for stream in streams]
        refused = False
        try:
            await connect(port)
        except OSError:  # ConnectionRefusedError on Linux; macOS reports EINVAL (porto-szb.69)
            refused = True
        return closed, refused

    assert run(main()) == ([True, True, True], True)


def test_a_server_that_cannot_listen_fails_start_with_the_oserror() -> None:
    async def main() -> int | None:
        async with await net.listen_tcp(LOOPBACK) as taken, porto.scope() as s:
            with pytest.raises(OSError, match=r"in use|Address already") as caught:
                await s.start(functools.partial(net.serve_tcp, echo, LOOPBACK, taken.port))
            return caught.value.errno

    assert run(main()) == errno.EADDRINUSE


def test_tls_is_not_implemented_yet() -> None:
    async def main() -> None:
        with pytest.raises(NotImplementedError, match="TLS is not implemented yet"):
            await net.serve_tcp(echo, LOOPBACK, tls=object())

    run(main())


@pytest.mark.parametrize("bad", [0, -1, 2**31, True, "3", 2.5])
def test_max_connections_must_be_a_positive_int(bad: Any) -> None:
    async def main() -> None:
        with pytest.raises(ValueError, match="max_connections must be an int from 1 to"):
            await net.serve_tcp(echo, LOOPBACK, max_connections=bad)

    run(main())


def test_a_host_name_is_a_value_error_until_the_resolver_lands() -> None:
    async def main() -> None:
        with pytest.raises(ValueError, match="resolve"):
            await net.serve_tcp(echo, "localhost")

    run(main())


def nodelay_of(stream: net.TcpStream) -> int:
    """The stream's TCP_NODELAY, read through a duplicate of its descriptor."""
    with socket.socket(fileno=os.dup(stream.fileno())) as duplicate:
        return duplicate.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)


def test_accepted_streams_have_tcp_nodelay_like_asyncios() -> None:
    seen: list[int] = []

    async def handler(stream: net.TcpStream) -> None:
        seen.append(nodelay_of(stream))
        await stream.write(b"ok")

    async def main() -> int:
        async with await net.listen_tcp(LOOPBACK) as listener:
            client = await connect(listener.port)
            async with client, await listener.accept() as accepted:
                accepted_nodelay = nodelay_of(accepted)
                accepted.set_nodelay(False)  # it can still be turned off
                assert nodelay_of(accepted) == 0
        async with Serving(handler) as port, await connect(port) as stream:
            assert await stream.read_exact(2) == b"ok"
        return accepted_nodelay

    assert run(main()) != 0
    assert seen
    assert all(value != 0 for value in seen)


@pytest.mark.parametrize(
    "disconnect",
    [
        porto.IncompleteReadError(b"par", 10),
        ConnectionResetError(errno.ECONNRESET, "reset"),
        BrokenPipeError(errno.EPIPE, "broken pipe"),
        ExceptionGroup("children", [ConnectionResetError(errno.ECONNRESET, "reset")]),
        ExceptionGroup(
            "children",
            [BrokenPipeError(errno.EPIPE, "pipe"), porto.IncompleteReadError(b"", 4)],
        ),
    ],
    ids=lambda error: type(error).__name__,
)
def test_a_client_disconnect_that_escapes_a_handler_is_logged_at_debug_only(
    disconnect: BaseException, caplog: pytest.LogCaptureFixture
) -> None:
    async def handler(stream: net.TcpStream) -> None:
        raise disconnect

    async def main() -> bool:
        async with Serving(handler) as port, await connect(port) as stream:
            return await at_eof(stream)

    with caplog.at_level(logging.DEBUG, logger="porto.net"):
        assert run(main())
    records = [r for r in caplog.records if r.name == "porto.net"]
    assert [r.levelno for r in records] == [logging.DEBUG]
    assert "client disconnected" in records[0].getMessage()


@pytest.mark.parametrize(
    "bug",
    [
        ValueError("a bug"),
        porto.ClosedResourceError(errno.EBADF, "used a stream it closed itself"),
        ExceptionGroup("children", [ConnectionResetError(errno.ECONNRESET, "r"), ValueError("b")]),
    ],
    ids=["ValueError", "ClosedResourceError", "mixed-group"],
)
def test_a_real_handler_bug_is_still_an_error(
    bug: Exception, caplog: pytest.LogCaptureFixture
) -> None:
    async def handler(stream: net.TcpStream) -> None:
        raise bug

    async def main() -> None:
        async with Serving(handler) as port, await connect(port) as stream:
            await at_eof(stream)

    with caplog.at_level(logging.DEBUG, logger="porto.net"):
        run(main())
    [record] = [r for r in caplog.records if r.name == "porto.net"]
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None


def test_stopping_the_server_refuses_new_connections_while_handlers_still_wind_down() -> None:
    async def slow_to_stop(stream: net.TcpStream) -> None:
        try:
            await porto.sleep_forever()
        finally:
            with porto.shield():
                await porto.sleep(0.4)  # cleanup that takes a while

    async def main() -> tuple[bool, bool]:
        async with await net.listen_tcp(LOOPBACK) as probe:
            port = probe.port
        async with porto.scope() as outer:
            server = outer.spawn(net.serve_tcp, slow_to_stop, LOOPBACK, port)
            await porto.sleep(0.05)
            async with await connect(port):
                await porto.sleep(0.05)  # the handler is running
                server.cancel()
                await porto.sleep(0.1)  # the handler is still in its cleanup
                still_winding_down = not server.done()
                try:
                    await connect(port)
                    refused = False
                except OSError:  # ConnectionRefusedError on Linux, EINVAL on macOS (szb.150)
                    refused = True
            return still_winding_down, refused

    assert run(main()) == (True, True)
