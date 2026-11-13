"""``porto.net.connect_tcp`` (17-networking-tls-dns.md §1): resolution, Happy Eyeballs v2 (RFC
8305) and what each attempt does with its socket.

A hanging address cannot be made portably (a connect to an unroutable address is refused at once
by some networks and left to time out by others), so the tests that need one wrap a *real*
socket's stream in a ``SlowStream`` whose connect never completes: the descriptor is real, so
the descriptor counts below prove that every attempt closes what it opened. Registering raw
sockets needs the AFD backend on Windows, which is not written yet, so these tests run on Unix.
Timing assertions have generous limits.
"""

import errno
import os
import socket
import sys
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import pytest

import porto
from porto import (  # pyright: ignore[reportPrivateUsage]
    _connect,  # pyright: ignore[reportPrivateUsage]
    _core,
    _dns,
    net,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="registering raw sockets needs the AFD backend (Windows)"
)

LOOPBACK = "127.0.0.1"


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    return porto.run(coro)


def open_descriptors() -> int:
    return len(os.listdir("/dev/fd"))


async def refused_port() -> int:
    """A loopback port nothing listens on."""
    listener = await net.listen_tcp(LOOPBACK)
    port = listener.port
    await listener.aclose()
    return port


class SlowStream:
    """A real stream whose connect never completes, and that records being closed."""

    def __init__(self, port: int) -> None:
        self.real = _core.TcpStream._connecting((LOOPBACK, port))  # pyright: ignore[reportPrivateUsage]
        self.closed = False

    def _finish_connect(self) -> Awaitable[None]:
        return porto.sleep_forever()  # type: ignore[return-value]

    async def aclose(self) -> None:
        self.closed = True
        await self.real.aclose()


class Setup:
    """Names for the tests: ``lookup`` answers with whatever addresses a test sets, and
    addresses in ``slow`` start as ``SlowStream``s."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.answers: list[net.SockAddr] = []
        self.slow: set[int] = set()
        self.started: list[tuple[int, float]] = []
        self.slow_streams: list[SlowStream] = []
        monkeypatch.setattr(_dns, "_lookup", lambda host, port, family: list(self.answers))
        real_start = _connect._start  # pyright: ignore[reportPrivateUsage]

        def start(address: net.SockAddr, local: list[net.SockAddr]) -> Any:
            self.started.append((address[1], porto.current_time()))
            if address[1] in self.slow:
                stream = SlowStream(address[1])
                self.slow_streams.append(stream)
                return stream
            return real_start(address, local)

        monkeypatch.setattr(_connect, "_start", start)


@pytest.fixture
def setup(monkeypatch: pytest.MonkeyPatch) -> Setup:
    return Setup(monkeypatch)


def test_connects_to_a_numeric_address_and_to_a_name() -> None:
    async def main() -> tuple[Any, Any]:
        async with (
            await net.listen_tcp(LOOPBACK) as listener,
            await net.connect_tcp(LOOPBACK, listener.port) as by_address,
            await net.connect_tcp("localhost", listener.port) as by_name,
        ):
            return by_address.peername[:2], by_name.peername[:2]

    by_address, by_name = run(main())
    assert by_address == by_name == (LOOPBACK, by_address[1])


def test_the_connection_works_both_ways() -> None:
    async def main() -> bytes:
        async with await net.listen_tcp(LOOPBACK) as listener:
            client = await net.connect_tcp(LOOPBACK, listener.port)
            async with client, await listener.accept() as server:
                await client.write(b"ping")
                assert await server.read_exact(4) == b"ping"
                await server.write(b"pong")
                return await client.read_exact(4)

    assert run(main()) == b"pong"


def test_an_unreachable_first_address_falls_through_to_the_second_after_the_delay(
    setup: Setup,
) -> None:
    async def main() -> tuple[float, Any]:
        async with await net.listen_tcp(LOOPBACK) as listener:
            setup.slow = {1}
            setup.answers = [(LOOPBACK, 1), (LOOPBACK, listener.port)]
            begun = porto.current_time()
            async with await net.connect_tcp("two.test", 9, happy_eyeballs_delay=0.2) as stream:
                return porto.current_time() - begun, stream.peername[1] == listener.port

    elapsed, reached_second = run(main())
    assert reached_second
    assert 0.2 <= elapsed < 5
    first, second = setup.started
    assert first[0] == 1
    assert second[1] - first[1] >= 0.2 - 0.01
    assert [stream.closed for stream in setup.slow_streams] == [True], "the loser was closed"


def test_a_delay_of_zero_starts_every_attempt_at_once(setup: Setup) -> None:
    async def main() -> None:
        async with await net.listen_tcp(LOOPBACK) as listener:
            setup.slow = {1, 2}
            setup.answers = [(LOOPBACK, 1), (LOOPBACK, 2), (LOOPBACK, listener.port)]
            async with await net.connect_tcp("three.test", 9, happy_eyeballs_delay=0):
                pass

    run(main())
    times = [at for _, at in setup.started]
    assert len(times) == 3
    assert max(times) - min(times) < 1
    assert all(stream.closed for stream in setup.slow_streams)


def test_a_failed_attempt_starts_the_next_at_once_not_after_the_delay(setup: Setup) -> None:
    async def main() -> float:
        async with await net.listen_tcp(LOOPBACK) as listener:
            setup.answers = [(LOOPBACK, await refused_port()), (LOOPBACK, listener.port)]
            begun = porto.current_time()
            async with await net.connect_tcp("two.test", 9, happy_eyeballs_delay=30):
                return porto.current_time() - begun

    assert run(main()) < 10


def test_the_losers_are_closed_and_no_descriptor_leaks(setup: Setup) -> None:
    async def main() -> tuple[int, int, int]:
        async with await net.listen_tcp(LOOPBACK) as listener:
            setup.slow = {1, 2, 3}
            setup.answers = [(LOOPBACK, n) for n in (1, 2, 3)] + [(LOOPBACK, listener.port)]
            before = open_descriptors()
            stream = await net.connect_tcp("many.test", 9, happy_eyeballs_delay=0)
            during = open_descriptors()
            await stream.aclose()
            return before, during, open_descriptors()

    before, during, after = run(main())
    assert during == before + 1, "only the winner stays open"
    assert after == before
    assert len(setup.slow_streams) == 3
    assert all(stream.closed for stream in setup.slow_streams)


def test_the_enclosing_deadline_cancels_cleanly_with_no_descriptor_leak(setup: Setup) -> None:
    async def main() -> tuple[bool, float, int, int]:
        setup.slow = {1, 2}
        setup.answers = [(LOOPBACK, 1), (LOOPBACK, 2)]
        before = open_descriptors()
        begun = porto.current_time()
        with porto.move_on_after(0.3) as deadline:
            await net.connect_tcp("slow.test", 9, happy_eyeballs_delay=0.05)
            raise AssertionError("connect_tcp returned although every attempt hangs")
        elapsed = porto.current_time() - begun
        await porto.sleep(0.05)
        return deadline.cancelled_caught, elapsed, before, open_descriptors()

    caught, elapsed, before, after = run(main())
    assert caught
    assert 0.3 <= elapsed < 5
    assert after == before
    assert len(setup.slow_streams) == 2
    assert all(stream.closed for stream in setup.slow_streams)


def test_a_timeout_comes_only_from_the_enclosing_scope(setup: Setup) -> None:
    async def main() -> None:
        setup.slow = {1}
        setup.answers = [(LOOPBACK, 1)]
        with porto.timeout(0.1):
            await net.connect_tcp("slow.test", 9)

    with pytest.raises(TimeoutError):
        run(main())
    assert all(stream.closed for stream in setup.slow_streams)


def test_a_refused_connection_is_a_connection_refused_error() -> None:
    async def main() -> None:
        with pytest.raises(ConnectionRefusedError) as caught:
            await net.connect_tcp(LOOPBACK, await refused_port())
        assert caught.value.errno == errno.ECONNREFUSED

    run(main())


def test_when_every_address_is_refused_the_first_error_is_raised_with_the_others_in_notes(
    setup: Setup,
) -> None:
    async def main() -> tuple[int, int]:
        first, second = await refused_port(), await refused_port()
        setup.answers = [(LOOPBACK, first), (LOOPBACK, second)]
        before = open_descriptors()
        with pytest.raises(ConnectionRefusedError) as caught:
            await net.connect_tcp("two.test", 9)
        assert caught.value.errno == errno.ECONNREFUSED
        notes = "\n".join(caught.value.__notes__)
        assert f"127.0.0.1:{second}" in notes
        assert "ConnectionRefusedError" in notes
        assert open_descriptors() == before
        return first, second

    run(main())


def test_a_name_that_does_not_resolve_is_a_gaierror(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing(host: str, port: int, family: int) -> list[net.SockAddr]:
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    monkeypatch.setattr(_dns, "_lookup", failing)

    async def main() -> None:
        await net.connect_tcp("nowhere.test", 80)

    with pytest.raises(socket.gaierror):
        run(main())


def test_local_addr_binds_the_socket_before_connecting() -> None:
    async def main() -> tuple[Any, Any]:
        async with await net.listen_tcp(LOOPBACK) as listener:
            probe = socket.socket()
            probe.bind((LOOPBACK, 0))
            wanted = probe.getsockname()[1]
            probe.close()
            async with (
                await net.connect_tcp(
                    LOOPBACK, listener.port, local_addr=(LOOPBACK, wanted)
                ) as stream,
                await listener.accept() as server,
            ):
                return stream.sockname[:2], server.peername[:2]

    local, seen_by_server = run(main())
    assert local == seen_by_server
    assert local[0] == LOOPBACK


def test_a_local_addr_of_another_family_fails_the_attempt_and_says_what_to_do() -> None:
    async def main() -> None:
        with pytest.raises(OSError, match="local_addr has no address") as caught:
            await net.connect_tcp("::1", 9, local_addr=(LOOPBACK, 0))
        assert caught.value.errno == errno.EAFNOSUPPORT

    run(main())


def test_tls_is_not_implemented_yet() -> None:
    async def main() -> None:
        await net.connect_tcp(LOOPBACK, 9, tls=True)

    with pytest.raises(NotImplementedError, match="TLS is not implemented"):
        run(main())


@pytest.mark.parametrize(
    ("kwargs", "error", "match"),
    [
        ({"port": 0}, ValueError, "port must be in 1..=65535"),
        ({"port": 70000}, ValueError, "port must be in 1..=65535"),
        ({"port": "80"}, TypeError, "port must be an int"),
        ({"server_hostname": "example.test"}, ValueError, "server_hostname"),
        ({"happy_eyeballs_delay": -1}, ValueError, "happy_eyeballs_delay"),
        ({"happy_eyeballs_delay": float("inf")}, ValueError, "happy_eyeballs_delay"),
        ({"happy_eyeballs_delay": float("nan")}, ValueError, "happy_eyeballs_delay"),
        ({"happy_eyeballs_delay": True}, ValueError, "happy_eyeballs_delay"),
        ({"local_addr": "0.0.0.0"}, ValueError, "local_addr must be a"),
        ({"local_addr": ("0.0.0.0",)}, ValueError, "local_addr must be a"),
        ({"host": b"127.0.0.1"}, TypeError, "host must be a str"),
        ({"host": ""}, ValueError, "host is empty"),
    ],
)
def test_a_wrong_argument_says_what_to_pass(
    kwargs: dict[str, Any], error: type[Exception], match: str
) -> None:
    arguments: dict[str, Any] = {"host": LOOPBACK, "port": 9}
    arguments.update(kwargs)

    async def main() -> None:
        await net.connect_tcp(**arguments)

    with pytest.raises(error, match=match):
        run(main())


def test_the_addresses_are_interleaved_by_family_starting_with_the_first() -> None:
    interleave: Callable[[list[net.SockAddr]], list[net.SockAddr]] = _connect._interleave  # pyright: ignore[reportPrivateUsage]
    a, b, c = ("10.0.0.1", 1), ("10.0.0.2", 1), ("10.0.0.3", 1)
    x, y = ("::1", 1, 0, 0), ("::2", 1, 0, 0)
    assert interleave([x, y, a, b, c]) == [x, a, y, b, c]
    assert interleave([a, b, c, x, y]) == [a, x, b, y, c]
    assert interleave([a]) == [a]
    assert interleave([a, b]) == [a, b]
