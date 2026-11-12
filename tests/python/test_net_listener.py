"""``porto.net.listen_tcp`` and ``TcpListener`` (17-networking-tls-dns.md §1, §1.2) over
loopback. Registering raw sockets needs the AFD backend on Windows, which is not written yet,
so these tests run on Unix."""

import contextlib
import errno
import logging
import os
import socket
import struct
import sys
import time
from collections.abc import Coroutine, Iterator
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


def has_ipv6_loopback() -> bool:
    if not socket.has_ipv6:
        return False
    try:
        with socket.socket(socket.AF_INET6) as probe:
            probe.bind(("::1", 0))
    except OSError:
        return False
    return True


def test_port_zero_reports_the_bound_port() -> None:
    async def main() -> tuple[int, tuple[Any, ...], int]:
        async with await net.listen_tcp(LOOPBACK) as listener:
            with socket.create_connection((LOOPBACK, listener.port)):
                stream = await listener.accept()
                async with stream:
                    return listener.port, listener.sockname, stream.sockname[1]

    port, sockname, accepted_port = run(main())
    assert port != 0
    assert sockname == (LOOPBACK, port)
    assert accepted_port == port


def test_an_explicit_port_is_used() -> None:
    with socket.socket() as probe:
        probe.bind((LOOPBACK, 0))
        free_port = probe.getsockname()[1]

    async def main() -> int:
        async with await net.listen_tcp(LOOPBACK, free_port) as listener:
            return listener.port

    assert run(main()) == free_port


def test_a_taken_port_is_an_address_in_use_error() -> None:
    async def main() -> None:
        async with await net.listen_tcp(LOOPBACK) as first:
            with pytest.raises(OSError, match="in use") as caught:
                await net.listen_tcp(LOOPBACK, first.port)
            assert caught.value.errno == errno.EADDRINUSE

    run(main())


def test_accept_returns_streams_in_connection_order_with_the_peers_address() -> None:
    async def main() -> list[tuple[bytes, tuple[Any, ...], tuple[Any, ...]]]:
        results: list[tuple[bytes, tuple[Any, ...], tuple[Any, ...]]] = []
        async with await net.listen_tcp(LOOPBACK) as listener:
            clients = [socket.create_connection((LOOPBACK, listener.port)) for _ in range(3)]
            try:
                for index, client in enumerate(clients):
                    client.sendall(b"hello %d" % index)
                    stream = await listener.accept()
                    async with stream:
                        data = await stream.read(16)
                        results.append((data, stream.peername, client.getsockname()))
            finally:
                for client in clients:
                    client.close()
        return results

    for index, (data, peer, client_name) in enumerate(run(main())):
        assert data == b"hello %d" % index
        assert peer == client_name


def test_accept_waits_for_a_connection() -> None:
    async def main() -> bytes:
        async with await net.listen_tcp(LOOPBACK) as listener:

            async def connect_later() -> None:
                await porto.sleep(0.02)
                client = await net._connect_tcp_numeric(LOOPBACK, listener.port)  # pyright: ignore[reportPrivateUsage]
                async with client:
                    await client.write(b"late")
                    await porto.sleep(0.05)

            async with porto.scope() as s:
                s.spawn(connect_later())
                async with await listener.accept() as stream:
                    return await stream.read(8)

    assert run(main()) == b"late"


def test_cancelling_an_accept_loses_no_connection() -> None:
    async def main() -> bytes:
        async with await net.listen_tcp(LOOPBACK) as listener:
            async with porto.move_on_after(0.02):
                await listener.accept()
            with socket.create_connection((LOOPBACK, listener.port)) as client:
                client.sendall(b"queued")
                async with await listener.accept() as stream:
                    return await stream.read(8)

    assert run(main()) == b"queued"


def test_async_for_accepts_until_the_listener_is_closed() -> None:
    async def main() -> list[bytes]:
        listener = await net.listen_tcp(LOOPBACK)
        seen: list[bytes] = []

        async def serve() -> None:
            async for stream in listener:
                async with stream:
                    seen.append(await stream.read(8))

        async with porto.scope() as s:
            s.spawn(serve())
            for word in (b"one", b"two"):
                with socket.create_connection((LOOPBACK, listener.port)) as client:
                    client.sendall(word)
                    while len(seen) < (1 if word == b"one" else 2):
                        await porto.sleep(0.005)
            await listener.aclose()  # ends the loop: the scope exits
        return seen

    assert run(main()) == [b"one", b"two"]


def test_closing_the_listener_fails_a_waiting_accept_and_every_later_one() -> None:
    async def main() -> None:
        listener = await net.listen_tcp(LOOPBACK)

        async def close_later() -> None:
            await porto.sleep(0.01)
            await listener.aclose()

        async with porto.scope() as s:
            s.spawn(close_later())
            with pytest.raises(porto.ClosedResourceError) as caught:
                await listener.accept()
        assert caught.value.errno == errno.EBADF
        with pytest.raises(porto.ClosedResourceError):
            await listener.accept()
        with pytest.raises(porto.ClosedResourceError):
            listener.fileno()
        await listener.aclose()  # again: nothing happens
        assert listener.port != 0, "the port it had is still known"

    run(main())


@pytest.mark.skipif(not has_ipv6_loopback(), reason="this host has no IPv6 loopback")
def test_dual_stack_accepts_ipv4_and_ipv6() -> None:
    async def main() -> list[tuple[Any, ...]]:
        peers: list[tuple[Any, ...]] = []
        async with await net.listen_tcp(None, 0) as listener:
            assert listener.sockname[0] == "::"
            for family, host in ((socket.AF_INET, LOOPBACK), (socket.AF_INET6, "::1")):
                with socket.socket(family) as client:
                    client.connect((host, listener.port))
                    async with await listener.accept() as stream:
                        peers.append(stream.peername)
        return peers

    v4, v6 = run(main())
    assert v4[0] == f"::ffff:{LOOPBACK}", "an IPv4 peer, as an IPv4-mapped IPv6 address"
    assert v6[0] == "::1"


@pytest.mark.skipif(not has_ipv6_loopback(), reason="this host has no IPv6 loopback")
def test_without_dual_stack_an_ipv6_listener_refuses_ipv4() -> None:
    async def main() -> None:
        async with await net.listen_tcp("::", 0, dual_stack=False) as listener:
            with socket.socket(socket.AF_INET6) as client:
                client.connect(("::1", listener.port))
                async with await listener.accept():
                    pass
            with pytest.raises(ConnectionRefusedError), socket.socket() as v4:
                v4.connect((LOOPBACK, listener.port))

    run(main())


def test_without_dual_stack_every_interface_means_ipv4() -> None:
    async def main() -> str:
        async with await net.listen_tcp(None, 0, dual_stack=False) as listener:
            with socket.create_connection((LOOPBACK, listener.port)):
                async with await listener.accept() as stream:
                    return str(listener.sockname[0]) + " " + str(stream.peername[0])

    assert run(main()) == f"0.0.0.0 {LOOPBACK}"


@pytest.mark.skipif(sys.platform == "win32", reason="SO_REUSEPORT is not on Windows")
def test_reuse_port_lets_two_listeners_share_a_port() -> None:
    async def main() -> tuple[int, int]:
        async with await net.listen_tcp(LOOPBACK, reuse_port=True) as first:
            async with await net.listen_tcp(LOOPBACK, first.port, reuse_port=True) as second:
                return first.port, second.port
            return 0, 0

    first, second = run(main())
    assert first == second != 0


def test_accepted_and_listening_sockets_are_close_on_exec() -> None:
    import fcntl

    def close_on_exec(fd: int) -> bool:
        return bool(fcntl.fcntl(fd, fcntl.F_GETFD) & fcntl.FD_CLOEXEC)

    async def main() -> tuple[bool, bool, bool, bool]:
        async with await net.listen_tcp(LOOPBACK) as listener:
            client = await net._connect_tcp_numeric(LOOPBACK, listener.port)  # pyright: ignore[reportPrivateUsage]
            async with client, await listener.accept() as accepted:
                return (
                    close_on_exec(listener.fileno()),
                    close_on_exec(accepted.fileno()),
                    close_on_exec(client.fileno()),
                    os.get_blocking(accepted.fileno()),
                )

    listening, accepted, connecting, blocking = run(main())
    assert listening
    assert accepted, "accept4(SOCK_CLOEXEC) or its fallback set FD_CLOEXEC"
    assert connecting
    assert not blocking, "accepted sockets are nonblocking"


def test_tls_is_not_implemented_yet() -> None:
    async def main() -> None:
        with pytest.raises(NotImplementedError, match="TLS is not implemented yet"):
            await net.listen_tcp(LOOPBACK, tls=object())

    run(main())


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"host": "localhost"}, "numeric IP address"),
        ({"host": "999.1.1.1"}, "numeric IP address"),
        ({"port": 70000}, "port must be an int in 0..=65535"),
        ({"port": -1}, "port must be an int in 0..=65535"),
        ({"backlog": -1}, "backlog must be an int"),
    ],
)
def test_bad_arguments_say_what_to_pass(kwargs: dict[str, Any], message: str) -> None:
    async def main() -> None:
        with pytest.raises(ValueError, match=message):
            await net.listen_tcp(**kwargs)

    run(main())


def test_listening_needs_a_running_runtime() -> None:
    async def main() -> None:
        await net.listen_tcp(LOOPBACK)

    coro = main()
    # Awaited by hand, outside porto.run: the socket is made, then refused for want of a run.
    with pytest.raises(RuntimeError, match="inside a running porto runtime"):
        coro.send(None)


def test_a_cancelled_task_opens_no_listener() -> None:
    async def main() -> None:
        async with porto.move_on_after(0) as scope:
            await porto.checkpoint()
            await net.listen_tcp(LOOPBACK)
            raise AssertionError("unreachable")
        assert scope.timed_out

    run(main())


# Running out of file descriptors (17 §1.2)


@pytest.mark.skipif(sys.platform == "win32", reason="RLIMIT_NOFILE")
def test_accept_backs_off_when_descriptors_run_out_and_recovers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """With no descriptor left, accept waits and retries every 100 ms instead of failing or
    spinning (the CPU stays idle), warns once, and works again as soon as a descriptor is free.

    Linux leaves the connection that hit ``EMFILE`` queued, so the retry accepts it; macOS
    drops it, so there the connection made after the recovery is the one accepted."""
    logging.getLogger("porto.net")  # imported before the descriptors are gone

    async def main() -> tuple[float, float, bytes]:
        listener = await net.listen_tcp(LOOPBACK)
        first, second = socket.socket(), socket.socket()  # made while descriptors are left
        first.connect((LOOPBACK, listener.port))  # queued by the kernel
        first.sendall(b"first")
        hoard: list[int] = []

        async def accept_one() -> net.TcpStream:
            return await listener.accept()

        try:
            while True:
                try:
                    hoard.append(os.dup(0))
                except OSError as error:
                    if error.errno not in (errno.EMFILE, errno.ENFILE):
                        raise
                    break
            async with porto.scope() as s:
                accepting = s.spawn(accept_one())
                cpu_before = time.process_time()
                wall_before = time.monotonic()
                await porto.sleep(0.6)  # six backoff periods
                cpu = time.process_time() - cpu_before
                wall = time.monotonic() - wall_before
                assert not accepting.done(), "accept does not fail: it waits for a descriptor"
                for fd in hoard[:3]:
                    os.close(fd)
                del hoard[:3]
                second.connect((LOOPBACK, listener.port))
                second.sendall(b"second")
                async with await accepting as stream:
                    data = await stream.read(16)
                return cpu, wall, data
        finally:
            for fd in hoard:
                os.close(fd)
            first.close()
            second.close()
            await listener.aclose()

    with caplog.at_level(logging.WARNING, logger="porto.net"):
        cpu, wall, data = run(main())
    assert cpu < 0.25 * wall, f"accept busy-looped: {cpu:.2f}s of CPU in {wall:.2f}s"
    records = [r for r in caplog.records if r.name == "porto.net"]
    assert len(records) == 1, "one warning, however many retries"
    assert "ran out of file descriptors" in records[0].getMessage()
    assert "ulimit -n" in records[0].getMessage()
    if sys.platform == "linux":
        assert data == b"first", "the connection that hit EMFILE stayed queued and was accepted"
    else:
        assert data in (b"first", b"second")


@contextlib.contextmanager
def no_descriptors_left() -> Iterator[None]:
    """Uses up every file descriptor this process may open, until the block ends."""
    hoard: list[int] = []
    try:
        while True:
            try:
                hoard.append(os.dup(0))
            except OSError as error:
                if error.errno not in (errno.EMFILE, errno.ENFILE):
                    raise
                break
        yield
    finally:
        for fd in hoard:
            os.close(fd)


def test_closing_a_listener_ends_an_accept_that_is_backing_off() -> None:
    """aclose() wakes the accept at once: it does not sit out the rest of its 100 ms pause (a
    pause only happens where the connection that hit EMFILE stays queued: Linux)."""

    async def main() -> tuple[bool, bool]:
        logging.getLogger("porto.net")
        listener = await net.listen_tcp(LOOPBACK)
        client = socket.socket()
        client.connect((LOOPBACK, listener.port))  # queued by the kernel
        errors: list[BaseException] = []

        async def accept_one() -> None:
            try:
                await listener.accept()
            except porto.ClosedResourceError as error:
                errors.append(error)

        try:
            with no_descriptors_left():
                async with porto.scope() as s:
                    accepting = s.spawn(accept_one())
                    await porto.sleep(0.03)  # inside the first pause
                    assert not accepting.done()
                    await listener.aclose()
                    await porto.sleep(0.03)  # well before the pause would have ended
                    ended = accepting.done()
        finally:
            client.close()
        return ended, bool(errors)

    ended, raised = run(main())
    assert ended, "the accept was still waiting out its backoff after the listener closed"
    assert raised


@pytest.mark.skipif(
    sys.platform != "linux", reason="needs the connection that hit EMFILE to stay queued (Linux)"
)
def test_a_hang_up_during_the_backoff_does_not_spin_the_lane() -> None:
    """A dup of the listener's descriptor shut down while accept backs off sets closed bits on
    the registration that never clear: waiting for the close must not resolve on them, or the
    lane would spin inside one poll and no timer or cancellation could run again."""

    async def main() -> tuple[float, float, type[BaseException]]:
        logging.getLogger("porto.net")
        listener = await net.listen_tcp(LOOPBACK)
        client = socket.socket()
        client.connect((LOOPBACK, listener.port))  # queued by the kernel
        duplicate = socket.socket(fileno=os.dup(listener.fileno()))
        failure: list[BaseException] = []

        async def accept_one() -> None:
            try:
                await listener.accept()
            except OSError as error:
                failure.append(error)

        try:
            async with porto.scope() as s:
                accepting = s.spawn(accept_one())
                with no_descriptors_left():
                    await porto.sleep(0.03)  # backing off
                    duplicate.shutdown(socket.SHUT_RDWR)  # HUP on the listener's registration
                    cpu_before = time.process_time()
                    wall_before = time.monotonic()
                    await porto.sleep(0.4)  # four backoff periods: timers still run
                    cpu = time.process_time() - cpu_before
                    wall = time.monotonic() - wall_before
                    assert not accepting.done(), "still backing off for want of a descriptor"
                # Descriptors are back: the shut-down listener now fails its accept (EINVAL).
                await accepting
        finally:
            duplicate.close()
            client.close()
            await listener.aclose()
        return cpu, wall, type(failure[0])

    cpu, wall, failure = run(main())
    assert cpu < 0.25 * wall, f"the lane spun: {cpu:.2f}s of CPU in {wall:.2f}s"
    assert issubclass(failure, OSError)


def test_the_exhaustion_warning_is_rate_limited_per_run(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One warning per run however often accept retries; a later run warns again."""
    logging.getLogger("porto.net")

    async def one_run() -> None:
        listener = await net.listen_tcp(LOOPBACK)
        client = socket.socket()
        client.connect((LOOPBACK, listener.port))
        try:
            with no_descriptors_left():
                async with porto.move_on_after(0.25):
                    await listener.accept()  # tries, warns, backs off, tries again, ...
        finally:
            client.close()
            await listener.aclose()

    with caplog.at_level(logging.WARNING, logger="porto.net"):
        run(one_run())
        first = len([r for r in caplog.records if r.name == "porto.net"])
        run(one_run())
        both = len([r for r in caplog.records if r.name == "porto.net"])
    assert first == 1, "one warning in the first run"
    assert both == 2, "the second run is not silenced by the first run's limit"


def test_accept_goes_on_past_clients_that_reset_before_it_accepted_them() -> None:
    """A connection reset before accept (ECONNABORTED where the OS reports it, an accepted
    socket that fails on first read elsewhere) never stops the listener."""

    async def main() -> bytes:
        async with await net.listen_tcp(LOOPBACK) as listener:
            for _ in range(5):
                bad = socket.create_connection((LOOPBACK, listener.port))
                bad.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                bad.close()
            good = socket.create_connection((LOOPBACK, listener.port))
            good.sendall(b"good")
            try:
                async with porto.move_on_after(5):
                    async for stream in listener:
                        async with stream:
                            try:
                                data = await stream.read(8)
                            except ConnectionResetError:
                                continue
                            if data == b"good":
                                return data
            finally:
                good.close()
        return b""

    assert run(main()) == b"good"


def test_several_tasks_may_accept_from_one_listener() -> None:
    async def main() -> list[bytes]:
        async with await net.listen_tcp(LOOPBACK) as listener:
            results: list[bytes] = []

            async def acceptor() -> None:
                async with await listener.accept() as stream:
                    results.append(await stream.read(8))

            async with porto.scope() as s:
                s.spawn(acceptor())
                s.spawn(acceptor())
                await porto.sleep(0.01)
                for word in (b"one", b"two"):
                    with socket.create_connection((LOOPBACK, listener.port)) as client:
                        client.sendall(word)
                        await porto.sleep(0.02)
            return sorted(results)

    assert run(main()) == [b"one", b"two"]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"port": 2**70}, r"port must be an int in 0\.\.=65535"),
        ({"port": -(2**70)}, r"port must be an int in 0\.\.=65535"),
        ({"backlog": 2**70}, r"backlog must be an int in 0\.\.=2147483647"),
        ({"backlog": 2**31}, r"backlog must be an int in 0\.\.=2147483647"),
    ],
)
def test_huge_integers_are_value_errors_not_overflow_errors(
    kwargs: dict[str, Any], message: str
) -> None:
    async def main() -> None:
        with pytest.raises(ValueError, match=message):
            await net.listen_tcp(**kwargs)

    run(main())
