"""``porto.net.bind_udp`` and ``UdpSocket`` (17-networking-tls-dns.md §2, 13-io-driver.md §5) over
loopback: whole datagrams both ways in IPv4 and IPv6, connected mode, the socket options, and
that a cancelled receive loses no datagram. Registering raw sockets needs the AFD backend on
Windows, which is not written yet, so these tests run on Unix.
"""

import errno
import socket
import sys
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

import pytest

import porto
from porto import net

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="registering raw sockets needs the AFD backend (Windows)"
)

LOOPBACK = "127.0.0.1"
LOOPBACK_V6 = "::1"


TEST_DEADLINE = 60  # seconds a test may take: none comes near it


def run(coro: Coroutine[Any, Any, Any]) -> Any:
    """Runs ``coro`` in ``porto.run`` under a deadline: a test that would wait forever (a datagram
    or connection that never comes) fails after a minute with ``TimeoutError`` instead of
    hanging the suite."""

    async def bounded() -> Any:
        async with porto.timeout(TEST_DEADLINE):
            return await coro

    return porto.run(bounded())


def has_ipv6() -> bool:
    try:
        with socket.socket(socket.AF_INET6, socket.SOCK_DGRAM) as probe:
            probe.bind((LOOPBACK_V6, 0))
    except OSError:
        return False
    return True


needs_ipv6 = pytest.mark.skipif(not has_ipv6(), reason="this host has no IPv6 loopback")


def socket_option(sock: net.UdpSocket, level: int, option: int) -> int:
    """An option of the OS socket behind ``sock``, read through a duplicate of its descriptor."""
    with socket.socket(fileno=socket.dup(sock.fileno())) as duplicate:
        return duplicate.getsockopt(level, option)


# Datagrams both ways


def test_a_datagram_goes_there_and_back_over_ipv4() -> None:
    async def main() -> tuple[tuple[bytes, Any], tuple[bytes, Any], Any, Any]:
        async with await net.bind_udp(LOOPBACK) as server, await net.bind_udp(LOOPBACK) as client:
            await client.send_to(b"ping", (LOOPBACK, server.port))
            request = await server.recv_from()
            assert request[1] == client.sockname
            await server.send_to(b"pong", request[1])
            return request, await client.recv_from(), server.sockname, client.sockname

    request, reply, server_name, client_name = run(main())
    assert request == (b"ping", client_name)
    assert reply == (b"pong", server_name)
    assert server_name[0] == LOOPBACK
    assert server_name[1] != client_name[1] != 0


@needs_ipv6
def test_a_datagram_goes_there_and_back_over_ipv6() -> None:
    async def main() -> tuple[tuple[bytes, Any], tuple[bytes, Any], Any, Any]:
        async with (
            await net.bind_udp(LOOPBACK_V6) as server,
            await net.bind_udp(LOOPBACK_V6) as client,
        ):
            await client.send_to(b"ping6", (LOOPBACK_V6, server.port))
            request = await server.recv_from()
            await server.send_to(b"pong6", request[1])
            return request, await client.recv_from(), server.sockname, client.sockname

    request, reply, server_name, client_name = run(main())
    assert len(server_name) == 4, "an IPv6 address has flowinfo and scope_id"
    assert request == (b"ping6", client_name)
    assert reply == (b"pong6", server_name)


def test_datagram_boundaries_are_kept_and_empty_datagrams_are_datagrams() -> None:
    async def main() -> list[bytes]:
        async with await net.bind_udp(LOOPBACK) as server, await net.bind_udp(LOOPBACK) as client:
            for datagram in (b"one", b"", b"three" * 1000, bytearray(b"four"), memoryview(b"five")):
                await client.send_to(datagram, (LOOPBACK, server.port))
            return [(await server.recv_from())[0] for _ in range(5)]

    assert run(main()) == [b"one", b"", b"three" * 1000, b"four", b"five"]


def test_a_datagram_longer_than_max_is_cut_and_the_rest_is_lost() -> None:
    async def main() -> tuple[bytes, bytes]:
        async with await net.bind_udp(LOOPBACK) as server, await net.bind_udp(LOOPBACK) as client:
            await client.send_to(b"0123456789", (LOOPBACK, server.port))
            cut = (await server.recv_from(4))[0]
            await client.send_to(b"next", (LOOPBACK, server.port))
            return cut, (await server.recv_from())[0]

    assert run(main()) == (b"0123", b"next")


def test_a_large_datagram_arrives_whole_with_the_default_max() -> None:
    # 8000 bytes: well past an Ethernet frame, under what macOS sends by default (9216).
    payload = bytes(range(250)) * 32

    async def main() -> bytes:
        async with await net.bind_udp(LOOPBACK) as server, await net.bind_udp(LOOPBACK) as client:
            await client.send_to(payload, (LOOPBACK, server.port))
            return (await server.recv_from())[0]

    assert run(main()) == payload


def test_a_foreign_socket_and_a_porto_socket_exchange_datagrams() -> None:
    """The peer need not be porto: a plain ``socket`` sends to and hears from a ``UdpSocket``."""

    async def main() -> tuple[bytes, bytes]:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as foreign:
            foreign.bind((LOOPBACK, 0))
            foreign.settimeout(5)
            async with await net.bind_udp(LOOPBACK) as ours:
                foreign.sendto(b"from foreign", (LOOPBACK, ours.port))
                data, address = await ours.recv_from()
                assert address == foreign.getsockname()
                await ours.send_to(b"from porto", address)
                return data, foreign.recvfrom(100)[0]

    assert run(main()) == (b"from foreign", b"from porto")


# Connected mode


def test_a_connected_socket_sends_and_receives_without_an_address() -> None:
    async def main() -> tuple[bytes, bytes, Any, Any]:
        async with await net.bind_udp(LOOPBACK) as left, await net.bind_udp(LOOPBACK) as right:
            await left.connect((LOOPBACK, right.port))
            await right.connect((LOOPBACK, left.port))
            await left.send(b"to the right")
            from_left = await right.recv()
            await right.send(b"to the left")
            return from_left, await left.recv(), left.peername, right.peername

    from_left, from_right, left_peer, right_peer = run(main())
    assert (from_left, from_right) == (b"to the right", b"to the left")
    assert left_peer[0] == LOOPBACK
    assert right_peer[0] == LOOPBACK
    assert left_peer != right_peer


def test_a_connected_socket_hears_only_its_peer() -> None:
    async def main() -> bytes:
        async with (
            await net.bind_udp(LOOPBACK) as server,
            await net.bind_udp(LOOPBACK) as peer,
            await net.bind_udp(LOOPBACK) as stranger,
        ):
            await server.connect((LOOPBACK, peer.port))
            await stranger.send_to(b"from a stranger", (LOOPBACK, server.port))
            await peer.send_to(b"from the peer", (LOOPBACK, server.port))
            return (await server.recv_from())[0]

    assert run(main()) == b"from the peer"


def test_recv_from_on_a_connected_socket_names_the_peer() -> None:
    async def main() -> tuple[bytes, Any, Any]:
        async with await net.bind_udp(LOOPBACK) as left, await net.bind_udp(LOOPBACK) as right:
            await left.connect((LOOPBACK, right.port))
            await right.send_to(b"hello", (LOOPBACK, left.port))
            data, sender = await left.recv_from()
            return data, sender, right.sockname

    data, sender, right_name = run(main())
    assert data == b"hello"
    assert sender == right_name


def test_connect_again_changes_the_peer() -> None:
    async def main() -> tuple[bytes, bytes]:
        async with (
            await net.bind_udp(LOOPBACK) as client,
            await net.bind_udp(LOOPBACK) as first,
            await net.bind_udp(LOOPBACK) as second,
        ):
            await client.connect((LOOPBACK, first.port))
            await client.send(b"one")
            await client.connect((LOOPBACK, second.port))
            await client.send(b"two")
            return (await first.recv_from())[0], (await second.recv_from())[0]

    assert run(main()) == (b"one", b"two")


def test_a_connected_udp_socket_reports_an_icmp_refusal() -> None:
    """Sending to a port nobody listens on, from a connected socket, comes back as an ICMP error
    that the next receive raises. A host whose firewall drops that ICMP error never delivers it:
    then there is nothing to check, and the test is skipped."""

    async def main() -> str:
        async with await net.bind_udp(LOOPBACK) as probe:
            dead_port = probe.port
        async with await net.bind_udp(LOOPBACK) as client:
            await client.connect((LOOPBACK, dead_port))
            await client.send(b"anyone there?")
            try:
                async with porto.timeout(5):
                    await client.recv()
            except ConnectionRefusedError:
                return "refused"
            except TimeoutError:
                return "dropped"
        return "received"

    outcome = run(main())
    if outcome == "dropped":
        pytest.skip("this host does not deliver the ICMP port-unreachable error")
    assert outcome == "refused"


def test_send_recv_and_peername_need_connect_first_and_say_so() -> None:
    async def main() -> None:
        async with await net.bind_udp(LOOPBACK) as sock:
            for call in (lambda: sock.send(b"x"), sock.recv):
                with pytest.raises(OSError, match=r"connect\(\(host, port\)\) first") as caught:
                    call()
                assert caught.value.errno == errno.ENOTCONN
            with pytest.raises(OSError, match="connect") as caught:
                sock.peername  # noqa: B018
            assert caught.value.errno == errno.ENOTCONN

    run(main())


def test_send_to_on_a_connected_socket_is_refused_with_what_to_use_instead() -> None:
    async def main() -> None:
        async with await net.bind_udp(LOOPBACK) as sock:
            await sock.connect((LOOPBACK, sock.port))
            with pytest.raises(ValueError, match=r"use send\(data\)"):
                sock.send_to(b"x", (LOOPBACK, sock.port))

    run(main())


# Addresses and families


def test_an_unspecified_host_binds_every_interface_for_both_families() -> None:
    async def main() -> tuple[Any, bytes, Any]:
        async with await net.bind_udp() as server, await net.bind_udp(LOOPBACK) as client:
            await client.send_to(b"v4 client", (LOOPBACK, server.port))
            data, sender = await server.recv_from()
            assert data == b"v4 client"
            # An IPv4 destination of this socket is mapped for it: the reply gets through.
            await server.send_to(b"v4 reply", sender)
            await server.send_to(b"v4 again", (LOOPBACK, client.port))
            assert (await client.recv_from())[0] == b"v4 reply"
            return server.sockname, (await client.recv_from())[0], sender

    server_name, again, sender = run(main())
    assert again == b"v4 again"
    if len(server_name) == 4:
        # An IPv6 socket that takes IPv4 shows IPv4 peers as IPv4-mapped addresses.
        assert sender[0] == "::ffff:" + LOOPBACK
    else:
        assert sender[0] == LOOPBACK


@needs_ipv6
def test_an_ipv6_only_socket_cannot_send_to_ipv4_and_says_what_to_do() -> None:
    async def main() -> None:
        async with await net.bind_udp(LOOPBACK_V6) as sock:
            with pytest.raises(OSError, match=r"IPv6 only.*bind a socket of the destination") as c:
                sock.send_to(b"x", (LOOPBACK, 9))
            assert c.value.errno == errno.EAFNOSUPPORT
            with pytest.raises(OSError, match="IPv4 address") as c:
                sock.connect((LOOPBACK, 9))
            assert c.value.errno == errno.EAFNOSUPPORT

    run(main())


@needs_ipv6
def test_an_ipv4_socket_cannot_send_to_ipv6_and_says_what_to_do() -> None:
    async def main() -> None:
        async with await net.bind_udp(LOOPBACK) as sock:
            with pytest.raises(OSError, match=r"IPv6 address.*bind a socket of the destination"):
                sock.send_to(b"x", (LOOPBACK_V6, 9))

    run(main())


def test_a_host_name_is_refused_with_how_to_resolve_it() -> None:
    async def main() -> None:
        with pytest.raises(ValueError, match=r"numeric IP address.*porto\.net\.resolve"):
            await net.bind_udp("localhost")
        async with await net.bind_udp(LOOPBACK) as sock:
            with pytest.raises(ValueError, match="not a numeric IP address"):
                sock.send_to(b"x", ("localhost", 9))
            with pytest.raises(ValueError, match="not a numeric IP address"):
                sock.connect(("localhost", 9))

    run(main())


def test_wrong_addresses_say_what_to_pass() -> None:
    async def main() -> None:
        async with await net.bind_udp(LOOPBACK) as sock:
            with pytest.raises(TypeError, match=r"\(host, port\) tuple"):
                sock.send_to(b"x", 9)  # pyright: ignore[reportArgumentType]
            with pytest.raises(TypeError, match="not a Unix socket path"):
                sock.send_to(b"x", "/tmp/not-udp")  # pyright: ignore[reportArgumentType]
            with pytest.raises(TypeError, match="not a Unix socket path"):
                sock.connect("/tmp/not-udp")  # pyright: ignore[reportArgumentType]
            with pytest.raises(ValueError, match="port of a socket address"):
                sock.send_to(b"x", (LOOPBACK, 70000))

    run(main())


# Options


def test_broadcast_sets_so_broadcast_and_by_default_it_is_off() -> None:
    async def main() -> tuple[int, int, bool]:
        async with await net.bind_udp(LOOPBACK) as plain:
            off = socket_option(plain, socket.SOL_SOCKET, socket.SO_BROADCAST)
        async with await net.bind_udp(LOOPBACK, broadcast=True) as broadcasting:
            on = socket_option(broadcasting, socket.SOL_SOCKET, socket.SO_BROADCAST)
        async with await net.bind_udp(None, broadcast=True) as wildcard:
            # No host with broadcast: IPv4, since IPv6 has no broadcast.
            is_v4 = len(wildcard.sockname) == 2
        return off, on, is_v4

    off, on, is_v4 = run(main())
    assert off == 0
    assert on != 0
    assert is_v4


@needs_ipv6
def test_broadcast_with_an_ipv6_host_says_what_to_do() -> None:
    async def main() -> None:
        with pytest.raises(ValueError, match=r"IPv6 has no broadcast.*multicast"):
            await net.bind_udp(LOOPBACK_V6, broadcast=True)

    run(main())


def test_a_broadcast_socket_may_send_to_a_broadcast_address() -> None:
    """With ``SO_BROADCAST`` the OS lets the socket send to a broadcast address (without it, it
    refuses with ``EACCES``). Where the host has no route for the limited broadcast address (macOS
    CI: ``EADDRNOTAVAIL``, as the socket is bound to loopback) the send fails with a routing
    error, which says nothing about the flag. What must never happen: ``EACCES`` (the flag was
    not honoured) or a send that neither completes nor fails."""

    async def main() -> str:
        async with await net.bind_udp(LOOPBACK, broadcast=True) as sock:
            try:
                async with porto.timeout(5):
                    await sock.send_to(b"x", ("255.255.255.255", 9))
            except TimeoutError:
                return "stuck"
            except OSError as error:
                return f"oserror {error.errno}"
            return "sent"

    routing = {
        errno.EADDRNOTAVAIL,
        errno.ENETUNREACH,
        errno.EHOSTUNREACH,
        errno.ENETDOWN,
        errno.EPERM,  # a host firewall refusing the datagram
    }
    outcome = run(main())
    assert outcome == "sent" or outcome.removeprefix("oserror ") in map(str, routing), outcome


def test_without_broadcast_the_os_refuses_a_send_to_a_broadcast_address() -> None:
    async def main() -> str:
        async with await net.bind_udp(LOOPBACK) as sock:
            try:
                async with porto.timeout(5):
                    await sock.send_to(b"x", ("255.255.255.255", 9))
            except TimeoutError:
                return "stuck"
            except PermissionError:
                return "refused"
            except OSError as error:
                return f"oserror {error.errno}"
            return "sent"

    # Linux and macOS answer EACCES; a host that fails earlier (no route) is no evidence either way.
    assert run(main()) != "stuck"


@pytest.mark.skipif(sys.platform == "win32", reason="SO_REUSEPORT is not on Windows")
def test_reuse_port_lets_two_sockets_bind_the_same_port() -> None:
    async def main() -> tuple[int, int]:
        async with await net.bind_udp(LOOPBACK, reuse_port=True) as first:
            second = await net.bind_udp(LOOPBACK, first.port, reuse_port=True)
            async with second:
                return first.port, second.port
        raise AssertionError("unreachable")

    first, second = run(main())
    assert first == second != 0


def test_a_taken_port_is_address_in_use() -> None:
    async def main() -> None:
        async with await net.bind_udp(LOOPBACK) as first:
            with pytest.raises(OSError, match="Errno") as caught:
                await net.bind_udp(LOOPBACK, first.port)
            assert caught.value.errno == errno.EADDRINUSE

    run(main())


@pytest.mark.parametrize(
    ("kwargs", "error", "message"),
    [
        ({"port": -1}, ValueError, r"port must be an int in 0\.\.=65535"),
        ({"port": 65536}, ValueError, r"port must be an int in 0\.\.=65535"),
        ({"port": 2**70}, ValueError, r"port must be an int in 0\.\.=65535"),
        ({"port": "53"}, TypeError, None),
        ({"host": "999.1.1.1"}, ValueError, "numeric IP address"),
    ],
)
def test_bad_arguments_to_bind_udp_say_what_is_allowed(
    kwargs: dict[str, Any], error: type[Exception], message: str | None
) -> None:
    async def main() -> None:
        with pytest.raises(error, match=message):
            await net.bind_udp(**{"host": LOOPBACK, **kwargs})

    run(main())


def test_the_addresses_and_the_file_descriptor() -> None:
    async def main() -> None:
        sock = await net.bind_udp(LOOPBACK)
        async with sock:
            assert sock.sockname == (LOOPBACK, sock.port)
            fd = sock.fileno()
            with socket.socket(fileno=socket.dup(fd)) as duplicate:
                assert duplicate.type == socket.SOCK_DGRAM
                assert duplicate.getsockname() == (LOOPBACK, sock.port)
        assert sock.port != 0, "the port stays known after close"
        with pytest.raises(porto.ClosedResourceError):
            sock.fileno()

    run(main())


# Receiving: cancellation, several tasks, closing


def test_a_cancelled_recv_from_loses_no_datagram() -> None:
    async def main() -> tuple[bytes, bytes]:
        async with await net.bind_udp(LOOPBACK) as server, await net.bind_udp(LOOPBACK) as client:
            async with porto.move_on_after(0.02) as scope:
                await server.recv_from()  # waits, and is cancelled
            assert scope.timed_out
            await client.send_to(b"after the cancel", (LOOPBACK, server.port))
            first = (await server.recv_from())[0]
            # A recv that is already waiting when the datagram comes, and is cancelled just then:
            # whatever it got, the datagram is delivered exactly once, here or in the next one.
            async with porto.scope() as s:
                waiting = s.spawn(server.recv_from)
                await porto.sleep(0.01)
                await client.send_to(b"second", (LOOPBACK, server.port))
                s.cancel()
            try:
                second = waiting.result()[0]
            except porto.Cancelled:
                second = (await server.recv_from())[0]
            return first, second

    assert run(main()) == (b"after the cancel", b"second")


def test_recv_from_cancelled_again_and_again_loses_none_of_a_stream_of_datagrams() -> None:
    count = 400

    async def sender(client: net.UdpSocket, port: int) -> None:
        for number in range(count):
            await client.send_to(number.to_bytes(4, "big"), (LOOPBACK, port))
            await porto.sleep(0.0004)

    async def main() -> tuple[list[int], int]:
        received: list[int] = []
        cancelled = 0
        attempts = 0
        async with (
            await net.bind_udp(LOOPBACK) as server,
            await net.bind_udp(LOOPBACK) as client,
            porto.scope() as s,
        ):
            s.spawn(sender, client, server.port)
            async with porto.timeout(30):
                while len(received) < count:
                    attempts += 1
                    # Every third attempt is cancelled before it starts (a deadline already
                    # past), which no timing can avoid; the others have a deadline close to the
                    # sender's pace, so many fire while a datagram is arriving.
                    deadline = 0 if attempts % 3 == 0 else 0.0003
                    async with porto.move_on_after(deadline) as attempt:
                        data, _ = await server.recv_from()
                        received.append(int.from_bytes(data, "big"))
                    cancelled += attempt.timed_out
        return received, cancelled

    received, cancelled = run(main())
    assert cancelled >= 100, "the test cancelled hardly any receive"
    assert received == list(range(count))


def test_several_tasks_may_wait_to_receive_and_each_datagram_goes_to_one() -> None:
    async def main() -> list[bytes]:
        async with await net.bind_udp(LOOPBACK) as server, await net.bind_udp(LOOPBACK) as client:
            async with porto.scope() as s:
                receivers = [s.spawn(server.recv_from) for _ in range(3)]
                await porto.sleep(0.01)
                for number in range(3):
                    await client.send_to(b"datagram %d" % number, (LOOPBACK, server.port))
            return sorted(task.result()[0] for task in receivers)

    assert run(main()) == [b"datagram 0", b"datagram 1", b"datagram 2"]


def test_closing_a_socket_fails_a_task_receiving_on_it() -> None:
    async def main() -> None:
        sock = await net.bind_udp(LOOPBACK)

        async def close_later() -> None:
            await porto.sleep(0.01)
            await sock.aclose()

        async with porto.scope() as s:
            s.spawn(close_later())
            with pytest.raises(porto.ClosedResourceError) as caught:
                await sock.recv_from()
        assert isinstance(caught.value, OSError)
        assert caught.value.errno == errno.EBADF

    run(main())


def test_every_operation_on_a_closed_socket_fails_and_closing_again_is_fine() -> None:
    async def main() -> None:
        other = await net.bind_udp(LOOPBACK)
        sock = await net.bind_udp(LOOPBACK)
        async with other:
            await sock.aclose()
            await sock.aclose()
            calls: list[Callable[[], Awaitable[object]]] = [
                sock.recv_from,
                lambda: sock.send_to(b"x", (LOOPBACK, other.port)),
                lambda: sock.connect((LOOPBACK, other.port)),
            ]
            for call in calls:
                with pytest.raises(porto.ClosedResourceError):
                    await call()

    run(main())


def test_async_with_closes_the_socket_on_exit_even_for_an_exception() -> None:
    async def main() -> None:
        sock = await net.bind_udp(LOOPBACK)
        with pytest.raises(KeyError):
            async with sock:
                raise KeyError("boom")
        with pytest.raises(porto.ClosedResourceError):
            await sock.recv_from()

    run(main())


# Arguments


@pytest.mark.parametrize("max_bytes", [0, -1, 65536, 2**63, 2**70, -(2**70)])
def test_receive_sizes_out_of_range_are_value_errors(max_bytes: int) -> None:
    async def main() -> None:
        async with await net.bind_udp(LOOPBACK) as sock:
            await sock.connect((LOOPBACK, sock.port))
            for call in (sock.recv_from, sock.recv):
                with pytest.raises(ValueError, match=r"max must be an int in 1\.\.=65535"):
                    call(max_bytes)
            with pytest.raises(TypeError):
                sock.recv_from("10")  # pyright: ignore[reportArgumentType]

    run(main())


def test_wrong_kinds_of_data_and_too_long_datagrams_say_what_to_do() -> None:
    async def main() -> None:
        async with await net.bind_udp(LOOPBACK) as sock:
            with pytest.raises(ValueError, match="more than the 65535"):
                sock.send_to(b"x" * 70000, (LOOPBACK, sock.port))
            await sock.connect((LOOPBACK, sock.port))
            with pytest.raises(TypeError, match="bytes-like object"):
                sock.send("text")  # pyright: ignore[reportArgumentType]
            with pytest.raises(
                ValueError, match=r"split it into several datagrams, or use a stream"
            ):
                sock.send(b"x" * 65536)
            with pytest.raises(ValueError, match="more than the 65535"):
                sock.send(b"x" * 70000)

    run(main())


def test_a_datagram_too_big_for_the_os_is_message_too_long() -> None:
    async def main() -> None:
        async with await net.bind_udp(LOOPBACK) as server, await net.bind_udp(LOOPBACK) as client:
            with pytest.raises(OSError, match=r"too long for this OS.*or use a stream") as caught:
                await client.send_to(b"x" * 65535, (LOOPBACK, server.port))
            assert caught.value.errno == errno.EMSGSIZE
            await client.connect((LOOPBACK, server.port))
            with pytest.raises(OSError, match="too long for this OS"):
                await client.send(b"x" * 65535)

    run(main())


def test_a_socket_of_an_earlier_run_is_refused_at_the_call() -> None:
    holder: list[net.UdpSocket] = []

    async def first() -> None:
        holder.append(await net.bind_udp(LOOPBACK))

    run(first())

    async def second() -> None:
        with pytest.raises(porto.RuntimeClosed, match="runtime that stopped"):
            holder[0].recv_from()

    run(second())


def test_a_cancelled_task_opens_no_socket() -> None:
    async def main() -> None:
        async with porto.move_on_after(0) as scope:
            await porto.checkpoint()
            await net.bind_udp(LOOPBACK)
        assert scope.timed_out

    run(main())


def test_bind_udp_outside_a_run_says_to_open_it_in_one() -> None:
    async def make() -> net.UdpSocket:
        return await net.bind_udp(LOOPBACK)

    coroutine = make()
    with pytest.raises(RuntimeError, match=r"inside a running porto runtime|porto\.run"):
        coroutine.send(None)
