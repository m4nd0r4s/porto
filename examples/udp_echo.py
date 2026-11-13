"""UDP: ``porto.net.bind_udp`` and ``UdpSocket``.

    uv run python examples/udp_echo.py

A datagram socket sends and receives whole messages, each with the address of its sender, so one
socket serves every client. ``bind_udp(host, port)`` opens it (port 0 picks a free port,
``sock.port`` reports it): ``send_to(data, (host, port))`` sends one datagram and
``recv_from()`` returns ``(data, address)`` of the next one. Datagrams keep their boundaries
(three sends are three receives, an empty datagram is a datagram), and a datagram longer than
``recv_from``'s ``max`` is cut. Addresses are numeric: look a host name up with
``porto.net.resolve`` first.

``connect(address)`` fixes the peer: ``send(data)`` and ``recv()`` then take no address, and the
socket hears only that peer (UDP has no handshake: ``connect`` does not wait for anyone).

A receive that is cancelled loses no datagram: a deadline can fire at any moment of a stream of
datagrams and every datagram still arrives exactly once, in the receive that comes next. Closing
a socket fails the tasks waiting on it with ``porto.ClosedResourceError``.
"""

import sys

import porto
from porto import net

HOST = "127.0.0.1"
CLIENTS = 3


async def echo_server(server: net.UdpSocket, datagrams: int) -> None:
    """Sends every datagram back to its sender, ``datagrams`` times."""
    for _ in range(datagrams):
        data, sender = await server.recv_from()
        await server.send_to(data, sender)


async def client(port: int, number: int) -> bytes:
    """A client: one socket, one datagram, one reply."""
    async with await net.bind_udp(HOST) as sock:  # port 0: the OS picks one
        await sock.send_to(b"hello from client %d" % number, (HOST, port))
        reply, sender = await sock.recv_from()
        assert sender == (HOST, port), sender  # the reply names the server
        return reply


async def echo_over_loopback() -> None:
    async with await net.bind_udp(HOST) as server:
        assert server.sockname == (HOST, server.port)
        replies: list[porto.Task[bytes]] = []
        async with porto.scope() as s:
            s.spawn(echo_server(server, CLIENTS))
            replies.extend(s.spawn(client, server.port, n) for n in range(CLIENTS))
    for number, task in enumerate(replies):
        assert task.result() == b"hello from client %d" % number, task.result()


async def datagram_boundaries() -> None:
    async with await net.bind_udp(HOST) as receiver, await net.bind_udp(HOST) as sender:
        for datagram in (b"first", b"", b"third, longer"):
            await sender.send_to(datagram, (HOST, receiver.port))
        assert [(await receiver.recv_from())[0] for _ in range(3)] == [
            b"first",
            b"",
            b"third, longer",
        ]
        # `max` cuts a longer datagram and the rest of it is gone: the next receive is the next
        # datagram, not the tail of this one.
        await sender.send_to(b"0123456789", (HOST, receiver.port))
        await sender.send_to(b"next", (HOST, receiver.port))
        assert (await receiver.recv_from(4))[0] == b"0123"
        assert (await receiver.recv_from())[0] == b"next"


async def connected_sockets() -> None:
    async with (
        await net.bind_udp(HOST) as left,
        await net.bind_udp(HOST) as right,
        await net.bind_udp(HOST) as stranger,
    ):
        await left.connect((HOST, right.port))
        await right.connect((HOST, left.port))
        assert left.peername == (HOST, right.port)
        await stranger.send_to(b"ignored: not the peer", (HOST, left.port))
        await right.send(b"to the left")  # no address: `connect` fixed it
        assert await left.recv() == b"to the left"  # the stranger's datagram never shows


async def cancelling_a_receive_loses_nothing() -> None:
    count = 50
    received: list[int] = []
    timeouts = 0
    async with await net.bind_udp(HOST) as receiver, await net.bind_udp(HOST) as sender:

        async def send_numbers() -> None:
            for number in range(count):
                await sender.send_to(number.to_bytes(2, "big"), (HOST, receiver.port))
                await porto.sleep(0.0004)

        async with porto.scope() as s:
            s.spawn(send_numbers())
            while len(received) < count:
                async with porto.move_on_after(0.0003) as attempt:  # often fires mid-stream
                    data, _ = await receiver.recv_from()
                    received.append(int.from_bytes(data, "big"))
                timeouts += attempt.timed_out
    assert received == list(range(count)), received  # none lost, none twice, in order
    assert timeouts > 0, "the deadline never fired"


async def closing_a_socket() -> None:
    sock = await net.bind_udp(HOST)

    async def close_later() -> None:
        await porto.sleep(0.01)
        await sock.aclose()

    async with porto.scope() as s:
        s.spawn(close_later())
        try:
            await sock.recv_from()  # waits for a datagram that never comes...
        except porto.ClosedResourceError as error:
            assert error.errno == 9  # ...until the socket is closed: an OSError with EBADF


async def main() -> None:
    await echo_over_loopback()
    await datagram_boundaries()
    await connected_sockets()
    await cancelling_a_receive_loses_nothing()
    await closing_a_socket()
    print("udp_echo: ok")


if __name__ == "__main__":
    if sys.platform == "win32":
        # The Windows I/O backend (IOCP) is not written yet: porto cannot register sockets there.
        print("udp_echo: skipped, porto has no sockets on Windows yet")
    else:
        porto.run(main)
