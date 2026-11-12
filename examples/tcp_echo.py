"""TCP: ``porto.net.listen_tcp``, ``TcpListener`` and ``TcpStream``.

    uv run python examples/tcp_echo.py

An echo server over loopback. The server is porto: ``listen_tcp`` opens the listener (port 0
picks a free port, ``listener.port`` reports it), ``async for`` accepts connections, and each
one is served by its own task in a scope. The clients are plain blocking sockets in OS threads,
the way a program that knows nothing about porto would connect (``porto.net.connect_tcp`` comes
later); each thread sets a ``porto.Event`` when it is done, which is how the async side waits
for a thread without blocking its lane.

A ``TcpStream`` reads and writes bytes: ``read`` returns ``b""`` at the end of the stream (the
peer closed, or half-closed with ``shutdown``), ``write`` sends all of its data, waiting while the
socket's buffers are full, and ``read_buffer`` returns a ``porto.Buffer`` without copying the
bytes into a ``bytes``. A stream belongs to the ``porto.run`` that opened it.
"""

import socket
import threading

import porto
from porto import net

CLIENTS = 3
MESSAGE = b"hello from client %d"


async def serve_one(stream: net.TcpStream) -> None:
    """Echoes everything it reads, until the client half-closes; then says how much it echoed."""
    echoed = 0
    async with stream:  # closes the stream on exit
        while True:
            chunk = await stream.read_buffer(4096)  # a porto.Buffer: no bytes copy
            if len(chunk) == 0:  # the end of the stream
                break
            echoed += len(chunk)
            await stream.write(chunk)
        # Two buffers in one call (a vectored write), then half-close: the client reads EOF.
        await stream.writev([b"|echoed ", str(echoed).encode()])
        await stream.shutdown_write()


async def serve(listener: net.TcpListener, connections: int) -> None:
    async with porto.scope() as connections_scope:
        accepted = 0
        async for stream in listener:  # ends when the listener is closed
            connections_scope.spawn(serve_one(stream))
            accepted += 1
            if accepted == connections:
                break


def blocking_client(port: int, number: int, replies: list[bytes], done: porto.Event) -> None:
    """A client that is not porto: connect, send, half-close, read the reply until EOF."""
    try:
        with socket.create_connection(("127.0.0.1", port)) as client:
            client.sendall(MESSAGE % number)
            client.shutdown(socket.SHUT_WR)
            reply = b""
            while chunk := client.recv(4096):
                reply += chunk
            replies[number] = reply
    finally:
        done.set()  # Event.set() is safe from any thread


async def echo_over_loopback() -> None:
    async with await net.listen_tcp("127.0.0.1") as listener:  # port 0: any free port
        assert listener.port != 0
        assert listener.sockname == ("127.0.0.1", listener.port)
        replies: list[bytes] = [b""] * CLIENTS
        finished = [porto.Event() for _ in range(CLIENTS)]
        threads = [
            threading.Thread(target=blocking_client, args=(listener.port, n, replies, finished[n]))
            for n in range(CLIENTS)
        ]
        async with porto.scope() as s:
            s.spawn(serve(listener, CLIENTS))
            for thread in threads:
                thread.start()
            for event in finished:
                await event.wait()
        for thread in threads:
            thread.join()
    for number, reply in enumerate(replies):
        message = MESSAGE % number
        assert reply == message + b"|echoed " + str(len(message)).encode(), reply


async def closing_a_listener() -> None:
    listener = await net.listen_tcp("127.0.0.1")

    async def stop_listening() -> None:
        await porto.sleep(0.01)
        await listener.aclose()

    async with porto.scope() as s:
        s.spawn(stop_listening())
        try:
            await listener.accept()  # waits for a connection that never comes...
        except porto.ClosedResourceError as error:
            # ...until the listener is closed: an OSError with errno EBADF.
            assert isinstance(error, OSError)
            assert error.errno == 9
    # An `async for` over a closed listener simply ends.
    async for _ in listener:
        raise AssertionError("no connection was accepted")


async def main() -> None:
    await echo_over_loopback()
    await closing_a_listener()
    print("tcp_echo: ok")


if __name__ == "__main__":
    porto.run(main)
