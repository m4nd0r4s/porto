"""TCP: ``porto.net.listen_tcp``, ``serve_tcp``, ``TcpListener`` and ``TcpStream``.

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

The second half is ``serve_tcp``, the same server without the accept loop: it runs a handler for
each connection in a scope of its own, and ``port = await s.start(net.serve_tcp, handler, host)``
returns the port once it listens. The handler reads lines with ``readline`` (which includes the
newline and raises ``porto.IncompleteReadError`` at the end of the stream, ``.partial`` holding an
unterminated last line) and ``read_exact`` (a fixed-size frame); the buffered reads scan the
stream's receive buffer, so lines that arrive split or glued together come out the same.
"""

import socket
import sys
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


async def shout(stream: net.TcpStream) -> None:
    """A ``serve_tcp`` handler: upper-cases each line, echoes a 4-byte frame, until the end."""
    async with stream:
        try:
            while True:
                line = await stream.readline()
                if line == b"frame\n":
                    await stream.write(await stream.read_exact(4))  # a fixed-size frame
                else:
                    await stream.write(line.upper())
        except porto.IncompleteReadError as error:  # the client closed: nothing left to answer
            assert error.partial == b""


def blocking_lines_client(port: int, replies: list[bytes], done: porto.Event) -> None:
    """A client that is not porto: lines sent in awkward pieces, then half-close and read all."""
    try:
        with socket.create_connection(("127.0.0.1", port)) as client:
            for piece in (b"hel", b"lo\nwor", b"ld\nfra", b"me\nabcd", b"done\n"):
                client.sendall(piece)
            client.shutdown(socket.SHUT_WR)
            reply = b""
            while chunk := client.recv(4096):
                reply += chunk
            replies.append(reply)
    finally:
        done.set()


async def serving_with_serve_tcp() -> None:
    replies: list[bytes] = []
    finished = porto.Event()
    async with porto.scope() as s:
        port = await s.start(net.serve_tcp, shout, "127.0.0.1")  # port 0: any free port
        thread = threading.Thread(target=blocking_lines_client, args=(port, replies, finished))
        thread.start()
        await finished.wait()
        thread.join()
        s.cancel()  # serve_tcp runs forever: cancelling the scope stops it and closes connections
    assert replies == [b"HELLO\nWORLD\nabcdDONE\n"], replies


async def main() -> None:
    await echo_over_loopback()
    await closing_a_listener()
    await serving_with_serve_tcp()
    print("tcp_echo: ok")


if __name__ == "__main__":
    if sys.platform == "win32":
        # The Windows I/O backend (IOCP) is not written yet: porto cannot register sockets there.
        print("tcp_echo: skipped, porto has no sockets on Windows yet")
    else:
        porto.run(main)
