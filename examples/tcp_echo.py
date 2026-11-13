"""TCP: ``porto.net.connect_tcp``, ``listen_tcp``, ``serve_tcp``, ``TcpListener`` and ``TcpStream``.

    uv run python examples/tcp_echo.py

An echo server over loopback. The server is porto: ``listen_tcp`` opens the listener (port 0
picks a free port, ``listener.port`` reports it), ``async for`` accepts connections, and each
one is served by its own task in a scope. The clients are tasks too: ``connect_tcp(host, port)``
resolves the host (a name or an IP address), races the addresses it finds as Happy Eyeballs
(RFC 8305) prescribes and returns the first ``TcpStream`` that connects. It has no timeout of its
own: put it in ``porto.timeout()`` or ``porto.move_on_after()`` for one.

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

import sys

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


async def client(port: int, number: int) -> bytes:
    """A client: connect, send, half-close, read the reply until the server closes."""
    async with await net.connect_tcp("localhost", port) as stream:  # a name, resolved first
        await stream.write(MESSAGE % number)
        await stream.shutdown_write()
        return await stream.read_to_end(limit=4096)


async def echo_over_loopback() -> None:
    async with await net.listen_tcp("127.0.0.1") as listener:  # port 0: any free port
        assert listener.port != 0
        assert listener.sockname == ("127.0.0.1", listener.port)
        clients: list[porto.Task[bytes]] = []
        async with porto.scope() as s:
            s.spawn(serve(listener, CLIENTS))
            clients.extend(s.spawn(client, listener.port, n) for n in range(CLIENTS))
    for number, task in enumerate(clients):
        message = MESSAGE % number
        assert task.result() == message + b"|echoed " + str(len(message)).encode(), task.result()


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


async def lines_client(port: int) -> bytes:
    """A client that sends lines in awkward pieces, then half-closes and reads all."""
    async with await net.connect_tcp("127.0.0.1", port) as stream:
        for piece in (b"hel", b"lo\nwor", b"ld\nfra", b"me\nabcd", b"done\n"):
            await stream.write(piece)
            await porto.sleep(0.001)  # let the server see the pieces apart
        await stream.shutdown_write()
        return await stream.read_to_end(limit=4096)


async def serving_with_serve_tcp() -> None:
    reply = b""
    async with porto.scope() as s:
        port = await s.start(net.serve_tcp, shout, "127.0.0.1")  # port 0: any free port
        reply = await lines_client(port)
        s.cancel()  # serve_tcp runs forever: cancelling the scope stops it and closes connections
    assert reply == b"HELLO\nWORLD\nabcdDONE\n", reply


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
