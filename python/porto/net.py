"""TCP networking (17-networking-tls-dns.md §1): ``listen_tcp`` and ``serve_tcp``, and the
``TcpListener`` and ``TcpStream`` they produce.

```python
import porto
from porto import net


async def main() -> None:
    async with await net.listen_tcp("127.0.0.1") as listener:
        print("listening on port", listener.port)
        async with await listener.accept() as stream:
            await stream.write(await stream.read())


porto.run(main)
```

``serve_tcp`` is the same server without the loop: it accepts forever and runs a handler for each
connection in a scope of its own, so one failing connection never stops the others:

```python
async def echo(stream: net.TcpStream) -> None:
    while True:
        try:
            line = await stream.readline()
        except porto.IncompleteReadError:
            return  # the client went away
        await stream.write(line)


async def main() -> None:
    async with porto.scope() as s:
        port = await s.start(net.serve_tcp, echo)  # returns once the server listens
        ...
```

Sockets belong to the ``porto.run`` that opened them. Name resolution (``connect_tcp``,
host names in ``listen_tcp``) arrives with the resolver; until then ``host`` is a numeric
address.
"""

import logging
from collections.abc import Awaitable, Callable
from typing import NoReturn, cast

from porto import _core
from porto._core import Semaphore, Started, TcpListener, TcpStream, scope
from porto.errors import IncompleteReadError

__all__ = ["TcpListener", "TcpStream", "listen_tcp", "serve_tcp"]

_logger = logging.getLogger("porto.net")


async def listen_tcp(
    host: str | None = None,
    port: int = 0,
    *,
    backlog: int = 1024,
    reuse_port: bool = False,
    tls: object | None = None,
    dual_stack: bool = True,
) -> TcpListener:
    """Opens a listening TCP socket and returns its ``TcpListener``.

    ``host`` is a numeric IP address, or ``None`` for every interface: with ``dual_stack`` one
    IPv6 socket that accepts IPv4 connections too (an IPv4 socket on a host without IPv6),
    without it IPv4 only. For an IPv6 ``host``, ``dual_stack`` decides whether the socket also
    accepts IPv4. ``port=0`` picks a free port, which ``listener.port`` reports. ``backlog`` is
    the queue of connections not accepted yet; ``reuse_port`` lets several listeners share the
    port (``NotImplementedError`` where the OS cannot). ``tls`` is not implemented yet:
    anything but ``None`` raises ``NotImplementedError``.

    Raises ``OSError`` if the OS refuses (the port is taken, ``EADDRINUSE``; no permission,
    ``EACCES``), ``ValueError`` for a ``host`` that is not a numeric address, a ``port`` outside
    0..65535 or a negative ``backlog``, ``RuntimeError`` outside ``porto.run``.
    """
    if tls is not None:
        raise NotImplementedError(
            "porto: TLS is not implemented yet, so listen_tcp(tls=...) cannot serve it; "
            "leave tls=None and terminate TLS in front of the program, or wait for porto.tls"
        )
    return _core._listen_tcp(host, port, backlog, reuse_port, dual_stack)  # pyright: ignore[reportPrivateUsage]


async def _connect_tcp_numeric(  # pyright: ignore[reportUnusedFunction]
    host: str, port: int, *, nodelay: bool = True
) -> TcpStream:
    """Connects to a numeric address: the core of ``connect_tcp`` (name resolution and
    Happy Eyeballs, porto-szb.69, build on it). Private until then."""
    address: tuple[str, int] = (host, port)
    stream = TcpStream._connecting(address)  # pyright: ignore[reportPrivateUsage]
    try:
        await stream._finish_connect()  # pyright: ignore[reportPrivateUsage]
        if nodelay:
            stream.set_nodelay(True)
    except BaseException:
        await stream.aclose()
        raise
    return stream


async def serve_tcp(
    handler: Callable[[TcpStream], Awaitable[object]],
    host: str | None = None,
    port: int = 0,
    *,
    tls: object | None = None,
    max_connections: int | None = None,
    started: Started[int] | None = None,
) -> NoReturn:
    """Listens on ``host`` and ``port`` (as ``listen_tcp`` does) and runs ``handler(stream)`` for
    every connection, forever: it returns only by raising, when the task running it is cancelled
    (cancelling the scope it runs in stops the server and closes every connection).

    Each handler runs as a task in a scope of its own, which it may spawn tasks into: they end
    with the handler, and a failure of one fails the handler, not the server. The connection is
    closed when the handler returns, raises or is cancelled. A handler that raises an
    ``Exception`` is logged on the ``porto.net`` logger (error level, with the traceback) and
    the server goes on; ``Cancelled`` and other ``BaseException`` propagate as always. A handler
    that lets a client's disconnect escape (``IncompleteReadError``, a ``ConnectionError`` such as
    ``ConnectionResetError`` or ``BrokenPipeError``, or an ``ExceptionGroup`` of only those) is
    not a bug and is logged at debug level only. Accepted streams have ``TCP_NODELAY`` set, like
    asyncio's.

    ``max_connections`` (a positive int, ``None`` for no limit) bounds the connections served
    at once: at the limit the server stops accepting until one ends, and further clients wait in
    the listener's backlog (``listen_tcp``'s default of 1024, then the OS refuses or drops them).
    ``started`` is a ``porto.Started[int]``: given by ``Scope.start``, it is set to the port once
    the server listens, so ``port = await s.start(net.serve_tcp, handler)`` returns the port
    the OS picked for ``port=0``.

    ``tls`` is not implemented yet: anything but ``None`` raises ``NotImplementedError``.
    Raises ``ValueError`` for a ``max_connections`` that is not a positive int, and what
    ``listen_tcp`` raises (``OSError`` when the port is taken, ``ValueError`` for a ``host`` that
    is not numeric).
    """
    if tls is not None:
        raise NotImplementedError(
            "porto: TLS is not implemented yet, so serve_tcp(tls=...) cannot serve it; leave "
            "tls=None and terminate TLS in front of the program, or wait for porto.tls"
        )
    if max_connections is not None and (
        isinstance(max_connections, bool)
        or not isinstance(max_connections, int)  # pyright: ignore[reportUnnecessaryIsInstance]
        or not 1 <= max_connections <= _MAX_CONNECTIONS_LIMIT
    ):
        raise ValueError(
            f"porto: serve_tcp's max_connections must be an int from 1 to {_MAX_CONNECTIONS_LIMIT} "
            f"or None for no limit, got {max_connections!r}; leave it out to serve any number "
            "of connections at once"
        )
    slots = None if max_connections is None else Semaphore(max_connections)
    # The listener is the inner context, so it closes first when the server stops: new
    # connections are refused at once while the handlers still running are cancelled and awaited.
    async with scope(name="serve_tcp") as server, await listen_tcp(host, port) as listener:
        if started is not None:
            started.set(listener.port)
        while True:
            if slots is not None:
                await slots.acquire()  # at the limit: stop accepting until a connection ends
            try:
                stream = await listener.accept()
            except BaseException:
                if slots is not None:
                    slots.release()
                raise
            server.spawn(_serve_connection(handler, stream, slots))
    # An `async with` may swallow what ends the loop, which only a cancellation can; the
    # cancellation of an outer scope passes through, so this line is not reached.
    raise AssertionError("porto: serve_tcp's accept loop ended without being cancelled")


# The most a porto.Semaphore holds (2**31 - 1).
_MAX_CONNECTIONS_LIMIT = 2**31 - 1


# What a client's disconnect raises in a handler that does not catch it. Not ClosedResourceError:
# a peer going away never raises it; using a stream the handler closed itself is a bug.
_DISCONNECTS = (IncompleteReadError, ConnectionError)


def _is_client_disconnect(error: Exception) -> bool:
    """Whether ``error`` is a client's disconnect, or a group (of failed children) of only those."""
    if isinstance(error, _DISCONNECTS):
        return True
    if isinstance(error, ExceptionGroup):
        group = cast("ExceptionGroup[Exception]", error)
        return group.split(_DISCONNECTS)[1] is None
    return False


async def _serve_connection(
    handler: Callable[[TcpStream], Awaitable[object]],
    stream: TcpStream,
    slots: Semaphore | None,
) -> None:
    """Runs ``handler(stream)`` in a scope of its own, closes the stream, gives back the slot."""
    try:
        try:
            async with stream, scope(name="connection"):
                await handler(stream)
        except Exception as error:
            if _is_client_disconnect(error):
                # The client went away (a handler that does not catch it): not a bug, so not
                # an error.
                _logger.debug(
                    "porto: serve_tcp's handler for the connection from %s ended because the "
                    "client disconnected (%r)",
                    _peer(stream),
                    error,
                    exc_info=error,
                )
                return
            _logger.error(
                "porto: serve_tcp's handler for the connection from %s failed with %r; the "
                "connection was closed and the server keeps serving",
                _peer(stream),
                error,
                exc_info=error,
            )
    finally:
        if slots is not None:
            slots.release()


def _peer(stream: TcpStream) -> object:
    try:
        return stream.peername
    except OSError:
        return "an unknown peer"
