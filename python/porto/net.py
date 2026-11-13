"""Networking (17-networking-tls-dns.md): TCP (§1), UDP and Unix domain sockets (§2).

TCP is ``connect_tcp``, ``listen_tcp`` and ``serve_tcp``, and the ``TcpListener`` and
``TcpStream`` they produce:

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

UDP is ``bind_udp``, which returns a ``UdpSocket`` that sends and receives whole datagrams:

```python
async with await net.bind_udp("127.0.0.1") as server:
    async with await net.bind_udp("127.0.0.1") as client:
        await client.send_to(b"ping", ("127.0.0.1", server.port))
        data, sender = await server.recv_from()  # (b"ping", the client's address)
```

Unix domain sockets are ``listen_unix`` and ``connect_unix`` (a ``UnixListener`` and a
``UnixStream``, which is a ``porto.abc.ByteStream`` like ``TcpStream``). A name that starts with a
NUL byte is in Linux's abstract namespace, which has no file:

```python
async with await net.listen_unix("/run/app.sock", mode=0o660) as listener:
    async with await net.connect_unix("/run/app.sock") as client:
        ...
```

Sockets belong to the ``porto.run`` that opened them. ``resolve`` is the default resolver
(``getaddrinfo`` on the blocking pool, cached for 30 s, concurrent identical lookups coalesced);
host names in ``listen_tcp`` arrive with it, so until then ``listen_tcp``'s ``host`` is a numeric
address.
"""

import errno
import logging
import os
from collections.abc import Awaitable, Callable
from typing import NoReturn, cast

from porto import _core
from porto._connect import connect_tcp
from porto._core import (
    Semaphore,
    Started,
    TcpListener,
    TcpStream,
    UdpSocket,
    UnixListener,
    UnixStream,
    scope,
    sleep,
)
from porto._dns import SockAddr, resolve
from porto.errors import IncompleteReadError

__all__ = [
    "SockAddr",
    "TcpListener",
    "TcpStream",
    "UdpSocket",
    "UnixListener",
    "UnixStream",
    "bind_udp",
    "connect_tcp",
    "connect_unix",
    "listen_tcp",
    "listen_unix",
    "resolve",
    "serve_tcp",
]

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


async def bind_udp(
    host: str | None = None,
    port: int = 0,
    *,
    reuse_port: bool = False,
    broadcast: bool = False,
) -> UdpSocket:
    """Opens a UDP socket bound to ``host`` and ``port`` and returns its ``UdpSocket``.

    ``host`` is a numeric IP address, or ``None`` for every interface: one IPv6 socket that
    takes IPv4 too (an IPv4 socket on a host without IPv6, or when ``broadcast`` is set). An
    IPv6 ``host`` binds an IPv6-only socket, an IPv4 ``host`` an IPv4 one. ``port=0`` picks a
    free port, which ``socket.port`` reports. ``reuse_port`` lets several sockets bind the same
    port, the kernel spreading the datagrams between them (``NotImplementedError`` where the OS
    cannot). ``broadcast`` sets ``SO_BROADCAST`` so the socket may send to a broadcast address
    (IPv4 only: ``ValueError`` with an IPv6 ``host``).

    A datagram is received whole by ``recv_from`` and sent whole by ``send_to``; a socket
    that ``connect``-ed to a peer uses ``send`` and ``recv``. An IPv4 destination of an IPv6
    socket that takes IPv4 is mapped for it, and its senders show as IPv4-mapped addresses
    (``::ffff:a.b.c.d``).

    Raises ``OSError`` if the OS refuses (the port is taken, ``EADDRINUSE``; no permission,
    ``EACCES``), ``ValueError`` for a ``host`` that is not a numeric address, a ``port`` outside
    0..65535 or ``broadcast`` with an IPv6 host, ``RuntimeError`` outside ``porto.run``.
    """
    return _core._bind_udp(host, port, reuse_port, broadcast)  # pyright: ignore[reportPrivateUsage]


async def listen_unix(
    path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
    *,
    mode: int = 0o660,
    backlog: int = 1024,
) -> UnixListener:
    """Opens a listening Unix domain socket at ``path`` and returns its ``UnixListener``.

    ``path`` is the socket file to create (relative paths are relative to the current
    directory), or on Linux a name starting with a NUL byte (``"\\0name"``) in the abstract
    namespace, which has no file. The file has exactly the permissions ``mode`` says (an int in
    0..0o777; connecting needs write permission on it), and is never visible with more: the
    default 0o660 lets the owner and the group connect. ``mode`` means nothing for an abstract
    name. ``backlog`` is the queue of connections not accepted yet.

    The path must not exist: a path that does (a running server's socket, or a file a crashed
    server left behind) is ``OSError`` with ``EADDRINUSE`` and is never removed on your behalf.
    Closing the listener removes the socket file it created, but not one that something else
    replaced since.

    Raises ``OSError`` if the OS refuses (``EADDRINUSE``, ``ENOENT`` for a missing directory,
    ``EACCES``), ``ValueError`` for an empty path, one with a NUL byte inside, one too long for
    the OS (about 100 bytes), an abstract name off Linux, or a ``mode`` outside 0..0o777,
    ``TypeError`` for a ``path`` of another type, ``NotImplementedError`` on Windows (use
    ``listen_tcp`` on the loopback), ``RuntimeError`` outside ``porto.run``.
    """
    name = _unix_name(path)
    try:
        return _core._listen_unix(name, backlog, mode)  # pyright: ignore[reportPrivateUsage]
    except OSError as error:
        _explain(error, "listen_unix", _LISTEN_HINTS)
        raise


async def connect_unix(
    path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
) -> UnixStream:
    """Connects to the Unix domain socket at ``path`` and returns its ``UnixStream``.

    ``path`` is a socket file, or on Linux a name starting with a NUL byte (``"\\0name"``) in the
    abstract namespace. A Unix connection is decided at once, so there is nothing to wait for
    but a full backlog: while the listener's queue is full (Linux) the call waits a few
    milliseconds and tries again, which only the enclosing scope's deadline or a cancellation
    ends.

    Raises ``FileNotFoundError`` if nothing is at ``path``, ``ConnectionRefusedError`` if a
    socket file is there but nobody listens on it (a server that stopped), ``PermissionError``
    if the socket's mode or its directory forbids this user, ``ValueError`` and ``TypeError``
    for a ``path`` as ``listen_unix`` does, ``NotImplementedError`` on Windows (use
    ``connect_tcp`` on the loopback), ``RuntimeError`` outside ``porto.run``.
    """
    name = _unix_name(path)
    delay = 0.001
    while True:
        try:
            stream = _core._connect_unix(name)  # pyright: ignore[reportPrivateUsage]
        except OSError as error:
            _explain(error, "connect_unix", _CONNECT_HINTS)
            raise
        if stream is not None:
            return stream
        # The listener's backlog is full and a Unix connect cannot be waited for with poll:
        # try again a little later, backing off to 50 ms.
        await sleep(delay)
        delay = min(delay * 2, 0.05)


def _unix_name(path: object) -> str | bytes:
    """``path`` as the ``str`` or ``bytes`` the socket layer takes (``os.PathLike`` is resolved)."""
    if isinstance(path, (str, bytes)):
        return path
    fspath = getattr(path, "__fspath__", None)
    if fspath is not None:
        name = fspath()
        if isinstance(name, (str, bytes)):
            return name
    raise TypeError(
        "porto: a Unix socket's path must be a str, bytes or os.PathLike naming the socket, not "
        f"{type(path).__name__}; pass the path of the socket file, or a name starting with "
        '"\\0" for a Linux abstract socket'
    )


_LISTEN_HINTS = {
    errno.EADDRINUSE: (
        "something is already at the path: a running server's socket, or the file a server that "
        "crashed left behind; if no server uses it, remove it (os.unlink) and listen again, or "
        "pick another path"
    ),
    errno.ENOENT: "the directory of the path does not exist; create it first",
    errno.EACCES: "this user may not create files in the directory of the path; pick another one",
}

_CONNECT_HINTS = {
    errno.ENOENT: "no socket is at the path; start the server first, or check the path",
    errno.ECONNREFUSED: (
        "a socket file is at the path but nothing listens on it, as after a server stopped "
        "without removing it; start the server, or remove the stale file"
    ),
    errno.EACCES: (
        "this user may not connect: connecting needs write permission on the socket file "
        "(the server's listen_unix mode) and search permission on its directories"
    ),
}


def _explain(error: OSError, call: str, hints: dict[int, str]) -> None:
    """Adds the hint for ``error``'s errno, if there is one, as a note."""
    hint = hints.get(error.errno or 0)
    if hint is not None:
        error.add_note(f"porto: {call}: {hint}")


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
