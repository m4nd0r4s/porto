"""TCP networking (17-networking-tls-dns.md §1): ``listen_tcp`` and the ``TcpListener`` and
``TcpStream`` it produces.

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

Sockets belong to the ``porto.run`` that opened them. Name resolution (``connect_tcp``,
host names in ``listen_tcp``) arrives with the resolver; until then ``host`` is a numeric
address.
"""

from porto import _core
from porto._core import TcpListener, TcpStream

__all__ = ["TcpListener", "TcpStream", "listen_tcp"]


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
