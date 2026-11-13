"""``porto.net.connect_tcp`` (17-networking-tls-dns.md §1): name resolution and Happy Eyeballs v2
(RFC 8305) over ``TcpStream._connecting`` and ``_finish_connect``.

The addresses of the host come from ``porto.net.resolve``, interleaved by family (the family of
the first one first). Attempts start one after the other in a scope of their own, each
``happy_eyeballs_delay`` after the one before, or at once when the one before failed. The first
to connect wins and the scope cancels the rest; every attempt closes the socket it opened however
it ends, so a loser, a failure or a cancelled ``connect_tcp`` leaves no descriptor open. The
only timeout is the one the caller's enclosing scope sets.
"""

from __future__ import annotations

import errno
import math

from porto._core import Event, Scope, TcpStream, checkpoint, move_on_after, scope, shield
from porto._dns import SockAddr, resolve

__all__ = ["connect_tcp"]


async def connect_tcp(
    host: str,
    port: int,
    *,
    tls: object = False,
    server_hostname: str | None = None,
    local_addr: tuple[str, int] | None = None,
    happy_eyeballs_delay: float = 0.25,
    nodelay: bool = True,
) -> TcpStream:
    """Opens a TCP connection to ``host`` and ``port`` and returns its ``TcpStream``.

    ``host`` is a name or an IP address. A name is resolved with ``porto.net.resolve`` (cached,
    concurrent lookups shared), and the addresses are tried as Happy Eyeballs v2 (RFC 8305)
    prescribes: IPv6 and IPv4 interleaved, the first family the resolver ranks first, one new
    attempt every ``happy_eyeballs_delay`` seconds (250 ms by default) or as soon as the one
    before failed, the first connection to succeed kept and every other attempt cancelled and
    closed. ``0`` starts every attempt at once. ``local_addr`` is a ``(host, port)`` to bind the
    socket to before connecting (an address of the family of each attempt is used). ``nodelay``
    sets ``TCP_NODELAY``, as ``serve_tcp``'s connections have it.

    ``connect_tcp`` has no timeout of its own: wrap it in ``porto.timeout()`` or
    ``porto.move_on_after()`` for one; cancelling it closes every socket it opened. ``tls`` is
    not implemented yet: anything but ``False`` raises ``NotImplementedError``, and
    ``server_hostname`` only means something with TLS.

    Raises ``ConnectionRefusedError`` (or another ``OSError`` with its errno) if every address
    failed: the error of the first address, with the others in its notes; ``socket.gaierror`` if
    the name does not resolve; ``ValueError`` for a ``port`` outside 1..65535, a malformed
    ``local_addr``, a ``happy_eyeballs_delay`` that is not a finite number >= 0 or a
    ``server_hostname`` without TLS; ``TypeError`` for a ``host`` that is not a ``str``;
    ``RuntimeError`` outside ``porto.run``.
    """
    if tls is not False:
        raise NotImplementedError(
            "porto: TLS is not implemented yet, so connect_tcp(tls=...) cannot secure the "
            "connection; leave tls=False and connect in the clear, or wait for porto.tls"
        )
    if server_hostname is not None:
        raise ValueError(
            "porto: connect_tcp's server_hostname names the host a TLS certificate is checked "
            "against, and tls is False, so it would do nothing; drop it, or wait for porto.tls"
        )
    if isinstance(port, bool) or not isinstance(port, int):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise TypeError(
            f"porto: connect_tcp's port must be an int from 1 to 65535, got "
            f"{type(port).__name__}; look up a service name with socket.getservbyname first"
        )
    if not 1 <= port <= 65535:
        raise ValueError(
            f"porto: connect_tcp's port must be in 1..=65535, got {port}; nothing listens on 0"
        )
    if (
        isinstance(happy_eyeballs_delay, bool)
        or not isinstance(happy_eyeballs_delay, (int, float))  # pyright: ignore[reportUnnecessaryIsInstance]
        or not math.isfinite(happy_eyeballs_delay)
        or happy_eyeballs_delay < 0
    ):
        raise ValueError(
            "porto: connect_tcp's happy_eyeballs_delay must be a finite number of seconds >= 0 "
            f"(0 starts every attempt at once), got {happy_eyeballs_delay!r}"
        )
    local_addresses = await _local_addresses(local_addr)
    addresses = await resolve(host, port)
    if local_addresses:
        # Only the families local_addr can bind are tried: an address of another family could
        # only fail, and its error would hide the real outcome of the others.
        families = {_is_ipv6(a) for a in local_addresses}
        usable = [a for a in addresses if _is_ipv6(a) in families]
        if not usable:
            raise OSError(
                errno.EAFNOSUPPORT,
                f"porto: connect_tcp's local_addr has no address of the family of any address of "
                f"{host!r} (local_addr={local_addr!r}); use a local_addr of the host's family, or "
                "a host name resolving to both families",
            )
        addresses = usable
    addresses = _interleave(addresses)
    stream = await _race(addresses, local_addresses, float(happy_eyeballs_delay))
    if nodelay:
        try:
            stream.set_nodelay(True)
        except BaseException:
            await stream.aclose()
            raise
    return stream


async def _local_addresses(local_addr: tuple[str, int] | None) -> list[SockAddr]:
    """The addresses ``local_addr`` names (none for ``None``)."""
    if local_addr is None:
        return []
    if (
        not isinstance(local_addr, tuple)  # pyright: ignore[reportUnnecessaryIsInstance]
        or len(local_addr) != 2  # pyright: ignore[reportUnnecessaryComparison]
        or not isinstance(local_addr[0], str)  # pyright: ignore[reportUnnecessaryIsInstance]
        or isinstance(local_addr[1], bool)
        or not isinstance(local_addr[1], int)  # pyright: ignore[reportUnnecessaryIsInstance]
    ):
        raise ValueError(
            "porto: connect_tcp's local_addr must be a (host, port) tuple such as "
            f"('0.0.0.0', 0), got {local_addr!r}"
        )
    return await resolve(local_addr[0], local_addr[1])


def _is_ipv6(address: SockAddr) -> bool:
    return len(address) == 4


def _interleave(addresses: list[SockAddr]) -> list[SockAddr]:
    """``addresses`` with the families alternating, the family of the first one first (RFC
    8305 §4), each family in the order it came."""
    first_is_v6 = _is_ipv6(addresses[0])
    ours = [a for a in addresses if _is_ipv6(a) == first_is_v6]
    other = [a for a in addresses if _is_ipv6(a) != first_is_v6]
    mixed: list[SockAddr] = []
    for index in range(max(len(ours), len(other))):
        if index < len(ours):
            mixed.append(ours[index])
        if index < len(other):
            mixed.append(other[index])
    return mixed


def _numeric(address: SockAddr) -> SockAddr:
    """``address`` as ``TcpStream._connecting`` takes it: the host without a ``%scope`` suffix
    (the scope is in the tuple)."""
    if len(address) == 4:
        return (address[0].partition("%")[0], address[1], address[2], address[3])
    return address


def _describe(address: SockAddr) -> str:
    host = address[0].partition("%")[0] if len(address) == 4 else address[0]
    return f"[{host}]:{address[1]}" if len(address) == 4 else f"{host}:{address[1]}"


class _Race:
    """What the attempts of one ``connect_tcp`` share."""

    def __init__(self, attempts: Scope) -> None:
        self.attempts = attempts
        self.winner: TcpStream | None = None
        self.errors: dict[int, tuple[SockAddr, OSError]] = {}


async def _race(
    addresses: list[SockAddr], local_addresses: list[SockAddr], delay: float
) -> TcpStream:
    """Connects to the first of ``addresses`` that answers, starting an attempt every ``delay``
    seconds (at once after a failure) and cancelling the rest when one succeeds."""
    race: _Race | None = None
    try:
        async with scope(name="connect_tcp") as attempts:
            race = _Race(attempts)
            for index, address in enumerate(addresses):
                failed = Event()
                attempts.spawn(_attempt, race, index, address, local_addresses, failed)
                if index + 1 < len(addresses):
                    with move_on_after(delay):
                        await failed.wait()
    except BaseException:
        # Cancelled (or failed) after an attempt won, before the winner was returned: nobody
        # else owns it, so close it, even though the cancel is pending.
        if race is not None and race.winner is not None:
            with shield():
                await race.winner.aclose()
        raise
    assert race is not None
    if race.winner is not None:
        return race.winner
    if len(race.errors) < len(addresses):
        # An attempt neither won nor failed: the caller's scope cancelled them, and the exit of
        # our scope does not raise that, so it takes effect at this checkpoint.
        await checkpoint()
        raise AssertionError("porto: connect_tcp's attempts were cancelled, but the task was not")
    raise _failure(race.errors)


async def _attempt(
    race: _Race,
    index: int,
    address: SockAddr,
    local_addresses: list[SockAddr],
    failed: Event,
) -> None:
    """One connection attempt: its socket is closed unless it wins (a failure, a cancel by the
    winner or by the caller, a tie lost in the same turn)."""
    try:
        stream = _start(address, local_addresses)
        try:
            await stream._finish_connect()  # pyright: ignore[reportPrivateUsage]
        except BaseException:
            await stream.aclose()
            raise
    except OSError as error:
        race.errors[index] = (address, error)
        failed.set()
        return
    if race.winner is not None:
        await stream.aclose()
        return
    race.winner = stream
    race.attempts.cancel()


def _start(address: SockAddr, local_addresses: list[SockAddr]) -> TcpStream:
    """A socket connecting to ``address``, bound to the local address of its family, if any."""
    local = None
    if local_addresses:
        local = next(
            (a for a in local_addresses if _is_ipv6(a) == _is_ipv6(address)),
            None,
        )
        if local is None:
            raise OSError(
                errno.EAFNOSUPPORT,
                f"porto: connect_tcp's local_addr has no address of the family of "
                f"{_describe(address)}; use a local_addr that is a host name resolving to both "
                "families, or connect to a host of the family of local_addr",
            )
    return TcpStream._connecting(  # pyright: ignore[reportPrivateUsage]
        _numeric(address), None if local is None else _numeric(local)
    )


def _failure(errors: dict[int, tuple[SockAddr, OSError]]) -> OSError:
    """The error to raise when every attempt failed: the first address's, with a note for each
    of the others."""
    first = min(errors)
    error = errors[first][1]
    for index in sorted(errors):
        if index != first:
            address, other = errors[index]
            error.add_note(f"porto: connecting to {_describe(address)} failed too: {other!r}")
    return error
