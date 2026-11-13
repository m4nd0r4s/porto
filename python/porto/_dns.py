"""The default resolver (17-networking-tls-dns.md §4): ``socket.getaddrinfo`` on the blocking pool,
with a positive cache and the coalescing of concurrent identical lookups.

- IP literals never reach the pool: they are converted on the lane (``AI_NUMERICHOST`` does no
  network I/O), so a connect to ``127.0.0.1`` costs no thread hop.
- A name is looked up by ``getaddrinfo`` on a pool thread. The answer is cached for
  ``POSITIVE_TTL`` seconds (``getaddrinfo`` exposes no TTL), per run and per lane (a runtime has
  one lane until M2). Failures are not cached.
- While a lookup of ``(host, port, family)`` runs, every other task that asks for the same
  triple waits for it instead of starting its own, so a burst of connects to one host costs one
  ``getaddrinfo``. The lookup belongs to the *name*, not to the task that started it: cancelling
  that task abandons its wait, the lookup finishes on its thread, fills the cache, and the other
  waiters get their answer.
"""

from __future__ import annotations

import socket
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TypeAlias

from porto import _core
from porto._core import Event
from porto._threads import to_thread

__all__ = ["POSITIVE_TTL", "SockAddr", "resolve"]

SockAddr: TypeAlias = tuple[str, int] | tuple[str, int, int, int]
"""A resolved address in the shape of the ``socket`` module: ``(host, port)`` for IPv4,
``(host, port, flowinfo, scope_id)`` for IPv6."""

POSITIVE_TTL = 30.0
"""Seconds a successful lookup is cached for."""

_MAX_CACHED = 1024
"""Entries a cache holds: when it is full, the expired ones go, then the oldest."""

_FAMILIES = (0, socket.AF_INET, socket.AF_INET6)

_Key: TypeAlias = tuple[str, int, int]


def _getaddrinfo(host: str, port: int, family: int) -> list[SockAddr]:
    """The blocking lookup: the TCP addresses of ``host``, duplicates removed, in the order the
    OS ranks them (RFC 6724). Raises ``socket.gaierror``."""
    found: list[SockAddr] = []
    for _family, _type, _proto, _name, address in socket.getaddrinfo(
        host, port, family, socket.SOCK_STREAM
    ):
        if address not in found:
            found.append(address)  # pyright: ignore[reportArgumentType]
    return found


# Test hooks: the blocking lookup and the clock the cache measures its TTL on.
_lookup: Callable[[str, int, int], list[SockAddr]] = _getaddrinfo
_clock: Callable[[], float] = time.monotonic


@dataclass(slots=True)
class _Flight:
    """One lookup in progress; ``done`` is set from the pool thread once it has an outcome."""

    done: Event = field(default_factory=Event)
    addresses: tuple[SockAddr, ...] = ()
    error: BaseException | None = None
    started: bool = False
    finished: bool = False


class _Resolver:
    """The cache and the lookups in flight of one lane of one run."""

    def __init__(self) -> None:
        self.cache: dict[_Key, tuple[float, tuple[SockAddr, ...]]] = {}
        self.flights: dict[_Key, _Flight] = {}

    def cached(self, key: _Key) -> tuple[SockAddr, ...] | None:
        entry = self.cache.get(key)
        if entry is None:
            return None
        expires, addresses = entry
        if _clock() >= expires:
            self.cache.pop(key, None)
            return None
        return addresses

    def store(self, key: _Key, addresses: tuple[SockAddr, ...]) -> None:
        if len(self.cache) >= _MAX_CACHED:
            now = _clock()
            for stale in [k for k, (expires, _) in self.cache.items() if expires <= now]:
                self.cache.pop(stale, None)
            while len(self.cache) >= _MAX_CACHED:
                self.cache.pop(next(iter(self.cache)), None)
        self.cache[key] = (_clock() + POSITIVE_TTL, addresses)

    def finish(
        self,
        key: _Key,
        flight: _Flight,
        addresses: tuple[SockAddr, ...],
        error: BaseException | None,
    ) -> None:
        """Records the outcome of ``flight`` (from the pool thread) and wakes its waiters."""
        if flight.finished:
            return
        flight.finished = True
        flight.addresses = addresses
        flight.error = error
        if error is None:
            self.store(key, addresses)
        if self.flights.get(key) is flight:
            del self.flights[key]
        flight.done.set()

    async def resolve(self, key: _Key) -> tuple[SockAddr, ...]:
        while True:
            hit = self.cached(key)
            if hit is not None:
                return hit
            flight = self.flights.get(key)
            if flight is None:
                flight = self.flights[key] = _Flight()
                await self._lead(key, flight)
            else:
                await flight.done.wait()
            if flight.error is not None:
                raise _again(flight.error)
            if flight.finished:
                return flight.addresses
            # The task that led the lookup was cancelled before its job ran: ask again.

    async def _lead(self, key: _Key, flight: _Flight) -> None:
        """Starts the lookup of ``flight`` and waits for it. Cancelling the wait leaves the
        lookup running; it finishes the flight by itself."""
        host, port, family = key

        def job() -> None:
            flight.started = True
            try:
                addresses = tuple(_lookup(host, port, family))
            except BaseException as error:  # delivered to every waiter
                self.finish(key, flight, (), error)
            else:
                self.finish(key, flight, addresses, None)

        try:
            await to_thread(job, cancellable=True)
        except BaseException:
            if not flight.started and not flight.finished:
                # Cancelled before the job was queued: nothing will finish this flight, so
                # forget it and let the waiters (woken, not finished) look again.
                if self.flights.get(key) is flight:
                    del self.flights[key]
                flight.done.set()
            raise
        await flight.done.wait()


def _again(error: BaseException) -> BaseException:
    """``error`` for one more waiter: a fresh exception of the same kind for an ``OSError``
    (``gaierror`` included), so waiters do not share one traceback."""
    if isinstance(error, OSError):
        try:
            return type(error)(*error.args)
        except Exception:
            return error
    return error


_resolvers: dict[tuple[int, int], _Resolver] = {}


def _current() -> _Resolver:
    """The resolver of the lane the calling task runs on, in the current run."""
    run = _core._run_id()  # pyright: ignore[reportPrivateUsage]
    lane = _core.current_lane()
    resolver = _resolvers.get((run, lane))
    if resolver is None:
        # Another run's caches are dead weight: drop them with the first lookup of this one.
        for stale in [k for k in _resolvers if k[0] != run]:
            del _resolvers[stale]
        resolver = _resolvers[(run, lane)] = _Resolver()
    return resolver


def _literal(host: str, port: int, family: int) -> list[SockAddr] | None:
    """The address ``host`` names if it is an IP literal (``None`` if it is a name): the
    conversion does no network I/O, so it runs on the lane. A literal of a family other than
    ``family`` raises ``socket.gaierror``."""
    try:
        info = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM, 0, socket.AI_NUMERICHOST)
    except socket.gaierror as error:
        if error.errno in _NOT_A_LITERAL:
            return None
        raise
    if family != 0 and info[0][0] != family:
        raise socket.gaierror(
            socket.EAI_NONAME,
            f"porto: {host!r} is an IP address of another family than the family={family!r} "
            "asked for; pass a host of that family or leave family=0",
        )
    return [info[0][4]]  # pyright: ignore[reportReturnType]


# getaddrinfo's answers to a host name with AI_NUMERICHOST: "that is not a numeric address".
_NOT_A_LITERAL = frozenset(
    code
    for code in (
        getattr(socket, "EAI_NONAME", None),
        getattr(socket, "EAI_NODATA", None),
        getattr(socket, "EAI_FAIL", None),
    )
    if code is not None
)


async def resolve(host: str, port: int = 0, *, family: int = 0) -> list[SockAddr]:
    """Resolves ``host`` to the TCP addresses it names, in the order the OS prefers them.

    ``host`` is a name or an IP literal; ``port`` goes into every address (0 by default);
    ``family`` is ``0`` (any), ``socket.AF_INET`` or ``socket.AF_INET6``. An IP literal is
    converted at once, without the thread pool. A name is looked up with ``getaddrinfo`` on the
    blocking pool; the answer is cached for 30 seconds (``getaddrinfo`` reports no TTL), and
    concurrent lookups of the same name share one ``getaddrinfo``. Failures are not cached.

    Cancelling the call abandons the wait only: the lookup finishes on its thread and fills the
    cache. Each call returns a list of its own.

    Raises ``socket.gaierror`` if the name does not resolve (or the literal does not fit
    ``family``), ``TypeError`` for a ``host`` that is not a ``str`` or a ``port`` that is not an
    ``int``, ``ValueError`` for an empty ``host``, a ``port`` outside 0..65535 or an unknown
    ``family``, ``porto.NoRuntimeError`` outside ``porto.run``.
    """
    if not isinstance(host, str):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise TypeError(
            f"porto: resolve's host must be a str (a name or an IP address), got "
            f"{type(host).__name__}; pass for example 'example.com' or '127.0.0.1'"
        )
    if not host:
        raise ValueError(
            "porto: resolve's host is empty; pass a host name or an IP address such as "
            "'127.0.0.1' (listen_tcp takes None for every interface, resolve does not)"
        )
    if isinstance(port, bool) or not isinstance(port, int):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise TypeError(
            f"porto: resolve's port must be an int from 0 to 65535, got {type(port).__name__}; "
            "look up a service name with socket.getservbyname first"
        )
    if not 0 <= port <= 65535:
        raise ValueError(f"porto: resolve's port must be in 0..=65535, got {port}")
    if family not in _FAMILIES:
        raise ValueError(
            f"porto: resolve's family must be 0 (any), socket.AF_INET or socket.AF_INET6, got "
            f"{family!r}"
        )
    literal = _literal(host, port, family)
    if literal is not None:
        return literal
    # Host names are case-insensitive: one cache entry for every spelling.
    return list(await _current().resolve((host.lower(), port, int(family))))
