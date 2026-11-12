"""``porto.channel[T](capacity)`` and ``porto.unbounded_channel[T]()``
(16-sync-primitives-and-channels.md §2).

Both are classes only so that they can be subscripted with the element type, at runtime and
for type checkers: calling one returns a ``(Sender[T], Receiver[T])`` pair, not an instance.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from porto import _core
from porto._core import (
    BroadcastReceiver,
    BroadcastSender,
    OneshotReceiver,
    OneshotSender,
    Receiver,
    Sender,
    WatchReceiver,
    WatchSender,
)

_T = TypeVar("_T")

_MISSING: Any = object()


class channel(Generic[_T]):
    """A bounded multi-producer multi-consumer channel: ``tx, rx = porto.channel[int](64)``.

    ``capacity`` (>= 1) is how many values it buffers; ``await tx.send(v)`` waits while it is
    full. Clone either end for more producers or consumers; every value goes to exactly one
    receiver.
    """

    if TYPE_CHECKING:

        def __new__(cls, capacity: int) -> tuple[Sender[_T], Receiver[_T]]: ...

    else:

        def __new__(cls, capacity=_MISSING):
            if capacity is _MISSING:
                raise TypeError(
                    "porto: porto.channel() needs a capacity, as in porto.channel[T](64): pass "
                    "how many values the channel may buffer before senders wait, or use "
                    "porto.unbounded_channel[T]() for no limit"
                )
            return _core._channel(capacity)


class unbounded_channel(Generic[_T]):
    """A channel without a capacity limit: ``tx, rx = porto.unbounded_channel[int]()``.

    Sends never wait, so a slow consumer lets the buffer grow without bound; prefer
    ``porto.channel`` unless the producer is bounded some other way.
    """

    def __new__(cls) -> tuple[Sender[_T], Receiver[_T]]:
        return _core._unbounded_channel()  # pyright: ignore[reportPrivateUsage]


class oneshot(Generic[_T]):
    """A channel for one value: ``tx, rx = porto.oneshot[int]()``.

    ``tx.send(value)`` never waits, so it is not awaited; ``await rx.recv()`` waits for the
    value. A sender closed or dropped without sending ends the channel
    (``porto.EndOfChannel`` at the receiver); a receiver closed or dropped first makes the send
    raise ``porto.BrokenResourceError``.
    """

    def __new__(cls) -> tuple[OneshotSender[_T], OneshotReceiver[_T]]:
        return _core._oneshot()  # pyright: ignore[reportPrivateUsage]


class watch(Generic[_T]):
    """A channel for the latest value: ``tx, rx = porto.watch[bool](False)``.

    ``tx.send(value)`` never waits (every send is a new version); ``rx.borrow()`` reads the
    latest value; ``await rx.changed()`` waits for a version it has not seen, once per call
    however many sends came before. ``watch[bool](False)`` is the clearable event: ``send(True)``
    sets it, ``send(False)`` clears it.
    """

    if TYPE_CHECKING:

        def __new__(cls, initial: _T) -> tuple[WatchSender[_T], WatchReceiver[_T]]: ...

    else:

        def __new__(cls, initial=_MISSING):
            if initial is _MISSING:
                raise TypeError(
                    "porto: porto.watch() needs an initial value, as in porto.watch[bool](False): "
                    "pass the value receivers read before the first send"
                )
            return _core._watch(initial)


class broadcast(Generic[_T]):
    """A channel where every receiver gets every message: ``tx, rx = porto.broadcast[str](64)``.

    The channel keeps the last ``capacity`` (>= 1) messages. ``tx.send(message)`` never waits;
    when the channel is full it overwrites the oldest message, and a receiver that had not read
    it gets ``porto.Lagged(skipped=n)`` with the exact count and carries on from the oldest
    message kept. ``rx.clone()`` is another receiver at the same position and ``tx.subscribe()``
    one that sees only later messages.
    """

    if TYPE_CHECKING:

        def __new__(cls, capacity: int) -> tuple[BroadcastSender[_T], BroadcastReceiver[_T]]: ...

    else:

        def __new__(cls, capacity=_MISSING):
            if capacity is _MISSING:
                raise TypeError(
                    "porto: porto.broadcast() needs a capacity, as in porto.broadcast[T](64): "
                    "pass how many messages the channel keeps for receivers that fall behind"
                )
            return _core._broadcast(capacity)
