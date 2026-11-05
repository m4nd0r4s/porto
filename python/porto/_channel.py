"""``porto.channel[T](capacity)`` and ``porto.unbounded_channel[T]()``
(16-sync-primitives-and-channels.md §2).

Both are classes only so that they can be subscripted with the element type, at runtime and
for type checkers: calling one returns a ``(Sender[T], Receiver[T])`` pair, not an instance.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from porto import _core
from porto._core import Receiver, Sender

_T = TypeVar("_T")

_NO_CAPACITY: Any = object()


class channel(Generic[_T]):
    """A bounded multi-producer multi-consumer channel: ``tx, rx = porto.channel[int](64)``.

    ``capacity`` (>= 1) is how many values it buffers; ``await tx.send(v)`` waits while it is
    full. Clone either end for more producers or consumers; every value goes to exactly one
    receiver.
    """

    if TYPE_CHECKING:

        def __new__(cls, capacity: int) -> tuple[Sender[_T], Receiver[_T]]: ...

    else:

        def __new__(cls, capacity=_NO_CAPACITY):
            if capacity is _NO_CAPACITY:
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
