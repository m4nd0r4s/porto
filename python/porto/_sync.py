"""``porto.Mutex[T]``: a lock that owns its value (16-sync-primitives-and-channels.md §1).

It is a ``porto.Lock`` plus the value: ``async with mutex as guard:`` holds the lock and gives
access through ``guard.value`` and ``guard.set(new)``. The guard stops working when the block
exits, so the value is only reached while the lock is held, which keeps code that shares it
between tasks (or free-threaded lanes) correct.
"""

from collections.abc import Awaitable
from types import TracebackType
from typing import Generic, TypeVar, final

from porto._core import Lock

_T = TypeVar("_T")


@final
class MutexGuard(Generic[_T]):
    """Access to a ``porto.Mutex``'s value while its lock is held; what ``async with`` gives."""

    __slots__ = ("_mutex",)

    def __init__(self, mutex: "Mutex[_T]", /) -> None:
        self._mutex: Mutex[_T] | None = mutex

    def _held(self) -> "Mutex[_T]":
        mutex = self._mutex
        if mutex is None:
            raise RuntimeError(
                "porto: this Mutex guard was used after its `async with` block exited; the "
                "value is only reachable while the lock is held, so use it inside the block "
                "(or enter the mutex again)"
            )
        return mutex

    @property
    def value(self) -> _T:
        """The value the mutex owns."""
        return self._held()._value  # pyright: ignore[reportPrivateUsage]

    def set(self, value: _T, /) -> None:
        """Replaces the value the mutex owns."""
        self._held()._value = value  # pyright: ignore[reportPrivateUsage]

    def __repr__(self) -> str:
        if self._mutex is None:
            return "<porto.MutexGuard released>"
        value = self._mutex._value  # pyright: ignore[reportPrivateUsage]
        return f"<porto.MutexGuard value={value!r}>"


@final
class Mutex(Generic[_T]):
    """``porto.Mutex[T](value)``: an async lock that owns ``value``.

    ``async with mutex as guard:`` waits for the lock (fair FIFO, like ``porto.Lock``), then
    ``guard.value`` reads the value and ``guard.set(new)`` replaces it. Not reentrant: entering
    it again inside its own block waits forever. Nothing is taken until ``__aenter__()`` is
    awaited, and the guard is made in the step that takes the lock, so a task cancelled or
    closed while entering never holds it.
    """

    __slots__ = ("_guard", "_lock", "_value")

    def __init__(self, value: _T, /) -> None:
        self._lock = Lock()
        self._value = value
        self._guard: MutexGuard[_T] | None = None

    def locked(self) -> bool:
        """Whether the lock is held (or being handed to a waiter)."""
        return self._lock.locked()

    def __aenter__(self) -> Awaitable[MutexGuard[_T]]:
        return self._lock._acquire_then(self._enter)  # pyright: ignore[reportPrivateUsage]

    def _enter(self) -> MutexGuard[_T]:
        guard = MutexGuard(self)
        self._guard = guard
        return guard

    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]:
        guard = self._guard
        if guard is not None:
            guard._mutex = None  # pyright: ignore[reportPrivateUsage]
            self._guard = None
        # Releases the lock, then a checkpoint (T2) that never holds it.
        return self._lock.__aexit__(exc_type, exc, traceback)

    def __repr__(self) -> str:
        return f"<porto.Mutex {'locked' if self.locked() else 'unlocked'}>"
