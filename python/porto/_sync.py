"""``porto.Mutex[T]``: a lock that owns its value (16-sync-primitives-and-channels.md §1).

It is a ``porto.Lock`` plus the value: ``async with mutex as guard:`` holds the lock and gives
access through ``guard.value`` and ``guard.set(new)``. The guard stops working when the block
exits, so the value is only reached while the lock is held, which keeps code that shares it
between tasks (or free-threaded lanes) correct.
"""

from collections.abc import Awaitable
from types import TracebackType
from typing import Generic, TypeVar, final

from porto._core import (
    Lock,
    _checkpoint_if_needed,  # pyright: ignore[reportPrivateUsage]
)

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
    it again inside its own block waits forever.
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
        if not self._lock.try_acquire():
            return self._acquire()
        try:
            return _checkpoint_if_needed(self._enter())
        except BaseException:
            self._exit()
            raise

    async def _acquire(self) -> MutexGuard[_T]:
        await self._lock.acquire()
        return self._enter()

    def _enter(self) -> MutexGuard[_T]:
        guard = MutexGuard(self)
        self._guard = guard
        return guard

    def _exit(self) -> None:
        guard = self._guard
        if guard is not None:
            guard._mutex = None  # pyright: ignore[reportPrivateUsage]
            self._guard = None
        self._lock.release()

    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]:
        self._exit()
        # After the release: a checkpoint here (T2) never holds the lock.
        return _checkpoint_if_needed(None)

    def __repr__(self) -> str:
        return f"<porto.Mutex {'locked' if self.locked() else 'unlocked'}>"
