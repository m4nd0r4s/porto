"""Porto's exceptions (20-python-api-reference.md, Exceptions); all are re-exported by ``porto``.

Every error porto raises is a ``PortoError``, except the two that must keep their standard
meaning for code that knows nothing about porto: ``Cancelled`` is an
``asyncio.CancelledError`` and ``Terminated`` is a ``SystemExit``. Several also subclass the
builtin a caller would catch for the same situation (``RuntimeError``, ``OSError``,
``EOFError``), so ``except RuntimeError`` keeps working.

Errors that come from Rust carry the Rust error chain as ``__notes__``, one
``caused by: ...`` note per level (26-errors-config-lifecycle.md §3).
"""

from asyncio import CancelledError

__all__ = [
    "BrokenResourceError",
    "BusyResourceError",
    "Cancelled",
    "ClosedResourceError",
    "EndOfChannel",
    "IncompleteReadError",
    "InvalidStateError",
    "Lagged",
    "LimitOverrunError",
    "NoRuntimeError",
    "PortoError",
    "RuntimeClosed",
    "RustPanic",
    "Terminated",
    "WouldBlock",
]


class PortoError(Exception):
    """Base class of the errors porto raises."""


class NoRuntimeError(PortoError, RuntimeError):
    """A porto API that needs a running runtime was used outside one.

    Start the program with ``porto.run(main)`` and call it from code that ``main`` runs.
    """


class RuntimeClosed(PortoError, RuntimeError):
    """An object was used after the runtime it belongs to shut down (or did not survive
    ``fork()``). Create a new runtime and new objects with it."""


class WouldBlock(PortoError):
    """A ``*_nowait`` operation could not complete right away; await the blocking form, or
    retry later."""


class EndOfChannel(PortoError):
    """The other side of a channel closed and nothing is left to receive; stop receiving."""


class BrokenResourceError(PortoError):
    """The resource broke from the other side (for example the peer reset the connection or
    every receiver went away); open a new one."""


class BusyResourceError(PortoError):
    """Two tasks used a resource that serves one at a time: a second task started a read (or a
    write) on a stream while another task's read (or write) was still waiting. Interleaved
    writes would corrupt the stream and two readers would split its messages, so let one task
    read and one task write, or serialize them with a ``porto.Lock``."""


class ClosedResourceError(PortoError, OSError):
    """The resource was closed by this program and cannot be used any more; open a new one."""


class IncompleteReadError(PortoError, EOFError):
    """The stream ended before the requested number of bytes arrived.

    ``partial`` holds the bytes that did arrive; ``expected`` the number asked for, or ``None``
    when reading up to a separator.
    """

    partial: bytes
    expected: int | None

    def __init__(self, partial: bytes, expected: int | None = None) -> None:
        wanted = "the separator" if expected is None else f"{expected} bytes"
        super().__init__(
            f"porto: the stream ended after {len(partial)} bytes, before {wanted}; read what "
            "arrived from the exception's .partial, or treat the end of the stream as the end "
            "of the message"
        )
        self.partial = partial
        self.expected = expected

    def __reduce__(self) -> tuple[type["IncompleteReadError"], tuple[bytes, int | None]]:
        return type(self), (self.partial, self.expected)


class LimitOverrunError(PortoError):
    """A read went past its ``limit`` without finding the separator (``read_until``,
    ``readline``) or reaching the end of the stream (``read_to_end``). Nothing was consumed: the
    bytes received so far are still buffered. Read them with ``read()``, pass a larger ``limit``,
    or close the stream if the peer should not send this much.

    ``consumed`` is how many bytes are buffered and unconsumed (``read_exact(e.consumed)`` takes
    them): at least ``limit`` for ``read_until``/``readline``, ``limit + 1`` for
    ``read_to_end``, more if earlier reads left bytes in the buffer."""

    consumed: int

    def __init__(self, message: str, consumed: int = 0) -> None:
        super().__init__(message)
        self.consumed = consumed

    def __reduce__(self) -> tuple[type["LimitOverrunError"], tuple[str, int]]:
        return type(self), (str(self), self.consumed)


class Lagged(PortoError):
    """A broadcast receiver fell so far behind that the oldest ``skipped`` messages were
    dropped before it read them. The next receive returns the oldest message still kept."""

    skipped: int

    def __init__(self, skipped: int) -> None:
        super().__init__(
            f"porto: this broadcast receiver fell behind and missed {skipped} messages; keep "
            "receiving to continue from the oldest message still buffered, or give the channel "
            "a larger capacity if no message may be lost"
        )
        self.skipped = skipped

    def __reduce__(self) -> tuple[type["Lagged"], tuple[int]]:
        return type(self), (self.skipped,)


class InvalidStateError(PortoError):
    """An operation was called in a state that does not allow it (for example
    ``Task.result()`` before the task finished)."""


class RustPanic(PortoError):
    """Rust code panicked while running a task; the task failed with this error and the
    runtime kept running. This is a bug in porto or in a Rust extension: please report it.

    ``message`` is the panic message, ``location`` the ``file:line:column`` it came from, when
    known.
    """

    message: str
    location: str | None

    def __init__(self, message: str, location: str | None = None) -> None:
        at = "" if location is None else f" at {location}"
        super().__init__(
            f"porto: Rust code panicked{at}: {message}; this is a bug in porto or in a Rust "
            "extension, please report it with this message"
        )
        self.message = message
        self.location = location

    def __reduce__(self) -> tuple[type["RustPanic"], tuple[str, str | None]]:
        return type(self), (self.message, self.location)


class Cancelled(CancelledError):
    """The task was cancelled (its scope was cancelled or hit its deadline). Let it propagate:
    the scope that cancelled the task catches it. Swallowing it keeps the task running when it
    was asked to stop (15-structured-concurrency-and-cancellation.md, C11)."""


class Terminated(SystemExit):
    """The process received SIGTERM: ``porto.run`` cancelled the program and raises this after
    shutdown, so the interpreter exits as it does for ``SystemExit``."""
