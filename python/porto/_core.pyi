"""The native extension module, built from crates/porto-py.

The public API (re-exported by ``porto``): ``run``, ``scope``, ``Scope``, ``Task``, ``sleep``,
``checkpoint``, ``current_time``, ``monotonic`` and ``__version__``
(docs/spec/10-porto-runtime/20-python-api-reference.md).
Underscore names are internal to porto: the trap protocol's awaitables
(11-task-model-and-trap-protocol.md §5-§6), the step driver's entry points (§4) and test hooks.
"""

from collections.abc import Awaitable, Callable, Coroutine, Generator
from types import TracebackType
from typing import Any, Generic, Literal, NoReturn, Self, TypeVar, TypeVarTuple, final, overload

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
_Ts = TypeVarTuple("_Ts")

__version__: str

# Running

def run(main: Callable[[*_Ts], Awaitable[_T]] | Awaitable[_T], /, *args: *_Ts) -> _T:
    """Runs ``main(*args)`` (or the awaitable ``main``) as the main task of a new runtime whose
    lane 0 is the calling thread, and returns its result or raises its exception.

    Ctrl-C raises ``KeyboardInterrupt`` out of it within about 50 ms, also while every task
    sleeps. A ``KeyboardInterrupt`` or ``SystemExit`` raised by a task nobody waits for also
    stops the run and is raised here. Tasks still running when it returns are closed. Raises
    ``RuntimeError`` when called from inside a running porto runtime.
    """

# Time

def current_time() -> float:
    """The runtime clock: seconds since the runtime started (virtual seconds in deterministic
    mode). Monotonic; sleep deadlines are measured on it: ``porto.current_time() + 5``.

    Raises ``RuntimeError`` outside ``porto.run``: the clock belongs to the runtime.
    """

monotonic = current_time
"""An alias of ``current_time``: the same function."""

def sleep(seconds: float, /) -> Awaitable[None]:
    """Resumes the task ``seconds`` from now, never earlier. ``sleep(0)`` yields once.

    Returns a trap, not a coroutine: ``await`` it right away, inside ``porto.run``.
    """

def checkpoint() -> Awaitable[None]:
    """Yields to the other ready tasks once.

    Returns a trap, not a coroutine: ``await`` it right away, inside ``porto.run``.
    """

# Structured concurrency

def scope() -> Scope:
    """A new scope; enter it with ``async with porto.scope() as s:``."""

@final
class Scope:
    """A nursery: every task spawned into it finishes before its ``async with`` block exits.

    If children raised, the exit raises them in an ``ExceptionGroup`` (``BaseExceptionGroup`` if
    one is not an ``Exception``), after the body's exception if the body raised. M0 scopes have
    no cancellation: a failing child does not stop its siblings.
    """

    def spawn(
        self,
        coro_or_fn: Coroutine[Any, Any, _T] | Callable[[*_Ts], Awaitable[_T]],
        /,
        *args: *_Ts,
    ) -> Task[_T]:
        """Starts a child task, in a copy of the current context: a coroutine object, or an
        async function called with ``args``. Raises ``RuntimeError`` outside the block."""

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None: ...
    def _wait_trap(self) -> _Trap | None: ...
    def _finish(self, exc: BaseException | None, /) -> None: ...

@final
class Task(Generic[_T_co]):
    """A child task of a scope. Its result is readable once its scope's block exited."""

    def done(self) -> bool:
        """Whether the task finished (returned or raised)."""

    def result(self) -> _T_co:
        """What the task returned, or raises what it raised; ``RuntimeError`` if it has not
        finished yet."""

@final
class _Trap:
    """A request to the driver. Awaiting it yields the trap, then returns what the driver sends.

    A trap is awaited exactly once: resuming it again raises ``RuntimeError("trap reused")``.
    """

    @property
    def kind(self) -> Literal["yield", "sleep", "park"]:
        """What the trap asks the driver for."""

    @property
    def deadline(self) -> float | None:
        """For a sleep, when it ends, in seconds on the runtime clock (``current_time()``)."""

    def __await__(self) -> Generator[_Trap, object, object]: ...
    def __iter__(self) -> Generator[_Trap, object, object]: ...
    def __next__(self) -> _Trap: ...
    def send(self, value: object, /) -> _Trap: ...
    @overload
    def throw(
        self, typ: BaseException, val: None = None, tb: TracebackType | None = None, /
    ) -> NoReturn: ...
    @overload
    def throw(
        self,
        typ: type[BaseException],
        val: object = None,
        tb: TracebackType | None = None,
        /,
    ) -> NoReturn: ...
    def close(self) -> None: ...

@final
class _Ready(Generic[_T_co]):
    """An awaitable that already holds its value: awaiting it never suspends."""

    def __await__(self) -> Generator[_Trap, object, _T_co]: ...
    def __iter__(self) -> Generator[_Trap, object, _T_co]: ...
    def __next__(self) -> NoReturn: ...
    def send(self, value: object, /) -> NoReturn: ...
    @overload
    def throw(
        self, typ: BaseException, val: None = None, tb: TracebackType | None = None, /
    ) -> NoReturn: ...
    @overload
    def throw(
        self,
        typ: type[BaseException],
        val: object = None,
        tb: TracebackType | None = None,
        /,
    ) -> NoReturn: ...
    def close(self) -> None: ...

def _run(coro: Coroutine[Any, Any, _T], /) -> _T:
    """Runs ``coro`` as a task on a fresh single-lane runtime; returns its result or raises.

    Tasks it spawned that are still running when it returns are closed.
    """

def _spawn(coro: Coroutine[Any, Any, object], /) -> None:
    """Starts ``coro`` as a task on the running runtime. An ``Exception`` it raises goes to
    ``sys.unraisablehook``; any other ``BaseException`` stops the run (``_run`` raises it)."""

def _yield_trap() -> _Trap:
    """A trap that yields to the scheduler once."""

def _sleep_trap(delay: float, /) -> _Trap:
    """A trap that resumes the task ``delay`` seconds from now, never earlier. Reads the runtime
    clock, so it raises ``RuntimeError`` outside ``porto.run``."""

def _make_ready(value: _T_co, /) -> _Ready[_T_co]:
    """Test hook: a ready awaitable holding ``value``."""

def _send(iterator: object, value: object, /) -> tuple[bool, object]:
    """Test hook: ``PyIter_Send``. ``(True, result)`` on return, ``(False, yielded)`` on yield."""

def _count_allocations(function: Callable[[], object], /) -> int:
    """Test hook: allocations CPython makes (MEM and OBJ domains) while ``function()`` runs."""
