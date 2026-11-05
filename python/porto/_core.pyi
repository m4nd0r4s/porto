"""The native extension module, built from crates/porto-py.

The public API (re-exported by ``porto``): ``run``, ``scope``, ``Scope``, ``Task``, ``sleep``,
``checkpoint``, ``current_time``, ``monotonic``, ``Event``, ``Notify``, ``Lock``, ``Sender``,
``Receiver`` and ``__version__``
(docs/spec/10-porto-runtime/20-python-api-reference.md).
Underscore names are internal to porto: the trap protocol's awaitables
(11-task-model-and-trap-protocol.md §5-§6), the step driver's entry points (§4) and test hooks.
"""

import contextvars
from collections.abc import Awaitable, Callable, Coroutine, Generator
from types import TracebackType
from typing import Any, Generic, Literal, NoReturn, Self, TypeVar, TypeVarTuple, final, overload

from porto._config import RuntimeConfig

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
_Ts = TypeVarTuple("_Ts")

__version__: str

# Running

def run(
    main: Callable[[*_Ts], Awaitable[_T]] | Awaitable[_T],
    /,
    *args: *_Ts,
    config: RuntimeConfig | None = None,
    **config_overrides: Any,
) -> _T:
    """Runs ``main(*args)`` (or the awaitable ``main``) as the main task of a new runtime whose
    lane 0 is the calling thread, and returns its result or raises its exception.

    The runtime's settings are ``config_overrides`` (``lanes=4``) over ``config`` over the
    ``PORTO_*`` environment variables over the defaults; see ``porto.RuntimeConfig``. They are
    checked before ``main`` is called. Unknown ``PORTO_*`` variables warn.

    Ctrl-C raises ``KeyboardInterrupt`` out of it within about 50 ms, also while every task
    sleeps. A ``KeyboardInterrupt`` or ``SystemExit`` raised by a task nobody waits for also
    stops the run and is raised here. Tasks still running when it returns are closed. Raises
    ``RuntimeError`` when called from inside a running porto runtime.
    """

# Time

def current_time() -> float:
    """The runtime clock: seconds since the runtime started (virtual seconds in deterministic
    mode). Monotonic; sleep deadlines are measured on it: ``porto.current_time() + 5``.

    Raises ``porto.NoRuntimeError`` outside ``porto.run``: the clock belongs to the runtime.
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
        async function called with ``args``. Raises ``RuntimeError`` outside the block
        (``porto.NoRuntimeError`` outside ``porto.run``)."""

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
        """What the task returned, or raises what it raised; ``porto.InvalidStateError`` if it
        has not finished yet, ``porto.RuntimeClosed`` if its runtime shut down first."""

# Synchronization (16-sync-primitives-and-channels.md §1)

@final
class Event:
    """A flag that starts unset and, once ``set()``, stays set: every waiter, current and
    future, is released. Not clearable (Trio semantics). Thread-safe."""

    def __init__(self) -> None: ...
    def set(self) -> None:
        """Sets the event and wakes every task waiting for it. Setting it again does nothing."""

    def is_set(self) -> bool:
        """Whether the event is set."""

    def wait(self) -> _SyncWait[None]:
        """Completes once the event is set; without suspending if it already is.

        Nothing happens until it is awaited; ``await`` it inside ``porto.run``.
        """

    def blocking_wait(self) -> None:
        """Blocks the calling thread until the event is set. Only from threads that do not run
        porto: raises ``RuntimeError`` on a lane thread.

        The wait does not check for signals: Ctrl-C does not interrupt it, and the
        ``KeyboardInterrupt`` is raised once it is over."""

@final
class Notify:
    """A wake-up signal between tasks. ``notify_one()`` wakes the longest waiting task, or
    stores one permit for the next ``notified()``; ``notify_all()`` wakes the tasks waiting
    now and stores nothing. A task woken by ``notify_one()`` that is cancelled before it ran
    passes the wake on. Thread-safe."""

    def __init__(self) -> None: ...
    def notify_one(self) -> None:
        """Wakes the task that has waited longest, or stores a permit (at most one)."""

    def notify_all(self) -> None:
        """Wakes every task waiting now. Stores no permit."""

    def notified(self) -> _SyncWait[None]:
        """Completes when notified: at once if a permit is stored (consuming it), otherwise on
        a ``notify_one()`` or ``notify_all()`` made after the ``await`` started.

        Nothing happens until it is awaited: one that is never awaited consumes no permit. A
        task closed or cancelled before it observed the wake passes it on.
        """

    def blocking_notified(self) -> None:
        """Blocks the calling thread until notified. Only from threads that do not run porto:
        raises ``RuntimeError`` on a lane thread.

        The wait does not check for signals: Ctrl-C does not interrupt it, and the
        ``KeyboardInterrupt`` is raised once it is over."""

@final
class Lock:
    """An async lock: ``async with lock:``. Fair: waiters get it in arrival order, and
    ``release()`` hands it straight to the first one. **Not reentrant**: acquiring it again
    from the task that holds it waits forever. A waiter that is cancelled never leaks it.
    Thread-safe.

    The holder is not recorded (like ``asyncio.Lock``): ``release()`` from a task that does
    not hold the lock, or a second ``release()``, releases it from under whoever holds it,
    breaking mutual exclusion; the holder's own ``release()`` then raises ``RuntimeError``.
    Release only what you acquired, or use ``async with``."""

    def __init__(self) -> None: ...
    def acquire(self) -> _SyncWait[None]:
        """Completes once the task holds the lock; without suspending if it is free.

        Nothing happens until it is awaited: one that is never awaited takes no lock. A task
        cancelled or closed before it observed the lock passes it on.
        """

    def _acquire_then(self, on_acquired: Callable[[], _T], /) -> _SyncWait[_T]:
        """Internal (``porto.Mutex``): ``acquire()``, then ``on_acquired()`` in the same step;
        the await evaluates to its result. If it raises, the lock is released first."""

    def try_acquire(self) -> bool:
        """Takes the lock if it is free right now; returns whether it did."""

    def release(self) -> None:
        """Releases the lock, handing it to the longest waiting task. ``RuntimeError`` if the
        lock is not held. Call it only from the task that acquired the lock: the holder is not
        checked."""

    def locked(self) -> bool:
        """Whether the lock is held (or being handed to a waiter)."""

    def blocking_acquire(self) -> None:
        """Blocks the calling thread until it holds the lock; ``release()`` releases it. Only
        from threads that do not run porto: raises ``RuntimeError`` on a lane thread.

        The wait does not check for signals: Ctrl-C does not interrupt it."""

    def __aenter__(self) -> _SyncWait[None]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...

# Channels (16-sync-primitives-and-channels.md §2; constructed by porto.channel and
# porto.unbounded_channel)

_T_contra = TypeVar("_T_contra", contravariant=True)

@final
class Sender(Generic[_T_contra]):
    """The sending end of a channel. Clone it for more producers.

    Closing it (``close()``, ``async with``, or dropping the last reference) is per handle:
    once every sender is closed, receivers drain what is buffered and then see the end.
    """

    def send(self, value: _T_contra, /) -> Awaitable[None]:
        """Sends ``value``, waiting for room while the channel is full. Raises
        ``porto.BrokenResourceError`` if every receiver is gone, ``porto.ClosedResourceError``
        if this sender was closed. Nothing happens until it is awaited."""

    def try_send(self, value: _T_contra, /) -> None:
        """Sends ``value`` if there is room now; raises ``porto.WouldBlock`` if the channel is
        full, and the errors of ``send``."""

    def blocking_send(self, value: _T_contra, /) -> None:
        """``send`` from a thread that does not run porto, blocking that thread while the
        channel is full. Raises ``RuntimeError`` on a porto lane thread."""

    def clone(self) -> Sender[_T_contra]:
        """Another sender on the same channel. A clone of a closed sender is closed."""

    def close(self) -> None:
        """Closes this sender; other clones are unaffected. Sends already waiting
        through it raise ``porto.ClosedResourceError``. Closing twice does nothing."""

    def is_closed(self) -> bool:
        """Whether sends through this sender fail: it was closed, or every receiver is gone."""

    @property
    def capacity(self) -> int | None:
        """The channel's capacity, ``None`` for an unbounded channel."""

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        """Closes the sender."""

@final
class Receiver(Generic[_T_co]):
    """The receiving end of a channel. Clone it for more consumers: each value goes to one.

    Closing it (``close()``, or dropping the last reference) is per handle: once every
    receiver is closed, sends raise ``porto.BrokenResourceError`` and the buffered values are
    dropped.
    """

    def recv(self) -> Awaitable[_T_co]:
        """The next value, waiting while the channel is empty. Raises ``porto.EndOfChannel``
        once every sender is closed and the buffer is drained, ``porto.ClosedResourceError``
        if this receiver was closed. Nothing happens until it is awaited."""

    def recv_many(self, max: int, /) -> Awaitable[list[_T_co]]:
        """Waits for at least one value, then returns up to ``max`` buffered values (``max``
        >= 1) without waiting again. Raises as ``recv``."""

    def try_recv(self) -> _T_co:
        """The next value if one is buffered now; raises ``porto.WouldBlock`` if the channel is
        empty, and the errors of ``recv``."""

    def blocking_recv(self) -> _T_co:
        """``recv`` from a thread that does not run porto, blocking that thread while the
        channel is empty. Raises ``RuntimeError`` on a porto lane thread."""

    def clone(self) -> Receiver[_T_co]:
        """Another receiver on the same channel. A clone of a closed receiver is closed."""

    def close(self) -> None:
        """Closes this receiver; other clones are unaffected. Receives already waiting
        through it raise ``porto.ClosedResourceError``. Closing twice does nothing."""

    def __aiter__(self) -> Self: ...
    def __anext__(self) -> Awaitable[_T_co]:
        """``async for value in rx``: like ``recv``; the end of the channel ends the loop."""

def _channel(capacity: int, /) -> tuple[Sender[Any], Receiver[Any]]: ...
def _unbounded_channel() -> tuple[Sender[Any], Receiver[Any]]: ...

@final
class _Trap:
    """A request to the driver. Awaiting it yields the trap, then returns what the driver sends.

    A trap is awaited exactly once: resuming it again raises ``RuntimeError("trap reused")``.
    """

    @property
    def kind(
        self,
    ) -> Literal["yield", "sleep", "notified", "io", "join", "scope_exit", "future"]:
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
class _SyncWait(Generic[_T_co]):
    """The awaitable a sync primitive's wait returns (``Event.wait()``, ``Notify.notified()``,
    ``Lock.acquire()``).

    Lazy: it tries the primitive when awaited, and only then. Await it once.
    """

    def __await__(self) -> Generator[_Trap, object, _T_co]: ...
    def __iter__(self) -> Generator[_Trap, object, _T_co]: ...
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
    clock, so it raises ``porto.NoRuntimeError`` outside ``porto.run``."""

def _make_ready(value: _T_co, /) -> _Ready[_T_co]:
    """Test hook: a ready awaitable holding ``value``."""

def _send(iterator: object, value: object, /) -> tuple[bool, object]:
    """Test hook: ``PyIter_Send``. ``(True, result)`` on return, ``(False, yielded)`` on yield."""

def _take_unnormalized_exception(
    exc_type: type[BaseException], value: object, traceback: TracebackType | None, /
) -> BaseException:
    """Test hook: ``PyErr_Restore`` of the raw triple, then porto's ``take_exception``."""

def _physical_cores() -> int:
    """The machine's physical core count (logical if unknown)."""

def _auto_lanes(gil_build: bool, cores: int, /) -> int:
    """What ``lanes="auto"`` resolves to with ``cores`` physical cores."""

def _count_allocations(function: Callable[[], object], /) -> int:
    """Test hook: allocations CPython makes (MEM and OBJ domains) while ``function()`` runs."""

def _raise_rust_error_chain(depth: int, /) -> NoReturn:
    """Test hook: raises ``PortoError("level 0")`` from a Rust error whose source chain is
    ``depth`` levels deep, one ``caused by: level N`` note per level."""

def _raise_rust_panic(message: str, /) -> NoReturn:
    """Test hook: panics in Rust with ``message`` and raises the resulting ``RustPanic``."""

@final
class _WaitProbe:
    """Test hook: a bare wait queue. ``wait()`` is a ``Notified`` trap on it."""

    def __init__(self) -> None: ...
    def wait(self) -> _Trap:
        """A trap that waits until a notify grants the awaiting task's wait node."""

    def notify_all(self) -> int:
        """Wakes every task waiting now; returns how many."""

    def notify_one(self) -> bool:
        """Grants the longest waiting task; returns whether one was waiting."""

    @property
    def waiting(self) -> int:
        """How many tasks are queued."""

@final
class _JoinTarget(Generic[_T_co]):
    """Test hook: a task spawned by ``_spawn_joinable``."""

    def join(self) -> Awaitable[_T_co]:
        """A ``Join`` trap: evaluates to the task's result, or raises its exception."""

    @property
    def done(self) -> bool:
        """Whether the task finished."""

    @property
    def steps(self) -> int:
        """How many times the driver resumed the coroutine; read it from another task."""

@final
class _TaskToken:
    """Test hook: the task that called ``_current_task_token``."""

def _spawn_joinable(
    coro: Coroutine[Any, Any, _T], /, context: contextvars.Context | None = None
) -> _JoinTarget[_T]:
    """Test hook: starts ``coro`` as a task on the running runtime, to join with ``join()``. It
    runs in a copy of the current context, or in ``context`` itself if given."""

def _checkpoint_if_needed(value: _T, /) -> Awaitable[_T]:
    """Test hook: what a synchronous completion returns (T2): a ``_Ready``, or a yield trap that
    resumes with ``value`` once the running task's budget is spent."""

def _current_task_token() -> _TaskToken:
    """Test hook: a token for the running task; ``RuntimeError`` outside a porto task."""

def _interrupt(token: _TaskToken, exception: BaseException, /) -> None:
    """Test hook: throws ``exception`` into the task at the await it waits in, giving its wait
    up first (T3), or at its next wait that is not over at once."""
