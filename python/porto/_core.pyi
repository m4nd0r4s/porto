"""The native extension module, built from crates/porto-py.

The public API (re-exported by ``porto``): ``run``, ``scope``, ``Scope``, ``Task``, ``sleep``,
``sleep_until``, ``sleep_forever``, ``checkpoint``, ``current_task``, ``current_time``,
``monotonic``, ``Event``, ``Notify``, ``Lock``, ``Mutex``, ``MutexGuard``, ``Semaphore``,
``CapacityLimiter``, ``RwLock``, ``Condition``, ``Barrier``, ``Sender``, ``Receiver``, ``Buffer``
and ``__version__``
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

    The runtime's settings are ``config_overrides`` (``coop_budget=64``) over ``config`` over
    the ``PORTO_*`` environment variables over the defaults; see ``porto.RuntimeConfig``. They
    are checked before ``main`` is called. Unknown ``PORTO_*`` variables warn. Multi-lane
    scheduling is not implemented yet: ``lanes`` above 1 (from the keyword, the config or
    ``PORTO_LANES``) raises ``NotImplementedError``. ``lanes="auto"`` is accepted and runs on
    one lane for now; once multi-lane scheduling lands it picks more lanes for the machine,
    with no change to code that passes it.

    ``main`` is the first task of the runtime's root scope, a nursery: a task of the root
    scope that fails cancels it, and with it ``main``. Once ``main`` ended, the tasks still in
    the root scope are cancelled and waited for, up to ``cancel_timeout``. It then returns
    main's result or raises main's exception; if tasks of the root scope failed, it raises an
    ``ExceptionGroup`` (``BaseExceptionGroup`` if one is not an ``Exception``) of main's
    exception, first, and theirs.

    Ctrl-C raises ``KeyboardInterrupt`` out of it within about 50 ms, also while every task
    sleeps. A ``KeyboardInterrupt`` or ``SystemExit`` raised by a task nobody waits for also
    stops the run and is raised here. Tasks still running when it returns are closed. Raises
    ``RuntimeError`` when called from inside a running porto runtime. A coroutine object
    ``main`` that is refused (inside a runtime, or with an invalid config) is closed, so no
    "never awaited" warning follows the error.
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
    """Resumes the task ``seconds`` from now, never earlier on ``current_time()``.
    ``seconds <= 0`` is a checkpoint (it yields once); ``inf`` sleeps until cancelled.

    Raises ``ValueError`` for NaN.

    Returns a trap, not a coroutine: ``await`` it right away, inside ``porto.run``. A trap
    freed without being awaited emits ``RuntimeWarning`` ("never awaited").
    """

# Time: sleep_until and sleep_forever (porto-szb.33)

def sleep_until(deadline: float, /) -> Awaitable[None]:
    """Resumes the task once ``current_time()`` reaches ``deadline``, never earlier. A deadline
    already passed is a checkpoint (it yields once); ``inf`` sleeps until cancelled.

    Raises ``ValueError`` for NaN. Returns a trap, not a coroutine: ``await`` it right away,
    inside ``porto.run``. A trap freed without being awaited emits ``RuntimeWarning``.
    """

def sleep_forever() -> Awaitable[NoReturn]:
    """Never resumes the task: only a cancellation (``porto.Cancelled``) ends the wait.

    Returns a trap, not a coroutine: ``await`` it right away, inside ``porto.run``. A trap
    freed without being awaited emits ``RuntimeWarning``.
    """

def checkpoint() -> Awaitable[None]:
    """Yields to the other ready tasks once.

    Returns a trap, not a coroutine: ``await`` it right away, inside ``porto.run``. A trap
    freed without being awaited emits ``RuntimeWarning`` ("never awaited").
    """

# Structured concurrency

def scope(
    *,
    timeout: float | None = None,
    deadline: float | None = None,
    shield: bool = False,
    raise_on_timeout: bool = True,
) -> Scope:
    """A new scope; enter it with ``async with porto.scope() as s:``.

    ``timeout`` (seconds, counted from the entry) and ``deadline`` (a time on the runtime clock,
    ``porto.current_time()``) give it a deadline, the earlier of the two if both are given: once
    it passed, the scope is cancelled. Its exit then raises the builtin ``TimeoutError`` if the
    cancel cut the block short, or goes on quietly with ``raise_on_timeout=False``
    (``timed_out`` tells). 0 or less expires at once; ``inf`` never does; NaN raises
    ``ValueError``. ``shield=True`` keeps the cancels and deadlines of the scopes it is in out
    of it (its own still count)."""

def shield(*, timeout: float | None = None) -> Scope:
    """``porto.scope(shield=True, timeout=timeout)``: a scope the cancels and deadlines of the
    scopes it is in do not reach. In a ``finally:`` that must await cleanup, enter it with sync
    ``with``::

        finally:
            with porto.shield(timeout=5):
                await conn.rollback()

    A scope entered with ``with`` is a cancel scope only: spawning into it raises
    ``RuntimeError``."""

def timeout(seconds: float, /) -> Scope:
    """``porto.scope(timeout=seconds)``: the block raises ``TimeoutError`` if it is still running
    ``seconds`` after it was entered."""

def move_on_after(seconds: float, /) -> Scope:
    """``porto.scope(timeout=seconds, raise_on_timeout=False)``: the block is cancelled
    ``seconds`` after it was entered and the program goes on after it; ``timed_out`` tells
    whether it was cut short."""

@final
class Scope:
    """A nursery: every task spawned into it finishes before its ``async with`` block exits.

    If children raised, the exit raises them in an ``ExceptionGroup`` (``BaseExceptionGroup`` if
    one is not an ``Exception``), after the body's exception if the body raised. A failing child
    cancels the scope, which stops its siblings and the body.

    It is also a cancel scope: once cancelled, every task in it (and in the scopes nested in it)
    gets ``porto.Cancelled`` at each of its awaits, until it leaves the scope. A ``Cancelled``
    the block lets out is absorbed at the exit when this scope was cancelled.
    """

    def spawn(
        self,
        coro_or_fn: Coroutine[Any, Any, _T] | Callable[[*_Ts], Awaitable[_T]],
        /,
        *args: *_Ts,
        name: str | None = None,
        pin: bool | None = None,
        context: contextvars.Context | None = None,
    ) -> Task[_T]:
        """Starts a child task: a coroutine object, or an async function called with ``args``
        (only positional ones: use ``functools.partial`` for keywords). It runs in a copy of the
        current context, or in ``context`` itself if given. ``name`` names it (default: the
        function's ``__qualname__``). ``pin=True`` keeps it on the lane it starts on; without
        ``pin`` it is pinned if the spawning task is.

        Raises ``RuntimeError`` outside the block (``porto.NoRuntimeError`` outside
        ``porto.run``), ``TypeError`` for an option of the wrong type or an unknown keyword; a
        refused coroutine object is closed."""

    async def start(
        self,
        fn: Callable[..., Awaitable[object]],
        /,
        *args: Any,
        name: str | None = None,
    ) -> Any:
        """Starts ``fn(*args, started=Started())`` as a task and waits until it calls
        ``started.set(value)``, then returns ``value`` (Trio's ``nursery.start``, 15 §3). Until
        then the task runs in a hidden scope the caller waits in: what it raises is raised from
        ``start`` as it is (not in a group), and cancelling the caller cancels the task. After
        ``set`` the task is a child of this scope: it counts there, and a failure is this
        scope's.

        ``fn`` must accept the keyword ``started``; ``name`` names the task (default: the
        function's ``__qualname__``). Raises ``RuntimeError`` if the task finishes without
        calling ``started.set()``, and what ``spawn`` raises for a scope that cannot take
        tasks."""

    def _new_started(self) -> Started[Any]:
        """For ``Scope.start``: the ``Started`` that moves a task into this scope."""

    def cancel(self, reason: str | None = None) -> None:
        """Cancels the scope: every task in it, and in the scopes nested in it, gets
        ``porto.Cancelled`` at its next await; waiting tasks are woken. Safe from any thread;
        cancelling again does nothing. Cancelling before the block is entered cancels it as it
        is entered; cancelling after it exited affects no task, but ``cancel_called`` still
        turns true."""

    @property
    def cancel_called(self) -> bool:
        """Whether the scope was cancelled: ``cancel()`` was called, or a child failed."""

    @property
    def deadline(self) -> float:
        """When the scope is cancelled by its deadline, in seconds on the runtime clock
        (``porto.current_time()``); ``inf`` for none. A ``timeout=`` counts from the entry: until
        then this reads the ``deadline=`` given, or ``inf``.

        Setting it moves the deadline and re-arms the timer (from any thread); a time already
        passed cancels the scope now, ``inf`` removes the deadline. A scope already cancelled
        stays cancelled. The root scope has no deadline: setting it raises ``RuntimeError``."""

    @deadline.setter
    def deadline(self, value: float) -> None: ...
    @property
    def shield(self) -> bool:
        """Whether the cancels and deadlines of the scopes this one is in are kept out of it
        (its own still count). Settable from any thread: the tasks in it are re-evaluated at
        once, and unshielding wakes the waiting ones an outer cancel now reaches. The root scope
        cannot be shielded: setting it raises ``RuntimeError``."""

    @shield.setter
    def shield(self, value: bool) -> None: ...
    @property
    def timed_out(self) -> bool:
        """Whether the scope's deadline cut its block short: the exit absorbed a
        ``porto.Cancelled`` the deadline caused. The exit raised ``TimeoutError`` then, unless
        the scope was made with ``raise_on_timeout=False`` (``porto.move_on_after``)."""

    @property
    def cancelled_caught(self) -> bool:
        """Whether this scope's cancellation was delivered and absorbed: a ``porto.Cancelled``
        it raised left the block, or ended a child task. True also when only the children got
        it, for example when the winner of a race calls ``cancel()`` after the block finished.
        A ``Cancelled`` raised by an enclosing scope's cancellation counts for that scope, not
        this one; one the block catches and swallows does not count."""

    async def __aenter__(self) -> Self: ...
    def __enter__(self) -> Self:
        """Enters the scope as a cancel scope only (``with porto.shield():``): no await, and
        spawning into it raises ``RuntimeError``."""

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> bool: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> bool: ...
    def _waiter_count(self) -> int: ...

@final
class Started(Generic[_T]):
    """What ``Scope.start()`` passes the task it starts, as the keyword ``started``: the task
    calls ``set(value)`` once it is ready, which ends the ``start`` and returns ``value`` to its
    caller. Made by ``start``, not by you."""

    @overload
    def set(self: Started[None], value: None = None, /) -> None: ...
    @overload
    def set(self, value: _T, /) -> None:
        """Reports that the task is ready and moves it into the scope ``start`` was called on.
        Call it once, from the started task, while that scope is open: otherwise it raises
        ``RuntimeError``."""

    def _bind(self, hidden: Scope, /) -> None:
        """For ``Scope.start``: the hidden scope the task is spawned into."""

    def _was_set(self) -> bool:
        """For ``Scope.start``: whether ``set`` was called."""

    def _result(self) -> Any:
        """For ``Scope.start``: what ``set`` was given; ``RuntimeError`` if it was not called."""

    def __class_getitem__(cls, item: object, /) -> object: ...

@final
class Task(Generic[_T_co]):
    """A task: the child of a scope (what ``Scope.spawn`` returns), or the one running now
    (``porto.current_task()``). Its result is readable once it finished, which is certain once
    its scope's block exited; a task can also be awaited: ``await task``.

    ``current_task()`` returns the very object ``spawn`` returned. A task spawned by no scope
    (an internal one) has no handle until ``current_task()`` makes one, named ``Task-<id>``."""

    @property
    def id(self) -> int:
        """The task's id: unique in the process, increasing in spawn order."""

    @property
    def name(self) -> str:
        """The name given to ``spawn``, else the ``__qualname__`` of the async function or
        coroutine the task runs."""

    @property
    def pinned(self) -> bool:
        """Whether the task stays on its lane: spawned with ``pin=True``, or without ``pin``
        by a pinned task."""

    @property
    def lane(self) -> int | None:
        """The lane the task last ran on (0: a runtime has one lane until M2), ``None`` once it
        finished."""

    def done(self) -> bool:
        """Whether the task finished: it returned, raised, or was cancelled."""

    def cancelled(self) -> bool:
        """Whether the task ended cancelled: a cancellation reached it (``Task.cancel()``,
        ``Scope.cancel()``, a deadline, a failing sibling), or its runtime shut down first."""

    def result(self) -> _T_co:
        """What the task returned, or raises what it raised (``porto.Cancelled`` if it was
        cancelled); ``porto.InvalidStateError`` if it has not finished yet,
        ``porto.RuntimeClosed`` if its runtime shut down first."""

    def exception(self) -> BaseException | None:
        """What the task raised, or ``None`` if it returned. Raises like ``result()`` for a
        task that was cancelled; ``porto.InvalidStateError`` if it has not finished yet."""

    def cancel(self) -> None:
        """Cancels this task and only it, as if it ran inside a scope of its own that
        ``Scope.cancel()`` was called on (15 §4): it gets ``porto.Cancelled`` at its next
        checkpoint (a waiting task is woken) and at every one after, and so do the tasks
        spawned into scopes it entered. Its siblings and the scope it was spawned into are not
        cancelled, and the ``Cancelled`` that ends it is not a failure: nothing is raised for
        it by the scope's exit, and ``cancelled()`` turns true. A shield the task entered holds
        the cancel off until it is left. Does nothing for a task that finished. Safe from any
        thread."""

    def __await__(self) -> Generator[Any, Any, _T_co]:
        """Waits for the task, then evaluates to its result or raises its exception. A task that
        finished already completes at once. Raises ``porto.NoRuntimeError`` outside a porto
        task, and ``RuntimeError`` if a task awaits itself."""

    def __class_getitem__(cls, item: object, /) -> object: ...

def current_task() -> Task[Any]:
    """The ``Task`` of the task that calls it: the object ``Scope.spawn`` returned, or, for a
    task with no handle, a new one (named ``Task-<id>``) that its next calls return again.
    Raises ``porto.NoRuntimeError`` outside a task of a porto runtime."""

# Low level (``porto.lowlevel``)

def trap_yield() -> _Trap:
    """The trap behind ``porto.checkpoint()``: awaiting it yields to the other tasks once, with
    the cancellation check of every checkpoint. Raises ``porto.NoRuntimeError`` outside
    ``porto.run``; a trap freed without being awaited emits ``RuntimeWarning``."""

def trap_wait_notify(notify: Notify, /) -> _Trap:
    """The trap behind ``await notify.notified()``: takes a stored permit, or waits for a
    ``notify_one``/``notify_all`` that comes after the ``await`` started. Raises
    ``porto.NoRuntimeError`` outside ``porto.run``, ``TypeError`` for anything but a
    ``porto.Notify``."""

def current_lane() -> int:
    """The index of the lane the calling task runs on (0: a runtime has one lane until M2).
    Raises ``porto.NoRuntimeError`` outside a task of a porto runtime."""

# Synchronization (16-sync-primitives-and-channels.md §1)

@final
class Buffer:
    """A growable byte buffer owned by porto, readable and writable through the buffer protocol
    (``memoryview(buf)``), its storage pooled per lane in size classes of 4, 16, 64 and 256 KiB
    (13-io-driver.md §6). A socket read fills one without copying; ``bytes(buf)`` copies.

    While a view is exported (a ``memoryview``, or an I/O operation using it) the buffer cannot
    change size: ``resize`` and ``extend`` raise ``BufferError``."""

    def __init__(self, size: int = 0, /) -> None:
        """A buffer of ``size`` zero bytes."""

    def __len__(self) -> int: ...
    def __bytes__(self) -> bytes:
        """A copy of the contents."""

    def __buffer__(self, flags: int, /) -> memoryview: ...
    def __release_buffer__(self, view: memoryview, /) -> None: ...
    @property
    def capacity(self) -> int:
        """How many bytes the buffer holds before its contents move to larger storage."""

    def resize(self, size: int, /) -> None:
        """Truncates, or appends zero bytes, to make the length ``size``."""

    def extend(self, data: Buffer | bytes | bytearray | memoryview, /) -> None:
        """Appends the bytes of ``data``."""

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

        Nothing happens until it is awaited; ``await`` it inside ``porto.run``. One freed
        without being awaited emits ``RuntimeWarning``.
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

        Nothing happens until it is awaited: one that is never awaited consumes no permit (and
        emits ``RuntimeWarning`` when freed). A
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

        Nothing happens until it is awaited: one that is never awaited takes no lock (and emits
        ``RuntimeWarning`` when freed). A task cancelled or closed before it observed the lock
        passes it on. ``async with lock:`` waits the same way.
        """

    def _acquire_then(self, on_acquired: Callable[[], _T], /) -> _SyncWait[_T]:
        """Internal, for tests: ``acquire()``, then ``on_acquired()`` in the same step; the
        await evaluates to its result. If it raises, the lock is released first."""

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

    def __aenter__(self) -> Awaitable[None]:
        """The entry ``async with lock:`` awaits: lazy like ``acquire()``, and named
        ``Lock.__aenter__()`` in the warning if it is never awaited."""
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...

@final
class MutexGuard(Generic[_T]):
    """Access to a ``porto.Mutex``'s value while its lock is held; what ``async with`` gives.
    It stops working when the block exits (``RuntimeError``).

    ``Mutex.__aenter__()`` returns the guard itself, not entered yet: awaiting it once takes
    the lock and evaluates to the guard (``async with`` does both)."""

    def __await__(self) -> Generator[_Trap, object, Self]: ...
    @property
    def value(self) -> _T:
        """The value the mutex owns."""

    def set(self, value: _T, /) -> None:
        """Replaces the value the mutex owns."""

@final
class Mutex(Generic[_T]):
    """``porto.Mutex[T](value)``: an async lock that owns ``value``.

    ``async with mutex as guard:`` waits for the lock (fair FIFO, like ``porto.Lock``), then
    ``guard.value`` reads the value and ``guard.set(new)`` replaces it. Not reentrant: entering
    it again inside its own block waits forever. Like ``Lock``, nothing is taken until the
    entry is awaited, and a task cancelled or closed while it waits for the mutex never holds
    it. Thread-safe.
    """

    def __init__(self, value: _T, /) -> None: ...
    def locked(self) -> bool:
        """Whether the lock is held (or being handed to a waiter)."""

    def __aenter__(self) -> MutexGuard[_T]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...

# Semaphore, CapacityLimiter, RwLock, Condition, Barrier (16-sync-primitives-and-channels.md §1)

@final
class Semaphore:
    """``porto.Semaphore(value)``: ``value`` permits, acquired and released several at a time.

    Fair: waiters are granted in arrival order, and one that wants more permits than are free
    blocks the ones behind it (so a large request is never starved by small ones). Bounded:
    releasing more permits than are acquired raises ``ValueError``. A waiter cancelled or
    closed before it observed its permits gives them back. Thread-safe.
    """

    def __init__(self, value: int, /) -> None:
        """``value`` from 1 to 2**31 - 1; ``ValueError`` otherwise (to signal between tasks,
        use ``porto.Event`` or ``porto.Notify``)."""

    def acquire(self, permits: int = 1, /) -> _SyncWait[None]:
        """Completes once the task holds ``permits`` permits; without suspending if nobody
        waits and enough are free. Lazy like ``Lock.acquire()``. ``ValueError`` unless
        ``permits`` is from 1 to the semaphore's value."""

    def try_acquire(self, permits: int = 1, /) -> bool:
        """Takes ``permits`` permits if nobody waits and enough are free right now."""

    def release(self, permits: int = 1, /) -> None:
        """Gives back ``permits`` permits, granting the longest waiting tasks. ``ValueError``
        if fewer are acquired."""

    @property
    def available(self) -> int:
        """The free permits."""

    def blocking_acquire(self, permits: int = 1, /) -> None:
        """Blocks the calling thread until it holds ``permits`` permits. Only from threads
        that do not run porto: raises ``RuntimeError`` on a lane thread."""

    def __aenter__(self) -> Awaitable[None]:
        """The entry ``async with semaphore:`` awaits: one permit, lazily (like ``Lock``)."""
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...

@final
class CapacityLimiter:
    """``porto.CapacityLimiter(total_tokens)``: at most ``total_tokens`` borrowers at a time,
    each holding one token (AnyIO's ``CapacityLimiter`` API).

    The default borrower is the running porto task; ``*_on_behalf_of(borrower)`` borrows for
    any object (compared by identity). A borrower holds at most one token: borrowing again
    raises ``RuntimeError``, and so does releasing a token it does not hold. Tokens are granted
    in arrival order. Thread-safe.
    """

    def __init__(self, total_tokens: float, /) -> None:
        """An ``int`` of at least 1, or ``math.inf``."""

    @property
    def total_tokens(self) -> float:
        """The most tokens borrowed at once (``math.inf`` for no limit). Raising it grants
        waiting tasks; lowering it takes tokens back as they are released."""

    @total_tokens.setter
    def total_tokens(self, value: float) -> None: ...
    @property
    def borrowed_tokens(self) -> int:
        """How many tokens are borrowed."""

    @property
    def available_tokens(self) -> float:
        """How many tokens are free (``math.inf`` for no limit)."""

    def acquire(self) -> _SyncWait[None]:
        """Completes once the running task borrows a token. Lazy."""

    def acquire_on_behalf_of(self, borrower: object, /) -> _SyncWait[None]:
        """Completes once ``borrower`` borrows a token. Lazy."""

    def acquire_nowait(self) -> None:
        """Borrows a token for the running task now; ``porto.WouldBlock`` if none is free."""

    def acquire_on_behalf_of_nowait(self, borrower: object, /) -> None:
        """Borrows a token for ``borrower`` now; ``porto.WouldBlock`` if none is free."""

    def release(self) -> None:
        """Gives back the running task's token."""

    def release_on_behalf_of(self, borrower: object, /) -> None:
        """Gives back ``borrower``'s token."""

    def __aenter__(self) -> Awaitable[None]:
        """The entry ``async with limiter:`` awaits: a token for the running task, lazily."""
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...

@final
class RwLock:
    """``porto.RwLock()``: any number of readers, or one writer.

    ``async with lock.read():`` and ``async with lock.write():``. Writer-preferring: a reader
    never takes the lock while a writer waits. Waiters are served in arrival order, consecutive
    readers together, so neither readers nor writers starve. Not reentrant; holders are not
    recorded. A waiter cancelled or closed before it observed the lock passes it on.
    Thread-safe.
    """

    def __init__(self) -> None: ...
    def read(self) -> _SyncEntry:
        """``async with lock.read():`` holds a read lock for the block."""

    def write(self) -> _SyncEntry:
        """``async with lock.write():`` holds the write lock for the block."""

    def acquire_read(self) -> _SyncWait[None]:
        """Completes once the task holds a read lock; ``release_read()`` releases it."""

    def acquire_write(self) -> _SyncWait[None]:
        """Completes once the task holds the write lock; ``release_write()`` releases it."""

    def try_acquire_read(self) -> bool:
        """Takes a read lock if no writer holds or waits for it right now."""

    def try_acquire_write(self) -> bool:
        """Takes the write lock if it is free and nobody waits right now."""

    def release_read(self) -> None:
        """Releases a read lock; ``RuntimeError`` if no reader holds it."""

    def release_write(self) -> None:
        """Releases the write lock; ``RuntimeError`` if no writer holds it."""

    @property
    def readers(self) -> int:
        """How many readers hold the lock."""

    def write_locked(self) -> bool:
        """Whether a writer holds the lock (or is being handed it)."""

    def blocking_acquire_read(self) -> None:
        """Blocks the calling thread until it holds a read lock. Only from threads that do not
        run porto."""

    def blocking_acquire_write(self) -> None:
        """Blocks the calling thread until it holds the write lock. Only from threads that do
        not run porto."""

@final
class Condition:
    """``porto.Condition(lock=None)``: a ``porto.Lock`` plus tasks waiting to be notified.

    ``async with condition:`` takes the lock; inside, ``await condition.wait()`` releases it,
    waits for ``notify()`` or ``notify_all()``, and takes it again, also when the task is
    cancelled meanwhile (the ``Cancelled`` is raised once the lock is held again). A
    notification goes to the longest waiting tasks and is not stored. A task cancelled after
    it was notified passes the notification on.
    """

    def __init__(self, lock: Lock | None = None) -> None:
        """``lock``: the ``porto.Lock`` to use; a new one by default."""

    @property
    def lock(self) -> Lock:
        """The lock the condition uses."""

    def acquire(self) -> _SyncWait[None]:
        """Completes once the task holds the lock."""

    def try_acquire(self) -> bool:
        """Takes the lock if it is free right now."""

    def release(self) -> None:
        """Releases the lock; ``RuntimeError`` if it is not held."""

    def locked(self) -> bool:
        """Whether the lock is held."""

    def wait(self) -> _ConditionWait[None]:
        """Releases the lock, waits until notified, and takes the lock again. ``RuntimeError``
        if the lock is not held. Lazy."""

    def wait_for(self, predicate: Callable[[], _T], /) -> _ConditionWait[_T]:
        """Waits (as ``wait()``) until ``predicate()`` is true; evaluates to its result. It is
        checked first, and after each notification, with the lock held."""

    def notify(self, n: int = 1, /) -> None:
        """Wakes up to ``n`` waiting tasks. ``RuntimeError`` if the lock is not held."""

    def notify_all(self) -> None:
        """Wakes every waiting task. ``RuntimeError`` if the lock is not held."""

    def __aenter__(self) -> Awaitable[None]: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...

@final
class Barrier:
    """``porto.Barrier(parties)``: ``await barrier.wait()`` waits until ``parties`` tasks
    arrived, then all go on, and the barrier is ready for the next round.

    The last arrival of a round leads: its ``wait()`` returns ``True``, the others ``False``. A
    task cancelled while it waits leaves its round, which then waits for one more arrival.
    """

    def __init__(self, parties: int, /) -> None:
        """``parties`` of at least 1."""

    def wait(self) -> _SyncWait[bool]:
        """Arrives and completes once the round's last task arrived: ``True`` for that one.
        Lazy: nothing happens until it is awaited."""

    def blocking_wait(self) -> bool:
        """Blocks the calling thread until the round completes. Only from threads that do not
        run porto."""

    @property
    def parties(self) -> int:
        """How many tasks each round waits for."""

    @property
    def n_waiting(self) -> int:
        """How many tasks wait in the current round."""

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
        if this sender was closed. Nothing happens until it is awaited; one freed unawaited
        emits ``RuntimeWarning``."""

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
        if this receiver was closed. Nothing happens until it is awaited; one freed unawaited
        emits ``RuntimeWarning``."""

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

# Oneshot channel (porto.oneshot): one value, from one sender to one receiver. Sending never
# waits, so ``send`` is a plain call.

@final
class OneshotSender(Generic[_T_contra]):
    """The sending end of a oneshot channel: sends one value, then is used up.

    Closing it (``close()``, ``async with``, or dropping the last reference) without sending
    ends the channel for the receiver.
    """

    def send(self, value: _T_contra, /) -> None:
        """Sends ``value``; never waits, so it is not awaited. Raises
        ``porto.BrokenResourceError`` if the receiver is gone, ``porto.ClosedResourceError`` if
        this sender was closed or already sent."""

    def close(self) -> None:
        """Closes this sender without sending: the receiver sees ``porto.EndOfChannel``.
        Closing twice, or after sending, does nothing."""

    def is_closed(self) -> bool:
        """Whether a send would fail: this sender was closed or used up, or the receiver is
        gone."""

    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None: ...

@final
class OneshotReceiver(Generic[_T_co]):
    """The receiving end of a oneshot channel.

    Closing it (``close()``, or dropping the last reference) makes sends raise
    ``porto.BrokenResourceError`` and drops a value that was sent but not received.
    """

    def recv(self) -> Awaitable[_T_co]:
        """The value, waiting until it is sent. Raises ``porto.EndOfChannel`` if the sender was
        closed or dropped without sending, or the value was already received,
        ``porto.ClosedResourceError`` if this receiver was closed. Nothing happens until it is
        awaited; one freed unawaited emits ``RuntimeWarning``."""

    def try_recv(self) -> _T_co:
        """The value if it was sent; raises ``porto.WouldBlock`` if not yet, and the errors of
        ``recv``."""

    def blocking_recv(self) -> _T_co:
        """``recv`` from a thread that does not run porto, blocking that thread until the
        value is sent. Raises ``RuntimeError`` on a porto lane thread."""

    def close(self) -> None:
        """Closes this receiver. Receives already waiting through it raise
        ``porto.ClosedResourceError``. Closing twice does nothing."""

    def __aiter__(self) -> Self: ...
    def __anext__(self) -> Awaitable[_T_co]:
        """``async for value in rx``: yields the one value, then the loop ends."""

def _oneshot() -> tuple[OneshotSender[Any], OneshotReceiver[Any]]: ...

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
class _SyncEntry:
    """The lazy entry of ``async with`` on a ``Semaphore``, a ``CapacityLimiter``, and
    ``RwLock.read()``/``RwLock.write()`` (for those two, also the context manager)."""

    def __await__(self) -> Generator[_Trap, object]: ...
    def send(self, value: object, /) -> _Trap: ...
    def close(self) -> None: ...
    async def __aenter__(self) -> None: ...
    def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> Awaitable[None]: ...

@final
class _ConditionWait(Generic[_T_co]):
    """The awaitable of ``Condition.wait()`` and ``Condition.wait_for()``."""

    def __await__(self) -> Generator[_Trap, object, _T_co]: ...
    def __iter__(self) -> Generator[_Trap, object, _T_co]: ...
    def __next__(self) -> _Trap: ...
    def send(self, value: object, /) -> _Trap: ...
    def throw(
        self,
        typ: type[BaseException] | BaseException,
        val: object = None,
        tb: TracebackType | None = None,
        /,
    ) -> _Trap: ...
    def close(self) -> None: ...

@final
class _LockLost(BaseException):
    """Raised in place of ``GeneratorExit`` by a ``Condition.wait()`` closed while another task
    holds the lock; the lock's ``async with`` exit releases nothing for it and turns it back into
    ``GeneratorExit``."""

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

def _hidden_scope() -> Scope:
    """The scope ``Scope.start`` runs a task in until it reports: a nursery whose exit raises
    a single failure as it is, not in a group."""

def _root_scope() -> Scope:
    """The root scope of the running ``porto.run`` (what ``Runtime.root_scope`` will return):
    its tasks outlive the scopes ``main`` opens, and one that fails cancels ``main``.
    ``porto.NoRuntimeError`` outside ``porto.run``."""

def _spawn(coro: Coroutine[Any, Any, object], /) -> None:
    """Starts ``coro`` as a task on the running runtime. An ``Exception`` it raises goes to
    ``sys.unraisablehook``; any other ``BaseException`` stops the run (``_run`` raises it)."""

def _yield_trap() -> _Trap:
    """A trap that yields to the scheduler once."""

def _task_handles_created() -> int:
    """Test hook: how many ``porto.Task`` objects were made so far in this process. An internal
    spawn makes none; ``Scope.spawn`` makes one per task, ``current_task()`` one for a task
    with none."""

def _sleep_trap(delay: float, /) -> _Trap:
    """A trap that resumes the task ``delay`` seconds from now, never earlier (``delay <= 0``:
    a deadline already passed, so a yield). Reads the runtime clock, so it raises
    ``porto.NoRuntimeError`` outside ``porto.run``; ``ValueError`` for NaN."""

def _armed_timers() -> int:
    """Test hook: how many timers are armed on the running runtime's wheel, its own included
    (the lane's signal check). A cancelled sleep leaves none behind. Raises
    ``porto.NoRuntimeError`` outside ``porto.run``."""

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
    """Test hook: a wait queue with permits. ``wait()`` is a ``Notified`` trap on it."""

    def __init__(self) -> None: ...
    def wait(self) -> _Trap:
        """A trap that takes a stored permit, or waits until a notify grants the awaiting task's
        wait node."""

    def release(self) -> None:
        """Grants the longest waiting task, or stores a permit if none waits (a lock's
        release)."""

    @property
    def permits(self) -> int:
        """The permits stored for the next waits."""

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
    up first (T3), or at its next wait that is not over at once. Also for waits a cancellation
    does not give up (``ScopeExit``)."""
