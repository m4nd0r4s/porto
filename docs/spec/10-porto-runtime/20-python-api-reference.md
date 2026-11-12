# 20 — Python API Reference (normative)

Module layout. Names not listed here are private (`_`-prefixed). Signatures use PEP 695 generics notation for brevity. Stubs MUST match exactly.

## `porto` (top level)

### Running
```python
def run[*Ts, T](main: Callable[[*Ts], Awaitable[T]] | Awaitable[T], /, *args: *Ts,
                config: RuntimeConfig | None = None, **config_overrides: Any) -> T
class Runtime:
    def __init__(self, config: RuntimeConfig | None = None, **overrides: Any) -> None
    def block_on[T](self, main: Callable[[], Awaitable[T]] | Awaitable[T]) -> T   # calling thread becomes lane 0
    @property
    def root_scope(self) -> Scope            # escape hatch (ADR-007); only valid while running
    def portal(self) -> Portal
    def shutdown(self, *, timeout: float = 10.0) -> None
    def __enter__(self) -> Runtime; def __exit__(self, *exc) -> None    # shutdown on exit
def runtime() -> Runtime                     # current runtime or NoRuntimeError
def in_runtime() -> bool
```

An entry point that takes a coroutine object and refuses it (`run` inside a running runtime or with an invalid config, `Scope.spawn` on a scope that is closed or not entered, a coroutine with arguments) MUST close it before raising, so the error is the only thing reported: no later "coroutine ... was never awaited".

### RuntimeConfig (frozen dataclass; also settable via env `PORTO_<UPPER_NAME>`)
| Field | Type | Default |
|---|---|---|
| `lanes` | `int \| Literal["auto"]` | `1` |
| `io_backend` | `Literal["auto","epoll","kqueue","iocp","io_uring"]` | `"auto"` |
| `io_backend_strict` | `bool` | `False` |
| `steal` | `Literal["adaptive","off","eager"]` | `"adaptive"` |
| `python_steal` | `Literal["adaptive","off"]` | `"adaptive"` |
| `python_steal_threshold` | `int` | `8` |
| `coop_budget` | `int` | `128` |
| `blocking_threads` | `int` | `512` |
| `blocking_keepalive` | `float` | `10.0` |
| `timer_resolution` | `float` | `0.001` |
| `asyncio_compat` | `bool` | `True` |
| `anyio_backend` | `Literal["native","facade","off"]` | `"native"` |
| `handle_signals` | `bool` | `True` (only effective on the main thread) |
| `debug` | `bool` | `False` (env `PORTO_DEBUG=1`, or `python -X dev` enables) |
| `slow_step_threshold` | `float \| None` | `0.1` |
| `console` | `str \| None` | `None` (e.g. `"127.0.0.1:6669"`) |
| `metrics` | `bool` | `True` |
| `deterministic` | `DeterministicConfig \| None` | `None` (§24) |
| `graceful_timeout` | `float` | `10.0` |
| `cancel_timeout` | `float` | `5.0` |

Until multi-lane scheduling lands (M2, `12`), the runtime runs one lane: `porto.run` MUST refuse an int `lanes` above 1, from any source, with `NotImplementedError` that names the source (`lanes=4`, or `PORTO_LANES='4'`) and says to leave it out or use 1, rather than silently run on one lane. `RuntimeConfig(lanes=4)` itself stays valid. `"auto"` is accepted from any source and runs on one lane for now. Once multi-lane scheduling lands it resolves as in `12` §7, with no change for callers that pass it.

### Time
`current_time() -> float`, `monotonic() -> float`, `sleep(seconds: float, /) -> Awaitable[None]`, `sleep_until(deadline: float, /) -> Awaitable[None]`, `sleep_forever() -> Awaitable[NoReturn]`, `checkpoint() -> Awaitable[None]`, `interval(period, *, missed="skip") -> AsyncIterator[float]`. Zero, negative, infinite and NaN arguments behave as `14` §3 says.

`sleep`, `sleep_until`, `sleep_forever` and `checkpoint` return a trap (§11 §5, T1), not a coroutine: they are the hottest awaits, and a trap costs one freelisted object where a coroutine costs a frame. Await the result right away. A trap freed without ever being awaited emits `RuntimeWarning` naming the call (§11 §10).

### Structured concurrency
```python
def scope(*, timeout: float | None = None, deadline: float | None = None, shield: bool = False,
          name: str | None = None, raise_on_timeout: bool = True) -> Scope
def shield(*, timeout: float | None = None) -> Scope
def timeout(seconds: float) -> Scope                      # = scope(timeout=seconds)
def move_on_after(seconds: float) -> Scope                # = scope(timeout=seconds, raise_on_timeout=False)

class Scope:   # async context manager (and sync CM for shield-only usage)
    def spawn[*Ts, T](self, coro_or_fn: Coroutine[Any, Any, T] | Callable[[*Ts], Awaitable[T]], /, *args: *Ts,
                 name: str | None = None, pin: bool | None = None,
                 context: contextvars.Context | None = None) -> Task[T]
    async def start[T](self, fn: Callable[..., Awaitable[Any]], *args: Any, name: str | None = None) -> T
    def cancel(self, reason: str | None = None) -> None
    deadline: float          # read/write; inf if none
    shield: bool             # read/write
    @property cancel_called: bool
    @property cancelled_caught: bool
    @property timed_out: bool
    @property name: str
    @property tasks: Sequence[Task[Any]]    # live children snapshot

class Started[T]:                        # made by Scope.start, not by you
    def set(self, value: T = None) -> None   # once, from the started task; RuntimeError twice, from another task, or into a closed scope (15 §3)

class Task[T]:     # awaitable
    id: int; name: str
    def done(self) -> bool; def cancelled(self) -> bool
    def result(self) -> T                    # raises InvalidStateError if not done; re-raises failure
    def exception(self) -> BaseException | None   # InvalidStateError if not done; raises Cancelled like result() if cancelled
    def cancel(self) -> None                 # 15 §4: cancels this task only; no-op once done
    def __await__(self) -> Generator[Any, None, T]
    @property lane(self) -> int | None; @property pinned(self) -> bool   # lane: the lane it last ran on, None once finished
def current_task() -> Task[Any]          # NoRuntimeError outside a task; the object spawn returned
```

`Scope.spawn` passes only positional arguments to the function (a keyword it does not take raises `TypeError` naming `functools.partial`). `name` names the task (`Task.name`, shown in its repr); without it the task is named after the `__qualname__` of the function or coroutine it runs. `pin=True` pins the task to its lane, `pin=False` does not, and `pin=None` inherits the pinning of the spawning task (pinning is inherited by the subtree, 03). `context` runs the task in that `contextvars.Context` itself, shared with whoever else uses it; without it the task runs in a copy of the current context (11 §8). An option of the wrong type raises `TypeError` that says what to pass.

### Sync & channels
`Lock`, `Mutex[T]`, `RwLock`, `Semaphore`, `CapacityLimiter`, `Event`, `Notify`, `Condition`, `Barrier`, and the channel constructors `channel[T](capacity)`, `unbounded_channel[T]()`, `oneshot[T]()`, `broadcast[T](capacity)`, `watch[T](initial)` returning `(Sender[T], Receiver[T])`-like pairs: `channel` and `unbounded_channel` return `Sender[T]`/`Receiver[T]`, and the others `OneshotSender[T]`/`OneshotReceiver[T]`, `WatchSender[T]`/`WatchReceiver[T]` and `BroadcastSender[T]`/`BroadcastReceiver[T]`, whose `send` never waits and is not awaited (see §16 for methods).

### Buffers
`porto.Buffer(size=0)`: a growable, pooled byte buffer with the read/write buffer protocol; `capacity`, `resize(size)`, `extend(data)`; `bytes(buf)` copies (13 §6).

### Threads
`async to_thread(fn, *args, cancellable=False, limiter=None, **kw)`, `from_thread.run`, `from_thread.run_sync`, `class Portal` (`submit(fn, *args) -> concurrent.futures.Future`, `close()`).

### Exceptions (`porto.errors`, re-exported at top level)
```
PortoError(Exception)
 ├─ NoRuntimeError(PortoError, RuntimeError)
 ├─ RuntimeClosed(PortoError, RuntimeError)
 ├─ WouldBlock(PortoError)
 ├─ EndOfChannel(PortoError)
 ├─ BrokenResourceError(PortoError)
 ├─ ClosedResourceError(PortoError, OSError)
 ├─ IncompleteReadError(PortoError, EOFError)   .partial: bytes
 ├─ LimitOverrunError(PortoError)
 ├─ Lagged(PortoError)  .skipped: int
 ├─ InvalidStateError(PortoError)
 └─ RustPanic(PortoError)  .message: str
Cancelled(asyncio.CancelledError)
Terminated(SystemExit)
```

## `porto.net`, `porto.tls`, `porto.fs`, `porto.process`, `porto.signals`
As specified in §17 and §18.

## `porto.lowlevel`
`wait_readable`, `wait_writable`, `notify_closing`, `current_driver_kind`, `Trap` constructors for library authors: `trap_yield()` (the trap behind `checkpoint()`), `trap_wait_notify(notify)` (the trap behind `await notify.notified()`; `TypeError` for anything but a `Notify`); `current_lane() -> int`; `spawn_rust_lane(fn)` (GIL builds: run a callable that only calls Rust-releasing APIs; advanced). The trap constructors and `current_lane` raise `NoRuntimeError` outside a task of a porto runtime; a trap freed without being awaited warns like one from `sleep()`.

## `porto.inspect`
`tasks() -> list[TaskInfo]`, `tree() -> ScopeInfo`, `stats() -> RuntimeStats`, `dump(file=sys.stderr)`. Dataclasses are defined in §23.

## `porto.testing`
`run(main, *, seed=0, clock="virtual", autojump=True, network=None, **cfg)`, `Clock`, `net.Network`, `net.Link`, `pytest` plugin (§24).

## `porto.asyncio` (compat)
`EventLoop` (facade class), `install()` (sets asyncio's loop factory/policy for code that calls `asyncio.run`), `run_asyncio(coro)` (await asyncio-native code explicitly), `to_asyncio_future(awaitable)`.

## Typing guarantees
- `py.typed` is shipped. pyright strict passes on `tests/typing/*.py`.
- Generic channel/mutex types are subscriptable at runtime (`porto.channel[int](10)`) via `__class_getitem__`.
