# 16 — Synchronization Primitives & Channels

All primitives are implemented in Rust (`porto-core::sync`) and are **thread-safe**. They may be shared across lanes and used from Rust tasks. From non-runtime threads, they are used via `blocking_*` methods, which are never allowed on lane threads (they raise `RuntimeError`). A `blocking_*` wait runs with the thread detached from Python and does not check for signals: Ctrl-C does not interrupt it (the `KeyboardInterrupt` is raised once the wait is over).

Waiting tasks use intrusive `WaitNode`s embedded in the task (PyTask: `wait_node`; RustTask: inside the future). Queues are FIFO (fair) unless stated otherwise.

## 1. Primitives

| Python | Rust | Semantics |
|---|---|---|
| `porto.Lock()` | `sync::Mutex<()>` | async, fair FIFO handoff, non-reentrant; `async with lock:`; `await acquire()`, `release()`; `locked()`; `try_acquire() -> bool`; `blocking_acquire()`. The holder is not recorded (as in `asyncio.Lock`): a `release()` by a task that does not hold the lock, or a second `release()`, releases it from under the holder and breaks mutual exclusion; only a release of an unlocked lock raises `RuntimeError` |
| `porto.Mutex[T](value)` | `porto.Lock` + the value | lock that owns the value: `async with m as v:` gives a `MutexGuard[T]`: `v.value`, `v.set(new)`, unusable once the block exits; `locked()`; encourages correct free-threaded code |
| `porto.RwLock()` | `sync::RwLock` | writer-preferring |
| `porto.Semaphore(n)` | `sync::Semaphore` | `acquire(k=1)`, `release(k=1)`, `async with`, `available` |
| `porto.CapacityLimiter(n)` | Semaphore + owner tracking | AnyIO-compatible; per-task borrow |
| `porto.Event()` | `sync::Event` | `set()`, `is_set()`, `await wait()`, `blocking_wait()`. Not clearable (Trio semantics); for clearable events use `watch` |
| `porto.Notify()` | `sync::Notify` | `notify_one()` (stores a permit if nobody waits), `notify_all()`, `await notified()`, `blocking_notified()` |
| `porto.Condition(lock=None)` | | `wait()`, `wait_for(pred)`, `notify(n)`, `notify_all()` |
| `porto.Barrier(n)` | | `await wait() -> bool` (True for the leader) |

Cancellation: a cancelled waiter is removed from the queue. If a cancelled waiter had **already been granted** the lock or permit (a race between grant and cancel), it passes the grant on to the next waiter. This invariant is tested with loom.

Awaiting: `wait()`, `notified()` and `acquire()` return lazy awaitables; nothing is taken until they are awaited, so one created and never awaited holds no lock and consumes no permit (it emits `RuntimeWarning` when freed, `11` §10). A grant is taken only in the step that resumes the coroutine with it: a wait that completes at once but finds the task's budget spent (T2) yields first and takes the grant afterwards, never the other way round. A task closed or cancelled at any suspension point therefore never holds a grant it did not observe.

`async with` on a `Lock` or `Mutex` follows the same rule: `__aenter__()` returns the same lazy wait as `acquire()` (for a `Mutex`, one that evaluates to the guard) and takes nothing itself. The statement awaits it at once, and its first resume takes a free lock without suspending (a T2 synchronous completion: it raises `Cancelled` first if the task is cancelled, and spends a unit of the budget). An exception between the call and the await (from 3.14 a profile or trace hook runs there, and a `KeyboardInterrupt` can land there) therefore leaves the lock free, and an `__aenter__()` dropped unawaited takes nothing and warns like `acquire()`.

The result of an await can outlive the last reference the task holds, until the task next suspends or ends. On 3.12+ GIL builds, a porto awaitable that completes with a value other than `None` can return it through one reused `StopIteration` (`11` §6), which keeps the latest such result until the task gives its lane up: after `async with mutex as guard:` and `del guard` in the same step, the guard (and the mutex it references) is freed only at the task's next suspension. Code MUST NOT rely on an await's result being freed before the task next suspends. 3.11 and free-threaded builds do not keep it.

## 2. Channels

```python
tx, rx = porto.channel[T](capacity: int)         # bounded MPMC (clone both ends)
tx, rx = porto.unbounded_channel[T]()            # explicit opt-in
tx, rx = porto.oneshot[T]()
tx, rx = porto.broadcast[T](capacity)            # each rx gets every msg; lagging → porto.Lagged(n)
tx, rx = porto.watch[T](initial)                 # latest-value; rx.changed(), rx.borrow()
```

### 2.1 Sender API
`await tx.send(v)` (waits when full), `tx.try_send(v)` (raises `porto.WouldBlock`), `tx.clone()`, `tx.close()`, `async with tx:` (closes on exit), `tx.blocking_send(v)` (non-lane threads only), `tx.capacity`, `tx.is_closed()`.

### 2.2 Receiver API
`await rx.recv()` (raises `porto.EndOfChannel` when closed and drained), `rx.try_recv()`, `async for v in rx:`, `rx.clone()`, `rx.close()`, `rx.blocking_recv()`, `await rx.recv_many(max)` → `list[T]` (batching, important for throughput).

### 2.3 Closure semantics
The channel closes when all senders are closed or dropped (receivers see the end after draining), or when all receivers are dropped (sends raise `porto.BrokenResourceError`). Dropping a handle is equivalent to closing it, and works on refcount release. Using a handle that was itself closed raises `porto.ClosedResourceError` (the program closed it) rather than `EndOfChannel` or `BrokenResourceError` (the other side went away). Full and empty `try_send`/`try_recv` raise `porto.WouldBlock`. `channel()` without a capacity is a `TypeError` that names `unbounded_channel()`.

### 2.4 Implementation notes
- Bounded MPMC: a Vyukov array-based queue (the `crossbeam::ArrayQueue` design, vendored so that loom models its atomics) plus two waiter lists (senders and receivers), each a wait queue under its own small mutex. Fast path: lock-free push/pop, then a `SeqCst` fence and a load of the other side's waiter hint; the lock is taken only when someone waits. Wake exactly one counterpart; a woken waiter retries (a waiter not yet queued may take the value or room first), and a woken waiter cancelled before retrying passes the wake on.
- Unbounded: a deque under a mutex; senders never wait, receivers wait as for the bounded channel.
- Python waits: `await tx.send(v)` / `await rx.recv()` do nothing until awaited, then try the fast path; if they must wait, the task parks its embedded `wait_node` on the waiter list (a `Notified` trap whose target is the handle, no allocation besides the trap) and retries when woken. The waiter's last look under the lock checks the buffer's length instead of pushing or popping, with the same hint-and-fence handshake, so the value never has to travel through the wake.
- Closing a handle (`close()`, or dropping it) is per handle. A task or thread already waiting through that handle wakes and raises `porto.ClosedResourceError`; waiters on other handles keep waiting. A send that races with the last receiver's close may succeed; its value is dropped with the channel.
- Elements are `PyObj`. Moving an object between lanes requires no copy; the free-threaded refcount handles ownership. On the GIL build, all Python tasks are on lane 0 anyway.
- Rust-side channels (`porto::sync::mpsc`) are generic over `T: Send`. The Python channel is `T = PyObj`.

## 3. Select (choice over several events)
`porto.select(*branches)` is deliberately **not** provided in v1. Users write a scope with competing tasks plus `scope.cancel()`, or use `rx.recv_many`. Rationale: select-style cancellation of partially completed ops is the main source of lost-message bugs in Tokio. Revisit after v1 (`55`).
