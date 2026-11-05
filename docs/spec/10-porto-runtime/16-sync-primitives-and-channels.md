# 16 — Synchronization Primitives & Channels

All primitives are implemented in Rust (`porto-core::sync`) and are **thread-safe**. They may be shared across lanes and used from Rust tasks. From non-runtime threads, they are used via `blocking_*` methods, which are never allowed on lane threads (they raise `RuntimeError`).

Waiting tasks use intrusive `WaitNode`s embedded in the task (PyTask: `wait_node`; RustTask: inside the future). Queues are FIFO (fair) unless stated otherwise.

## 1. Primitives

| Python | Rust | Semantics |
|---|---|---|
| `porto.Lock()` | `sync::Mutex<()>` | async, fair FIFO handoff, non-reentrant; `async with lock:`; `locked()`; `try_acquire() -> bool` |
| `porto.Mutex[T](value)` | `sync::Mutex<PyObj>` | lock that owns the value: `async with m as v:` gives access; `v.set(new)`; encourages correct free-threaded code |
| `porto.RwLock()` | `sync::RwLock` | writer-preferring |
| `porto.Semaphore(n)` | `sync::Semaphore` | `acquire(k=1)`, `release(k=1)`, `async with`, `available` |
| `porto.CapacityLimiter(n)` | Semaphore + owner tracking | AnyIO-compatible; per-task borrow |
| `porto.Event()` | `sync::Event` | `set()`, `is_set()`, `await wait()`, `blocking_wait()`. Not clearable (Trio semantics); for clearable events use `watch` |
| `porto.Notify()` | `sync::Notify` | `notify_one()` (stores a permit if nobody waits), `notify_all()`, `await notified()`, `blocking_notified()` |
| `porto.Condition(lock=None)` | | `wait()`, `wait_for(pred)`, `notify(n)`, `notify_all()` |
| `porto.Barrier(n)` | | `await wait() -> bool` (True for the leader) |

Cancellation: a cancelled waiter is removed from the queue. If a cancelled waiter had **already been granted** the lock or permit (a race between grant and cancel), it passes the grant on to the next waiter. This invariant is tested with loom.

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
The channel closes when all senders are closed or dropped (receivers see the end after draining), or when all receivers are dropped (sends raise `porto.BrokenResourceError`). Dropping a handle is equivalent to closing it, and works on refcount release.

### 2.4 Implementation notes
- Bounded MPMC: a Vyukov array-based queue (the `crossbeam::ArrayQueue` design, vendored so that loom models its atomics) plus two waiter lists (senders and receivers), each a wait queue under its own small mutex. Fast path: lock-free push/pop, then a `SeqCst` fence and a load of the other side's waiter hint; the lock is taken only when someone waits. Wake exactly one counterpart; a woken waiter retries (a waiter not yet queued may take the value or room first), and a woken waiter cancelled before retrying passes the wake on.
- Unbounded: a deque under a mutex; senders never wait, receivers wait as for the bounded channel.
- Closing a handle (`close()`, or dropping it) is per handle. A send that races with the last receiver's close may succeed; its value is dropped with the channel.
- Elements are `PyObj`. Moving an object between lanes requires no copy; the free-threaded refcount handles ownership. On the GIL build, all Python tasks are on lane 0 anyway.
- Rust-side channels (`porto::sync::mpsc`) are generic over `T: Send`. The Python channel is `T = PyObj`.

## 3. Select (choice over several events)
`porto.select(*branches)` is deliberately **not** provided in v1. Users write a scope with competing tasks plus `scope.cancel()`, or use `rx.recv_many`. Rationale: select-style cancellation of partially completed ops is the main source of lost-message bugs in Tokio. Revisit after v1 (`55`).
