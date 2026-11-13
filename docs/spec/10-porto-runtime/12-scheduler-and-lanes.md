# 12 — Scheduler & Lanes

## 1. Configuration inputs
`lanes` (int ≥ 1 or `"auto"`), `python_lanes` (derived, §7), `steal` (`"adaptive"` default | `"off"` | `"eager"`), `python_steal` (`"adaptive"` | `"off"`), `coop_budget` (default 128), `event_interval` (61, as in Tokio: poll the injector/reactor every N tasks), `global_queue_interval` (31).

## 2. Lane structure

```rust
struct Lane {
    index: u16,
    kind: LaneKind,                       // Python | RustOnly
    local: crossbeam_deque::Worker<TaskRef>, // LIFO-off FIFO local queue, capacity 256 (overflow → injector in batches of 128)
    lifo_slot: Cell<Option<TaskRef>>,
    stealer: Stealer<TaskRef>,            // shared with other lanes
    park: Parker,                         // futex-based, also wakes reactor if this lane owns it
    tick: u32,
    is_searching: AtomicBool,
    thread_state: Option<PyThreadStatePtr>, // Python lanes only
    metrics: LaneMetrics,
    deferred_decref: SegQueue<*mut PyObject>, // §25 P1
}
struct Shared {
    injector: crossbeam_deque::Injector<TaskRef>,
    py_injector: Injector<TaskRef>,       // Python tasks scheduled from outside a Python lane (e.g., Rust lane wakes)
    idle: IdleSet,                        // bitset + num_searching (Tokio's "idle" algorithm)
    driver: DriverHandle,                 // §13
    timers: TimerHandle,                  // §14
    shutdown: AtomicBool,
}
```

**M1 (single lane).** The structure above is the M2 target. M1 builds it with one lane and without stealers: the local queue is a `VecDeque` with no capacity limit and no LIFO slot; `Shared` holds the injector (a mutex-protected FIFO whose length is mirrored in an atomic, so an empty check takes no lock, as in Tokio), one `Remote` slot per lane (its parking word and its deferred-work queue), the timers and the driver. `deferred_decref` is a lane's deferred-work queue: `Handle::defer(f)` runs `f` on lane 0 at its next maintenance tick, unparking it. Remote wakes go to the injector and notify lane 0. M2 notifies an idle lane from the idle set instead, and adds the per-lane inbox of §4. `event_interval` and `global_queue_interval` are fixed at 61 and 31 until `RuntimeConfig` passes them in.

## 3. Main loop (per lane)

```
loop:
  tick += 1
  if tick % event_interval == 0: maintenance()        // poll reactor non-blocking, fire timers, drain deferred_decref
  task = next_task()
  if let Some(t) = task: run(t); continue
  task = steal_work()
  if let Some(t) = task: run(t); continue
  park()

next_task():
  if tick % global_queue_interval == 0: try injector first (fairness)
  lifo_slot.take() (max 3 consecutive LIFO runs, then force local queue to avoid starvation)
  local.pop()
  (Python lane) py_injector.steal_batch_and_pop(local)
  injector.steal_batch_and_pop(local)

run(t):
  if t.is_python() and !lane.is_python(): push to py_injector; notify a Python lane; return
  attach_if_needed()           // Python tasks only, see §6
  t.poll(lane)
```

## 4. Waking & the LIFO slot
- `wake(task)` from **the same lane** while running a task: place it in `lifo_slot`, and move the previous occupant to the local queue.
- `wake` from **another lane / non-lane thread**:
  - If the task's `owner_lane` is idle, push it to that lane's *remote inbox*. The inbox is a per-lane `SegQueue`, consumed at the start of `next_task`, and the lane is unparked.
  - Otherwise, push to the injector and unpark one idle lane if `num_searching == 0`.
- For Python tasks on a non-Python lane (GIL build), push to `py_injector` and unpark lane 0.

Soft affinity (ST-4): wakes prefer the task's `owner_lane` inbox, so tasks stay where their caches are warm unless stolen.

## 5. Stealing
- `steal="adaptive"`: a lane that finds no work enters searching state (bounded: at most `lanes/2` searchers). It picks a random start victim and steals **half** of the victim's local queue (`steal_batch_and_pop`).
- **Python task stealing** (`python_steal="adaptive"`): a Python task is stealable only if it is not pinned *and* the victim queue length ≥ `python_steal_threshold` (default 8). This prevents migration churn under light load.
- `steal="off"`: no stealing. This gives thread-per-core semantics, where each lane only runs what it spawned or was woken into its inbox.
- `steal="eager"`: Tokio default behavior (threshold 1).

## 6. Python attachment policy
- **GIL build:** lane 0 is the only Python lane. It holds the GIL while running Python tasks and keeps it across consecutive Python tasks (no release between tasks). It releases the GIL **before parking** (`PyEval_SaveThread`) and **before any blocking syscall** (`epoll_wait` with timeout > 0). Rust tasks on lane 0 run while the GIL is held; that is fine because they are short.
  - "Periodic GIL release": if other Python threads exist (`threading.active_count() > 1`, checked in maintenance), lane 0 releases and re-acquires the GIL every `gil_switch_interval` of continuous Python execution. This mirrors CPython's eval-loop drop request for fairness with user threads, and covers stretches of Rust-only tasks that never reach a bytecode that honors it. `gil_switch_interval` is `sys.getswitchinterval()` (5 ms by default) read when the run starts; it is not a `RuntimeConfig` field. Maintenance passes only count: the clock is read every 32nd pass and `threading.active_count()` only once the interval has elapsed, so a lane with no other threads pays a counter increment per 61 ticks. The release is a detach, a `yield_now` and a reattach, and restarts the interval.
  - On a **free-threaded** build there is no GIL to hand over: the periodic release is compiled out (a no-op), and the lane still detaches to park.
- **Free-threaded build:** each Python lane has a `PyThreadState` created at lane start (`PyThreadState_New` + `PyEval_RestoreThread`). It stays attached while running Python and detaches (`PyEval_SaveThread`) before parking or blocking. This is REQUIRED so stop-the-world GC can proceed (see `02` §9).

## 7. Lane count and kinds

| Build | `lanes` | Python lanes | Rust-only lanes |
|---|---|---|---|
| GIL | 1 (default) | lane 0 | — |
| GIL | N > 1 | lane 0 | 1..N-1 (TLS handshakes, compression, Rust-heavy work offloaded via `porto.spawn_rust`/ABI) |
| Free-threaded | 1 (default) | lane 0 | — |
| Free-threaded | N > 1 | all N | — |
| Free-threaded, GIL-degraded (§8) | N | lane 0 | 1..N-1 |

`"auto"` = number of physical cores (fallback: logical), capped at 64. On GIL builds `"auto"` resolves to `min(2, cores)`, because Python cannot use more. Until multi-lane scheduling lands (M2), `porto.run` accepts `"auto"` and runs on one lane, while an int above 1 raises `NotImplementedError` (`20`). Code that passes `"auto"` gets more lanes once M2 lands, with no change.

## 8. GIL-degraded mode (ST-6)
- At runtime start on a free-threaded build, the runtime installs a `sys.meta_path` finder wrapper. After each extension-module import, it checks `sys._is_gil_enabled()`.
- When the GIL flips on:
  1. Set `Shared.degraded = true`.
  2. Lanes 1..N-1 stop accepting Python tasks. They hand their Python tasks to lane 0's inbox at their next scheduling point.
  3. Log `porto: GIL was re-enabled by importing '<module>'; Python tasks now run on 1 lane. Set PYTHON_GIL=0 to force-disable at your own risk.`
- Degradation is one-way for the process lifetime.

## 9. Parking & the driver
- There is exactly one driver per runtime (§13). At most one parked lane at a time **owns the driver** (`driver_lock: TryLock`). It blocks in `epoll_wait` with a timeout equal to the next timer deadline. Other parked lanes block on their futex.
- On `unpark(lane)`: if that lane owns the driver, wake the driver via its waker (eventfd/kqueue user event). Otherwise, wake the futex.
- Maintenance (`event_interval`) polls the driver non-blocking (timeout 0) if no lane is currently blocked in it and anything is registered with it (`Driver::has_registrations`). With no registration there is no event to find, and an empty poll is a syscall (about 12 µs for `kevent` on macOS, against tens of nanoseconds per task). A wake of the driver's waker left pending meanwhile makes the next park return at once.
- No wake is lost between a lane deciding to park and a wake from another thread. Each lane has a parking word, EMPTY, PARKED or NOTIFIED (Tokio's parker). A waker publishes its work, then swaps the word to NOTIFIED, and unparks the lane (thread or driver waker) only if the word was PARKED. The lane moves EMPTY to PARKED with a compare-exchange before it blocks. If it finds NOTIFIED instead, it consumes it with a swap (never a store, which could erase a second waker's notification) and looks at its queues again. A loom model covers this (`sched::loom_tests`).

## 10. Cooperative budget
- Reset to `coop_budget` at each `poll`.
- Every ready-fast-path await and every step of a Python task (the resume after a trap resolved, or a throw) decrements it.
- At 0: the next checkpoint yields (`Pending::YieldThen`), and the task is pushed to the **back** of the local queue (not the LIFO slot).
- Rust futures use `porto_core::coop::poll_proceed(cx)` like Tokio: the lane runs each task poll under `coop::budget(coop_budget, ..)`, and `poll_proceed` returns `Pending` (waking the task) once it is spent; the unit is given back unless the operation made progress. A Python task copies the budget into its poll's frame (`coop::remaining()`), so a step touches no thread local; Rust futures it awaits draw on the lane's count separately.

## 11. Shutdown of the scheduler
1. The task registry (23 §1) closes, then cancels every task still linked in it, until none is left: this is what ends tasks kept alive only by wakers. A cancelled Python task is **closed**, not thrown `Cancelled`: a synchronous `finally` runs (and may read `Scope.tasks`, which the closed registry answers), but an `await` in it cannot (a close that awaits does not finish: the task is dropped and a warning names it, 26 §2 step 5). The graceful phase, `Cancelled` thrown into every task with up to `cancel_timeout` to finish, is `porto.run`'s CANCELLING step (26 §2), before the runtime is dropped.
2. Lanes finish the current task, then drain: the queues are dropped, which cancels the tasks in them (a Rust task's future is dropped, a Python task's coroutine is closed).
3. Lanes join.
4. Driver closed.

## 12. Required tests
- loom: wake/steal/inbox races (3 lanes, 3 tasks).
- Property test: no task is ever polled concurrently by two lanes.
- Fairness test: a self-waking task does not starve the injector, which must make progress within 2 × `global_queue_interval` ticks.
- Starvation test: a LIFO ping-pong pair doesn't starve a third task.
