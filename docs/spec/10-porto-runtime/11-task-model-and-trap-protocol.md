# 11 — Task Model & Trap Protocol (normative core)

This file is the heart of Porto. Implement it first and benchmark it (milestone M1) before anything else.

## 1. Two kinds of tasks, one header

```rust
#[repr(C)]
pub struct Header {
    state: AtomicU32,            // see §3
    owner_lane: AtomicU16,       // last lane that ran it (soft affinity)
    flags: u16,                  // PIN, PYTHON, DETACHED_DROP, ...; fixed at spawn
    vtable: &'static TaskVTable, // poll, drop, dealloc, schedule, type-name hooks
    id: u64,                     // monotonically increasing, never reused
    scope: Option<TaskScope>,    // spawning scope: one thin pointer, owns a counted ref
    queue_next: UnsafeCell<*const Header>, // intrusive run-queue link (injector)
    registry: RegistryNode,      // intrusive list for introspection (§23)
}
```
- **RustTask<F: Future>**: Header + scheduler handle + `F` (then its output, in the same slot) + join waker. Polled via `Future::poll`.
- **PyTask**: Header + PyTask body (§2). Its `poll` implementation performs **steps** (§4).

The two 16-bit fields sit in the padding after `state`, so the header is 56 B on 64-bit targets (vtable first would pad it to 64 B).

`scope` links the type-erased header to a `ScopeNode<E>`, which is generic over the failure type it collects. It is a thin pointer to the node, whose first field (`repr(C)`) is a `&'static` vtable for its `E`, so the header stays one word and a task outside any scope pays one null check. The pointer is a counted reference to the node, set at spawn (after `live_children` was incremented) and taken exactly once when the task ends: by the lane completing it (completed, cancelled or panicked), or by `dealloc` when the task is freed without completing (it was idle when its last reference went). Either way the scope hears of the end once, then the reference is released.

Target (ADR-034): `size_of::<Header>() <= 64` including the registry node, `size_of::<PyTaskBody>() <= 192` including the embedded timer entry and wait node (excluding the Python coroutine).

The 64 B budget counts the header only. The scheduler handle a task stores right after it (the `Schedule` implementation a wake pushes through; 8 B, one `Arc`, for the built-in scheduler) belongs to the body. On 64-bit targets it fills the last 8 B of the header's cache line, so a wake reads one line; a layout test keeps it at offset 56 and the body at 64.

Reference counting: tasks are `Arc`-like with a manual refcount packed in `state` (as in Tokio, from `REF_ONE = 1 << 7` here: one more lifecycle bit than Tokio's six). Wakers, run-queue entries and the join handle each own one reference.

## 2. PyTask body

```rust
struct PyTaskBody {
    coro: *mut ffi::PyObject,            // strong ref; NULL when finished
    context: *mut ffi::PyObject,         // contextvars.Context (strong)
    resume: Resume,                      // value to send on next step
    pending: Pending,                    // what the task waits for (inline small-enum)
    cancel_stack: SmallVec<[ScopeRef; 2]>, // scopes entered *by this task* (cancel scopes)
    budget: u8,
    result: TaskOutcome,                 // Ok(PyObject) | Err(PyObject) | Cancelled
    join: JoinWaiters,                   // intrusive list of awaiting tasks
    py_handle: *mut ffi::PyObject,       // weakly-cached porto.Task wrapper (lazily created)
    stats: TaskStats,                    // first_poll, total_poll_ns, steps, last_step_start
    timer: TaskTimer,                    // embedded wheel entry + deadline (§6)
    wait_node: WaitNode,                 // embedded waiter node (§6)
}
enum Resume { None, Value(PyObj), Throw(PyObj) }
```

A PyTask has no output stage and no join-handle waker slot (a RustTask has both): the body keeps its outcome and its joiners itself. In porto-core it is a `TaskBody` (`Handle::spawn_body`), polled with the same state transitions as a RustTask; a body cancelled instead of polled (its run-queue entry dropped at shutdown) records `Cancelled` and closes `join`.

The fields above are logical. The layout packs them so the body is 64 B without its two embedded nodes (168 B with them on 64-bit targets: `TaskTimer` 56 B, `WaitNode` 48 B):

- `resume` and `result` share one payload word with a one-byte kind: the resume value exists only while the task runs, the outcome only once it finished.
- `pending` is a one-byte kind plus one payload word, for every kind: a Python object (`YieldThen`, `AsyncioFuture`), a deadline (`Sleep`, also armed in the timer), or one pointer. Pointers to trait objects keep their vtable in the allocation (a thin box for `Future` and `Io`, a thin `Arc` for the primitive a `Notified` waits on), and a `TaskBodyRef` is one word because the body offset sits in the task vtable. The `_Trap` object stores its request the same way, so it stays two words.
- `budget` lives in the poll's frame, not in the body: it is reset at every poll anyway, and `checkpoint_if_needed` (T2) reaches it there, through the lane's record of the running task.
- `cancel_stack` is the innermost entered scope plus a one-byte depth. Scopes a task enters nest (each one's parent is the scope the task was in), so the rest of the stack is that scope's first `depth - 1` ancestors; scopes exit in reverse order.
- `join` is one word: the head of an intrusive list of `WaitNode`s, a spin-lock bit and a CLOSED bit. Completion stores the outcome, then closes the list (waking every waiter); a waiter reads the outcome only after its wait returned ready.
- `stats` counts steps. The timing fields need a clock read per step, too slow for the hot path; they come with the task registry (§23), behind its switch.

## 3. Task state machine

Bits in `state: AtomicU32`:

| Bit | Name | Meaning |
|-----|------|---------|
| 0 | RUNNING | A lane is currently stepping/polling it |
| 1 | NOTIFIED | A wake arrived. Must be (re)scheduled after the current poll |
| 2 | COMPLETE | Output stored, coroutine released |
| 3 | CANCEL_REQ | Cancellation requested. For a PyTask, a hint from an enclosing scope (the truth is the scope tree). A RustTask that sees it at poll start drops its future instead of polling it and completes as cancelled (`JoinHandle::abort`, shutdown) |
| 4 | JOIN_INTEREST | Someone holds a join handle |
| 5 | SCHEDULED | Currently in a run queue |
| 6 | JOIN_WAKER | The join handle's waker is stored in the task (hands the waker slot between handle and lane, as Tokio's bit of the same name) |
| 7.. | REF count | |

Transitions (CAS loops) follow Tokio's `harness.rs` logic:
- `wake`: if RUNNING, set NOTIFIED. Else if neither SCHEDULED nor COMPLETE, set SCHEDULED and push to a queue.
- `poll start`: SCHEDULED→RUNNING (clear NOTIFIED).
- `poll end Pending`: if NOTIFIED, reschedule (to the LIFO slot if the current lane is woken by itself, otherwise the local queue). Else clear RUNNING.
- `poll end Ready`: set COMPLETE, clear RUNNING, wake joiners, notify scope. The future is dropped (on the lane, while still RUNNING) and the output stored before COMPLETE is set. If JOIN_INTEREST is clear at that point, the lane drops the output; a panic nobody joins is resumed on the lane. The task notifies its scope itself (decrement `live_children`, wake the `exit_waiter` at zero, record a failure and cancel the scope for C4), after the joiner is woken and before the scheduler hears of it (`Schedule::task_completed`) and the run reference is released. A task freed without completing notifies its scope from `dealloc`, after its future is dropped; it does not call `task_completed`.
- `abort` (`JoinHandle::abort`): set CANCEL_REQ, then act as `wake`, so the task is polled once more and cancels. A poll in progress finishes first; if it returns `Ready`, the output wins.

`loom` model tests MUST cover wake-during-poll, wake-after-complete, concurrent cancel+wake, and drop-of-last-waker.

### 3.1 What a wake publishes

Every transition that changes the state word is an `AcqRel` read-modify-write, so it synchronizes-with every later transition. A wake that changes nothing writes nothing (as in Tokio): `wake_by_ref` on a task that is SCHEDULED, COMPLETE, or RUNNING with NOTIFIED already set is a plain `Acquire` load. A by-value `wake` always writes, because it drops its reference.

The guarantee is therefore: every wake is followed by a poll that starts after the wake's read of the state word. It is **not** a release fence: writes the waker made before a wake that wrote nothing may be invisible to that poll (the store-buffer pattern). A future MUST therefore observe its event through synchronization the waker also uses: a lock, or a read-modify-write on the resource's own atomic (as an `AtomicWaker`-style registration does). Porto's primitives do (the wait queue publishes under its lock, timers fire on the lane). A plain `Release` store of a flag followed by `wake_by_ref` is not enough. A loom model pins this boundary, so making wakes release operations later would be a visible, deliberate change.

### 3.2 Join handle, cancellation and shutdown

`spawn` returns a `JoinHandle<T>` (Rust API, §21). It owns one reference and JOIN_INTEREST. Awaiting it yields `Result<T, JoinError>`, where `JoinError` is either a panic (with its payload) or a cancellation. Dropping it detaches the task. Waker slot ownership follows Tokio: while JOIN_WAKER is clear only the handle touches the slot; while it is set the handle may only read it, and the lane may read it once COMPLETE; after completion only the lane clears JOIN_WAKER, and whoever clears the last of JOIN_INTEREST and JOIN_WAKER drops the waker. The output belongs to the handle if JOIN_INTEREST was set at completion, to the lane otherwise.

Dropping a run-queue entry instead of running it cancels the task: the drop polls it with CANCEL_REQ set, which drops the future, completes the task as cancelled and wakes the joiner. This defines a wake after runtime shutdown: the closed scheduler drops the entry the wake hands it, so the task is cancelled on the waking thread, its joiner gets a cancelled `JoinError`, and the allocation goes with its last reference. A task that is never woken again keeps its future until its last waker is dropped.

## 4. The step algorithm (PyTask::poll)

```
fn poll(task, lane, cx) -> Poll<()>:
  assert lane.attached()                         // Python lane, thread state attached
  enter task.context  (PyContext_Enter)
  set lane.current_task = task                   // for porto.current_task(), traps
  task.budget = config.coop_budget
  loop:
    // 1. Resolve pending wait, if any
    if task.pending != None:
       if task.budget == 0: wake(self); break Pending   // unpolled: a grant stays in its node
       match task.pending.poll(cx, task):        // §5
         Ready(r)  -> task.resume = r; task.pending = None   // a wait that is over wins
         Pending   ->
           // 2. Cancellation of a waiting task (level-triggered, §5.2)
           if task.pending is cancellable and scopes::effective_cancel(task):
                abandon(task.pending)            // T3
                task.resume = Throw(Cancelled)
           else: register the wait with the task's innermost scope; break with Poll::Pending
    // 3. Step Python
    stats.step_start = now()
    r = match task.resume.take():
          Value(v) -> PyIter_Send(coro, v, &out)
          None     -> PyIter_Send(coro, Py_None, &out)
          Throw(e) -> call coro.throw(e) → out / exception
    stats.record_step()
    match r:
      PYGEN_RETURN(v)  -> task.complete(Ok(v)); break Ready
      PYGEN_ERROR      -> exc = PyErr_GetRaisedException();
                          if exc is Cancelled and it belongs to a scope that is cancelling → task.complete(Cancelled)
                          else task.complete(Err(exc)); break Ready
      PYGEN_NEXT(obj)  ->
          if Py_TYPE(obj) == &TrapType:   task.pending = Trap::take(obj)      // §5
          elif has_attr(obj, "_asyncio_future_blocking") and it is True:
                                        task.pending = Pending::AsyncioFuture(obj)  // §22
          elif obj is None:               task.pending = Pending::Yield        // bare `yield` from types.coroutine
          else: task.resume = Throw(TypeError("porto: task awaited an unsupported object: {type}")); continue
          // a checkpoint while effectively cancelled: not waited on (YieldThen, ScopeExit excepted)
          if task.pending is not YieldThen/ScopeExit and scopes::effective_cancel(task):
               drop(task.pending); task.resume = Throw(Cancelled); continue
          // fast loop: if the pending wait is immediately ready, continue without yielding
          if task.budget == 0: task.pending = Pending::YieldThen(pending); break Pending (reschedule)
          continue
  exit context (PyContext_Exit); lane.current_task = None
```

Notes:
- `PyIter_Send` (CPython ≥ 3.10) MUST be used for send. For throw, call the coroutine's `throw` method through a cached `PyObject*` for the attribute name, using `PyObject_CallMethodOneArg`.
- The step loop **continues** when a trap resolves synchronously (e.g., a `Pending::Sleep(0)` with budget left is still a yield; `Pending::Notified` that is already notified is not). This keeps the number of scheduler round trips minimal.
- `PyErr_GetRaisedException` exists only in ≥ 3.12. For 3.11, use `PyErr_Fetch` + `PyErr_NormalizeException`. Abstract this behind `py_compat::take_exception()`.

## 5. The trap protocol

### 5.1 Trap object

```rust
#[pyclass(frozen, freelist = 512, module = "porto._core", name = "_Trap")]
struct Trap { inner: UnsafeCell<Option<TrapKind>> }   // taken exactly once by the driver
```
Its `__await__` returns `self`, so a Trap is directly awaitable. `__next__` yields `self` on the first call. When the driver resumes the coroutine, the value arrives through `send()` on the *delegating* coroutine chain, which calls `Trap.send(v)`. That raises `StopIteration(v)`, so the `await` expression evaluates to `v`. `throw(e)` re-raises `e`. A Trap reused after completion raises `RuntimeError("trap reused")`.

Implement `Trap` with `tp_iternext`, `am_send` (CPython ≥ 3.10 `PyAsyncMethods.am_send`), and a `throw` method. The `am_send` slot is what makes `PyIter_Send` on the *outer* coroutine route the value without creating a `StopIteration` object on the hot path. This is critical for performance.

### 5.2 TrapKind

```rust
pub enum TrapKind {
    Yield,                                    // schedule point (checkpoint)
    Sleep { deadline: Deadline },             // runtime-clock ns (§14); wheel entry embedded in PyTask (no alloc)
    Notified(WaitNodeRef),                    // intrusive waiter node for sync primitives (§16)
    Io { reg: RegistrationRef, interest: Interest, op: IoOp },  // §13 retry-on-ready
    Join { target: TaskRef },                 // await another task
    ScopeExit { scope: ScopeRef },            // wait for children of a scope (§15)
    Future(Pin<Box<dyn Future<Output = Resume> + Send>>), // generic: anything Rust
    AbiFuture(FfiFuture<AbiResume>),          // from third-party via porto-abi (§21)
}
```

The `Pending` variants mirror `TrapKind` plus `AsyncioFuture(PyObj)` and `YieldThen`. `YieldThen(value)` completes the set in the implementation: a `Yield` that resumes with `value`, which is how T2 stashes a synchronous result when the budget runs out (`_Trap.kind` shows `"yield"`). It holds the value itself, not a boxed `Pending`.

`Io` is packed as one pointer to the operation, which owns its registration reference, interest and op state (13-io-driver.md §5). The operation is a future: polling it attempts the op, and dropping it unregisters its waker.

A woken task whose wait is not over yet is where cancellation is delivered (§4 step 2 for a task that cannot step): the wait is abandoned under T3 and the exception thrown in. A wait that is already over wins: its result is delivered, and the cancellation waits for the next wait. A task cancelled when it yields a trap gets `Cancelled` thrown back at once instead of waiting. The cancellable kinds are all but `Yield`, `YieldThen` and `ScopeExit` (C5: an exit never abandons its children); a `YieldThen` holds a result its coroutine must get (a lock taken by a fast path), so it is never replaced by `Cancelled`. For the same reason a task out of budget yields *before* polling a wait: a grant that resolved it stays in the wait node, where abandoning the wait passes it on.

A cancellable wait that does not resolve at once is registered with the task's innermost scope (`ScopeState::waiters`, the task's waker by task key) until it ends, which is how `Scope.cancel()` wakes it (15 C10). The registration re-walks the effective cancellation after filing the waker, so a cancel from another thread racing with it is never missed. The scope's hold on the waker is the same cycle as a `Notified` wait's, broken at shutdown the same way (the lane keeps the waker).

### 5.3 Rules
- **T1** Every Porto awaitable MUST either complete synchronously or yield exactly one Trap.
- **T2** Synchronous completion MUST call `checkpoint_if_needed()`. It raises `Cancelled` if the effective scope is cancelled (before it spends anything, so an awaitable that took something on its fast path gives it back). Otherwise it spends a unit of the budget. When the budget is already spent, it converts the completion into `Trap::Yield` with the result stashed (`Pending::YieldThen(Ready(v))`). Every step spends a unit too (a throw included), so the step that resumes a `YieldThen` spends the first unit of the next poll, and a loop of synchronous completions yields every `coop_budget` awaits.
- **T3** Traps are cancel-safe: dropping a pending trap (on cancellation) MUST unregister wakers and must not lose data. For I/O ops, the op is re-attempted only after readiness, so data is never consumed by a cancelled op.
- **T4** A trap yielded by a coroutine **not** running inside a Porto task (e.g., awaited inside an asyncio Task on the facade) is handled by the facade (§22): the Trap implements `_asyncio_future_blocking` via a slow path.

## 6. Allocation strategy
- **Trap:** freelist (pyclass `freelist`). Most hot primitives return *ready awaitables* that never allocate a Trap.
- **Ready awaitable:** a single freelisted `_Ready` type holding the result. `__await__` returns self, and `__next__` raises `StopIteration(result)`. Use `am_send` to return `PYGEN_RETURN` directly with no StopIteration allocation.
- **Timer entries** for `Sleep` are embedded in the PyTask body (one active sleep per task), so there is no allocation.
- **Waiter nodes** for sync primitives are embedded in the PyTask body (`wait_node`). A task waits on at most one thing at a time.

## 7. porto.Task (Python handle)

The Python-visible `Task[T]` is created lazily: `scope.spawn` returns it, but internal spawns (Douro request tasks) never create it. It holds a `TaskRef`. Its methods are listed in §20. `await task` produces `TrapKind::Join` (or the ready fast path if already complete).

## 8. contextvars
- `scope.spawn(coro)` copies the *current* context (`PyContext_CopyCurrent`) by default, matching asyncio semantics. `context=` overrides it.
- Each step runs inside `PyContext_Enter/Exit` for the task's context. Contexts are not thread-bound, so migration is safe.

## 9. Async generators
On first run, the runtime calls `sys.set_asyncgen_hooks(firstiter, finalizer)` per lane thread. `firstiter` registers the agen in the current lane's weak set. `finalizer` spawns `agen.aclose()` into the **root scope** with shield and a 5 s deadline. At shutdown, all tracked agens are closed before lanes stop. (Mirrors asyncio's approach.)

## 10. Coroutine origin & debugging
In `debug=True` mode:
- `sys.set_coroutine_origin_tracking_depth(8)` is enabled.
- The spawn site is captured (`traceback.extract_stack(limit=10)` → stored as a compact string).

Always, not only in debug mode: a Porto awaitable that a user call returned (`sleep`, `checkpoint`, `Event.wait`, `Notify.notified`, `Lock.acquire`, the channel operations) and that is freed without ever being resumed, thrown into or closed emits `RuntimeWarning` on dealloc, naming the call and saying to add `await` (the counterpart of CPython's "coroutine ... was never awaited"). The awaitable already records whether it was resumed, so the awaited path pays nothing; a warning turned into an error goes to `sys.unraisablehook`, and an exception already set is kept. Traps the runtime makes for itself never warn.

## 11. Performance targets for this layer (M1 gate)

The gates tighten by milestone (ADR-034): the M0 spike must reach ≥ 2× asyncio+uvloop and these absolute targets within 30%; M1 must meet them; 1.0 must meet the `53-benchmarks.md` table.

| Micro-benchmark | Target (CPython 3.14, per lane, on the reference machine of `53` §1) |
|---|---|
| `await porto.checkpoint()` (budget not exhausted) | ≤ 60 ns |
| Task switch via `Yield` round-trip through the local queue | ≤ 250 ns |
| spawn + run-to-completion of `async def f(): return 1` | ≤ 900 ns |
| oneshot channel send/recv between two tasks | ≤ 600 ns |
| For comparison, asyncio+uvloop `create_task` + await, same machine | measure, must be ≥ 2.5× slower than Porto |
