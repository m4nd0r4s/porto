# 11 — Task Model & Trap Protocol (normative core)

This file is the heart of Porto. Implement it first and benchmark it (milestone M1) before anything else.

## 1. Two kinds of tasks, one header

```rust
#[repr(C)]
pub struct Header {
    state: AtomicU32,            // see §3
    vtable: &'static TaskVTable, // poll, drop, dealloc, schedule, id hooks
    owner_lane: AtomicU16,       // last lane that ran it (soft affinity)
    flags: u16,                  // PIN, PYTHON, DETACHED_DROP, ...
    id: u64,                     // monotonically increasing, never reused
    scope: *const ScopeNode,     // spawning scope (strong ref held by body)
    queue_next: UnsafeCell<*const Header>, // intrusive run-queue link (injector)
    registry: RegistryNode,      // intrusive list for introspection (§23)
}
```
- **RustTask<F: Future>**: Header + `F` + output slot + join waker. Polled via `Future::poll`.
- **PyTask**: Header + PyTask body (§2). Its `poll` implementation performs **steps** (§4).

Target: `size_of::<Header>() <= 64`, `size_of::<PyTaskBody>() <= 64` (excluding the Python coroutine).

Reference counting: tasks are `Arc`-like with a manual refcount packed in `state` (as in Tokio: `REF_ONE = 1 << 6`). Wakers clone and drop refs.

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
}
enum Resume { None, Value(PyObj), Throw(PyObj) }
```

## 3. Task state machine

Bits in `state: AtomicU32`:

| Bit | Name | Meaning |
|-----|------|---------|
| 0 | RUNNING | A lane is currently stepping/polling it |
| 1 | NOTIFIED | A wake arrived. Must be (re)scheduled after the current poll |
| 2 | COMPLETE | Output stored, coroutine released |
| 3 | CANCEL_REQ | An enclosing scope requested cancellation (hint; the truth is the scope tree) |
| 4 | JOIN_INTEREST | Someone holds a join handle |
| 5 | SCHEDULED | Currently in a run queue |
| 6.. | REF count | |

Transitions (CAS loops) follow Tokio's `harness.rs` logic:
- `wake`: if RUNNING, set NOTIFIED. Else if neither SCHEDULED nor COMPLETE, set SCHEDULED and push to a queue.
- `poll start`: SCHEDULED→RUNNING (clear NOTIFIED).
- `poll end Pending`: if NOTIFIED, reschedule (to the LIFO slot if the current lane is woken by itself, otherwise the local queue). Else clear RUNNING.
- `poll end Ready`: set COMPLETE, clear RUNNING, wake joiners, notify scope.

`loom` model tests MUST cover wake-during-poll, wake-after-complete, concurrent cancel+wake, and drop-of-last-waker.

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
       match task.pending.poll(cx, task):        // §5
         Pending   -> break with Poll::Pending
         Ready(r)  -> task.resume = r; task.pending = None
    // 2. Cancellation check (level-triggered)
    if let Some(exc) = scopes::effective_cancel(task) && task.resume is not Throw:
         // Only inject if the task is at a checkpoint (i.e., it just resumed from a trap)
         task.resume = Throw(Cancelled(exc))
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
    Sleep { deadline: Instant },              // uses timer wheel entry embedded in PyTask (no alloc)
    Notified(WaitNodeRef),                    // intrusive waiter node for sync primitives (§16)
    Io { reg: RegistrationRef, interest: Interest, op: IoOp },  // §13 retry-on-ready
    Join { target: TaskRef },                 // await another task
    ScopeExit { scope: ScopeRef },            // wait for children of a scope (§15)
    Future(Pin<Box<dyn Future<Output = Resume> + Send>>), // generic: anything Rust
    AbiFuture(FfiFuture<AbiResume>),          // from third-party via porto-abi (§21)
}
```

The `Pending` variants mirror `TrapKind` plus `AsyncioFuture(PyObj)` and `YieldThen(Box<Pending>)`.

### 5.3 Rules
- **T1** Every Porto awaitable MUST either complete synchronously or yield exactly one Trap.
- **T2** Synchronous completion MUST call `checkpoint_if_needed()`. It raises `Cancelled` if the effective scope is cancelled and the cancel was not yet delivered to this checkpoint. It also decrements the budget. When the budget hits 0, it converts the completion into `Trap::Yield` with the result stashed (`Pending::YieldThen(Ready(v))`).
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
- Never-awaited Porto awaitables (a trap created but never awaited) emit `RuntimeWarning` on dealloc.

## 11. Performance targets for this layer (M1 gate)

| Micro-benchmark | Target (CPython 3.14, x86-64, per lane) |
|---|---|
| `await porto.checkpoint()` (budget not exhausted) | ≤ 60 ns |
| Task switch via `Yield` round-trip through the local queue | ≤ 250 ns |
| spawn + run-to-completion of `async def f(): return 1` | ≤ 900 ns |
| oneshot channel send/recv between two tasks | ≤ 600 ns |
| For comparison, asyncio+uvloop `create_task` + await, same machine | measure, must be ≥ 2.5× slower than Porto |
