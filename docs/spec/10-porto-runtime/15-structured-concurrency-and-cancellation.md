# 15 — Structured Concurrency & Cancellation

## 1. The Scope

`porto.scope(...)` returns a `Scope`. It is both:
- a **cancel scope**: it has a deadline, can be cancelled, and can be shielded;
- a **nursery**: tasks spawned into it must finish before the `async with` block exits.

```python
async with porto.scope(timeout=None, deadline=None, shield=False,
                       name=None, raise_on_timeout=True) as s:
    t = s.spawn(work())          # Task[T]
    value = await s.start(server, port=0)   # waits for started.set(value)
```

### 1.1 Scope tree (Rust)
```rust
struct ScopeNode {
    id: u64,
    parent: Option<Arc<ScopeNode>>,
    owner_task: TaskId,            // the task whose `async with` created it
    state: Mutex<ScopeState>,      // children list, deadline, counters
    status: AtomicU8,              // cancel_called and shield, written under `state`, read lock-free
    cancel_epoch: AtomicU64,       // bumped on cancel; tasks compare to detect new cancels quickly
    walk_cache: AtomicU64,         // (cancel_generation << 1 | cancelled) of the last walk from here
    deadline_timer: TimerEntry,
}
struct ScopeState {
    // cancel_called: Option<CancelReason> (Explicit | Deadline | Parent | ChildFailed | Shutdown)
    // and shield: bool are logically here, but live packed in ScopeNode::status so the walk
    // (§1.2) takes no lock. The first cancel reason wins.
    deadline: Option<Instant>,
    live_children: u32,                   // tasks spawned into this scope, not complete
    child_scopes: IntrusiveList<ScopeNode>, // nested scopes (for propagation)
    errors: Vec<PyObj>,                   // failures collected (ExceptionGroup at exit)
    exit_waiter: Option<Waker>,           // owner waiting in ScopeExit
    waiters: HashMap<TaskKey, Waker>,     // tasks in a cancellable wait, innermost scope here (C10)
}
```

### 1.2 Effective cancellation of a task
A task is **effectively cancelled** if walking from its innermost entered scope outward reaches a scope with `cancel_called` set before it reaches a scope with `shield=True`, counting the shielded scope itself: a shielded scope's own cancellation still counts.

Tasks spawned into scope S start with S as their base scope. The walk continues through S's ancestors.

Optimization: each task caches `(scope_id, cancel_epoch_snapshot)` and the walk's result (`CancelCache`, for tasks that have room for it). The walk's result depends only on the innermost scope, so a PyTask, whose body has no room for a cache, uses the scope's own: `ScopeNode::walk_cache` holds `(cancel_generation, result)` for every task in that scope, refilled by the first check after the generation moved. The cache compares scope ids, not pointers, because a freed scope's address can be reused by a new one. The global `cancel_generation: AtomicU64` is bumped on any cancel or shield change. A cancel bumps the `cancel_epoch` of the scope and of every descendant its cancellation reaches, stopping at shielded scopes. Changing a shield does the same from that scope. The check costs one atomic load when the generation is unchanged. When it moved, the check compares the scope's epoch with the snapshot, and the walk runs only if the epoch changed too.

## 2. Semantics (normative)

- **C1 Level-triggered:** while effectively cancelled, every checkpoint raises `Cancelled`.
- **C2 Delivery:** cancellation is delivered only at checkpoints, meaning Porto awaits or traps. Pure Python code between awaits is never interrupted.
- **C3 Absorption:** when a `Cancelled` propagates out of the `async with` of the scope that caused it, the scope swallows it. A `Cancelled` counts for the scope whose cancel caused it, which is the first cancelled scope the effective-cancellation walk reaches (§1.2), as in Trio. A child task that ends with a `Cancelled` caused by this scope counts too: the scope absorbs it when the child ends, and the exit drops it. `scope.cancelled_caught` becomes `True` when the exit absorbed such a `Cancelled` from the body or from a child. It is also `True` when only the children were reached: the body finished, or never awaited, before the cancel ("first answer wins": children race, and the winner calls `s.cancel()`). A child ended by an enclosing scope's cancel counts for that enclosing scope, never for the nested one it ran in. A `Cancelled` the body catches and swallows is not absorbed. If the reason was a deadline and `raise_on_timeout=True`, the scope raises `TimeoutError` (builtin) instead.
- **C4 Child failure:** an exception in a child task (other than `Cancelled` from this scope) is recorded and **cancels the scope**, which cancels siblings and the body. At exit, the scope raises `ExceptionGroup("porto scope '<name>' failed", errors)` (`BaseExceptionGroup` if any is a BaseException). The body's own exception is included first.
- **C5 Exit waits:** `__aexit__` waits (`TrapKind::ScopeExit`) until `live_children == 0`. While waiting it is itself a checkpoint, but cancellation only causes child cancellation; the exit never abandons children.
- **C6 Spawn into a cancelled scope:** allowed. The child starts already cancelled and raises at its first checkpoint, as in Trio.
- **C7 Spawn into a closed scope:** after `__aexit__` completes, `spawn` raises `RuntimeError("scope is closed")`.
- **C8 Shield:** `porto.shield()` ≡ `porto.scope(shield=True)`. Common pattern:
  ```python
  finally:
      with porto.shield(timeout=5):     # sync `with` allowed for shield-only scopes with no spawning
          await conn.rollback()
  ```
  A scope used with sync `with` cannot `spawn`; calling it raises `RuntimeError`.
- **C9 Deadlines nest:** the effective deadline is the min over unshielded ancestors. Setting `scope.deadline` later re-arms the timer.
- **C10 Cross-lane:** scopes span lanes. `cancel()` from any lane or thread is safe. It bumps the epoch, then wakes every waiting task in the subtree whose pending trap is cancellable (the `waiters` of each scope the cancel reaches, collected under the locks and woken after them). Wakes are delivered to owning lanes via inbox.
- **C11 Cancelled type:** `porto.Cancelled(asyncio.CancelledError)`. Users MUST NOT swallow it. The debug mode warns when a task finishes normally after having been delivered `Cancelled` inside an effectively-cancelled scope ("cancellation was swallowed").
- **C12 Unhandled errors in the root scope:** if `main` raises, `porto.run` re-raises after shutdown. Background tasks in the root scope that fail cancel `main` (the root scope is a nursery). `main` is the root scope's first task, and the scopes it opens nest in the root scope, so a failure there cancels them too. Once `main` ended, the root scope is cancelled (`Shutdown`) and `porto.run` waits up to `cancel_timeout` for the tasks left in it (§26 §2), then closes it (C7). It returns main's result, or raises main's exception as is, when no other task of the root scope failed; otherwise it raises the strict group `porto root scope failed` (ADR-009) of main's exception, first, and the other failures. Tasks still in the root scope when `cancel_timeout` runs out are abandoned (dropped with the runtime), and `porto.run` emits a `RuntimeWarning` that counts them. An interrupt that ends `porto.run` itself (Ctrl-C or `SystemExit` while it waits) is raised bare, never grouped with the failures.

## 3. `start()` protocol
```python
async def server(port: int, *, started: porto.Started[int]) -> None:
    listener = await porto.net.listen_tcp("0.0.0.0", port)
    started.set(listener.port)
    ...
port = await s.start(server, 0)
```
- `start` spawns the task with `started=Started()` injected as a keyword argument. It awaits until `started.set(v)` and returns `v`.
- Until `set`, the task belongs to a hidden scope owned by the caller. Exceptions before `set` propagate to the caller of `start`. After `set`, the task is re-parented into `s`. This matches Trio's `nursery.start` semantics.

## 4. Task cancellation
`task.cancel()` cancels an implicit per-task scope that wraps the task's coroutine. It is equivalent to the task having been spawned inside `porto.scope()` and that scope being cancelled. It does not cancel siblings, and the absorbed `Cancelled` does not count as a failure.

Reading the outcome of a task that ended cancelled (`task.result()`, or joining it) raises `porto.Cancelled`, as `asyncio.Task.result()` raises `CancelledError`: a fresh exception per read, whose `__cause__` is the `Cancelled` the task ended with. A joiner whose own scope is not cancelled that lets it propagate fails like any other exception (C4); catching it is how to tell "the task was cancelled" apart.

## 5. Interaction with asyncio-style code (summary; details §22)
`asyncio.CancelledError` raised by asyncio code inside a Porto task, when the Porto scope is not cancelled, is treated as a regular exception (a failure). This is deliberate. It surfaces bugs where asyncio libraries cancel things that aren't theirs.

## 6. Exceptions table

| Situation | Exception |
|---|---|
| deadline hit, `raise_on_timeout=True` | `TimeoutError` (builtin) |
| child failed | `ExceptionGroup` / `BaseExceptionGroup` |
| spawn after close | `RuntimeError` |
| await outside a Porto task (no runtime) | `porto.NoRuntimeError(RuntimeError)` |
| `started.set` twice | `RuntimeError` |
