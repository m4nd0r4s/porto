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
- **C4 Child failure:** an exception in a child task (other than `Cancelled` from this scope) is recorded and **cancels the scope**, which cancels siblings and the body. At exit, the scope raises `ExceptionGroup("porto scope '<name>' failed", errors)` (`BaseExceptionGroup` if any is a BaseException). An exception leaving the body cancels the scope the same way (other than a `Cancelled` from a cancellation already reaching it), so the exit does not wait for children that would never end on their own. The body's own exception is included first.
- **C5 Exit waits:** `__aexit__` waits (`TrapKind::ScopeExit`) until `live_children == 0`. While waiting it is itself a checkpoint, but cancellation only causes child cancellation; the exit never abandons children.
- **C6 Spawn into a cancelled scope:** allowed. The child starts already cancelled and raises at its first checkpoint, as in Trio.
- **C7 Spawn into a closed scope:** after `__aexit__` completes, `spawn` raises `RuntimeError("scope is closed")`.
- **C8 Shield:** `porto.shield()` ≡ `porto.scope(shield=True)`. Common pattern:
  ```python
  finally:
      with porto.shield(timeout=5):     # sync `with` allowed for shield-only scopes with no spawning
          await conn.rollback()
  ```
  A scope used with sync `with` cannot `spawn`; calling it raises `RuntimeError`. Sync `with` works for any scope used only as a cancel scope (shield, deadline, `cancel()`): entering and leaving it do not await. Entering a shielded scope is not a cancellation checkpoint of the scopes around it, with `async with` too: the shield is entered first, so `finally: async with porto.scope(shield=True, timeout=5):` in a cancelled task runs its cleanup instead of raising at the entry (the entry still spends coop budget, T2). Its exit is a checkpoint of the enclosing scope, as for any scope. `scope.shield` is read/write, from any thread: the tasks in the scope are re-evaluated at once, and unshielding wakes the waiting ones an outer cancel now reaches (C10). The root scope cannot be shielded: setting it raises `RuntimeError`.
- **C9 Deadlines nest:** the effective deadline is the min over unshielded ancestors. Each scope arms only its own deadline: an ancestor's deadline cancels the ancestor, which reaches the task through the walk (§1.2) unless a shield is in between. Setting `scope.deadline` later re-arms the timer, from any thread; a time already passed cancels the scope at once, and `inf` removes the deadline. A scope already cancelled stays cancelled whatever deadline it is given (the first reason wins). `timeout=` counts from the block's entry (with `deadline=` too, the earlier wins); `0` or less expires at once, NaN raises `ValueError`. The root scope has no deadline: setting one raises `RuntimeError`. `scope.timed_out` is true when the deadline cut the block short: the exit absorbed the `Cancelled` the scope's own deadline caused (C3), or the block finished with no exception after children were ended by it. A `raise_on_timeout=True` scope then raises `TimeoutError`, unless children failed too: their `ExceptionGroup` is raised instead (`timed_out` stays true). Any other exception leaving the block (a cleanup bug after the `Cancelled`, `KeyboardInterrupt`) is the body's own: it goes on as C4 says, `timed_out` stays false, also when children were cut short; if children failed too, their `ExceptionGroup` is raised instead.
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
- `start` spawns the task with `started=Started()` injected as a keyword argument (it takes no other keyword but `name`; `functools.partial` binds the rest). It awaits until `started.set(v)` and returns `v`.
- Until `set`, the task belongs to a hidden scope owned by the caller. Exceptions before `set` propagate to the caller of `start`, as they are, not in a group. After `set`, the task is re-parented into `s`. This matches Trio's `nursery.start` semantics.

Mechanics, so that the rules above hold with the scope tree of §1:
- The hidden scope is an ordinary scope nested in the scope the caller is in, and the caller waits at its exit (C5), which ends when the task has called `set` or has ended. So cancelling the caller (its scope, a deadline) reaches the task through the walk and cancels it, and the caller's wait ends with its own cancellation (`start` raises `Cancelled`, not a complaint that the task never reported). A failure of the task before `set` is C4's: it cancels the hidden scope and is raised from `start` bare (a hidden scope with one failure raises it as is).
- `set(v)` moves the task, which must be the one `start` started (otherwise it raises `RuntimeError`), in two steps: the task counts in `s` from then on (its `live_children`, its failures, its exit wait) and is counted out of the hidden scope, which ends the caller's wait; and the hidden scope is *adopted* by `s` (`ScopeNode::adopt`): the walk, the cancel propagation and the waiter wake-ups (C10) of everything below it (the task, the scopes it entered before `set` and their tasks) see `s` as its parent from then on, and the caller's scopes no longer. If `s` is cancelled by then the task is cancelled at its next checkpoint, as for any spawn into a cancelled scope (C6). If `s` closed meanwhile (C7), `set` raises `RuntimeError` and the task stays in the hidden scope.
- A task that ends without calling `set` makes `start` raise `RuntimeError` (unless the caller was cancelled: then `Cancelled`).

## 4. Task cancellation
`task.cancel()` cancels an implicit per-task scope that wraps the task's coroutine. It is equivalent to the task having been spawned inside `porto.scope()` and that scope being cancelled. It does not cancel siblings, and the absorbed `Cancelled` does not count as a failure. It is level-triggered like any cancel (C1): a task that catches the `Cancelled` gets it again at its next checkpoint. It cancels everything the task runs in scopes it entered (they are nested in the implicit scope), and a shield the task entered holds it off until it is left (C8). Cancelling a task that finished, or cancelling again, does nothing. Safe from any thread (C10).

The implicit scope costs nothing at spawn. It is a node of the scope tree made when the task first enters a scope, as a child of the scope it was spawned into and the parent of the scopes it enters (cancelled at once if `cancel()` was called before). Until then, and in a task that never enters one, `cancel()` sets a flag the task reads at its checkpoints, and wakes it. A task spawned by no scope (an internal spawn) has no handle to cancel it with; one made by `current_task()` inside scopes the task had entered already (its implicit scope would have to sit above them) cancels the task itself at its checkpoints but does not reach the tasks of those scopes: call `current_task()` before opening scopes.

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
