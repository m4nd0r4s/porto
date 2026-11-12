# 23 — Observability

## 1. Task registry
Every task (Rust and Python) is linked into a per-lane intrusive list on spawn and unlinked on completion. The lane owns the list, so the hot path needs no locks and no atomic read-modify-write: a spawn on the lane links the task at the head, and the completion of a task on the lane unlinks it, with plain loads and stores (the node is two pointers in the header, `RegistryNode`, inside the 64 B budget). Everything another thread does about a task goes around the list:

- **A spawn from another thread** does not link: the task goes on the injector, and the lane that takes it from there links it. A snapshot counts what the injector still holds, so it is exact all the same.
- **A task freed off its lane while still linked** (its last reference was a waker dropped on another thread) is pushed on a lock-free stack of freed tasks, and the lane unlinks and frees it at its next maintenance tick (the push wakes a parked lane). The body's destructor, and so its Python references, runs on the lane (25 P1).
- **Shutdown.** When the runtime is dropped the lane's ownership ends: the stack is drained, and from then on every list operation, from any thread, takes the registry's lock (the shutdown path is the only one that walks). The registry then **cancels every task that is still linked**, with the injector closed so the cancellation completes each one where it is, and repeats until no task is left. So no task outlives its runtime however its references are held: a task kept alive only by wakers (a task waiting on something that holds its own waker, a waker cycle) is cancelled and freed with the runtime instead of waiting for a wake that never comes (11 §3.2).

A snapshot is a request posted to every lane's deferred-work inbox (the same inbox `Handle::defer` uses): the lane copies its list at its next maintenance tick (≤ `event_interval` tasks), or immediately if it is idle (the request wakes it), and the requester waits for each lane's copy. On a lane's own thread the lane's copy is taken at once. Deterministic and cheap; a snapshot is exactly the set of tasks that were live (linked, not complete) when the lane copied, plus the injector's. The raw record is `TaskRecord { id, kind, state, type_name, flags, owner_lane, scope_id, cancel_requested }`; `porto.inspect` (M5) shapes it into `TaskInfo`.

The **introspection switch** (`Builder::introspection`, off by default; the Python runtime turns it on in debug mode and with a `console`) turns on the per-task timing fields of 11 §2 (`first_poll`, `total_poll_ns`, `last_step_start`): they need a clock read per step, so with the switch off the hot path reads no clock. The registry and its snapshots work either way.

The lane's own code can also walk its list (`Handle::with_live_tasks`): `Scope.tasks` is derived that way (15, 20), so a scope keeps no list of its own tasks.

```python
@dataclass(frozen=True)
class TaskInfo:
    id: int; name: str; kind: Literal["python","rust"]; lane: int | None
    state: Literal["scheduled","running","waiting","complete"]
    waiting_on: str | None        # "io:read fd=12", "sleep 1.2s", "lock#7", "join task#33", "scope-exit #9", "asyncio-future"
    scope_id: int; parent_scope_id: int | None
    spawned_at: str | None        # debug mode
    created: float; total_busy: float; steps: int; last_step: float | None
    python_stack: list[str] | None  # only for waiting Python tasks: cr_frame walk (cheap, no execution)
@dataclass(frozen=True)
class ScopeInfo:
    id: int; name: str; cancel_called: bool; deadline: float | None; shield: bool
    tasks: list[TaskInfo]; children: list["ScopeInfo"]
```
Python stacks for *suspended* coroutines are produced by walking `cr_await` chains and `cr_frame` (`f_lineno`, `co_qualname`). The coroutine is not executed, so this is safe from any attached thread under a critical section on the coroutine.

## 2. Blocking-step detector (watchdog)
- Each lane publishes `step_start_ns` (atomic) when it starts stepping Python, and 0 when the step ends.
- A watchdog thread wakes every `slow_step_threshold/4`. If `now - step_start_ns > threshold` for a lane, it:
  1. attaches briefly;
  2. captures that thread's current frame via `sys._current_frames()[lane_thread_id]` and formats ≤ 30 frames;
  3. logs a `porto.slow_step` WARNING once per (task, step), with task name, lane, duration so far, and stack;
  4. increments the metric `porto_slow_steps_total`.
- On step end, if the step was reported, it logs a final duration at INFO.

## 3. Metrics
Counters and gauges are kept per lane (no atomics contention) and aggregated on read:

`tasks_spawned_total{kind}`, `tasks_alive{kind}`, `polls_total`, `steals_total`, `steal_attempts_total`, `park_total`, `park_seconds_total`, `injector_depth`, `local_queue_depth`, `lifo_hits_total`, `budget_exhausted_total`, `io_registrations`, `io_events_total`, `timers_active`, `timers_fired_total`, `blocking_threads`, `blocking_queue_depth`, `slow_steps_total`, `asyncio_bridged_awaits_total`, `gil_degraded` (0/1).

`porto.inspect.stats()` returns them. `porto.metrics.prometheus_text()` renders the Prometheus exposition format. An OpenTelemetry metrics exporter is available behind the `porto[otel]` extra.

## 4. Tracing
- Rust: `tracing` spans for runtime internals (feature `tracing`, off by default in release wheels).
- Python: `porto.tracing` integrates with OpenTelemetry if installed. Each spawned task optionally creates a span (`trace_tasks=True`) linked to the parent's span via contextvars, which works because contexts are copied on spawn.

## 5. Console (`porto-console`)
- With `console="host:port"`, lane 0 runs a tiny Rust TCP server (Rust task, no Python) that serves length-prefixed JSON snapshots (`tasks`, `tree`, `stats`, `slow_steps`) and accepts commands: `cancel <task_id>` (debug only), `dump`.
- `porto-console host:port`: a ratatui TUI with views for task list (sortable by busy time and state), scope tree, lanes (queue depth, park %, steals/s), and a slow-step log.
- Shipped as a separate wheel `porto-console` and a standalone binary.

## 6. Debug mode extras (`debug=True`)
Spawn-site capture, coroutine origin tracking, never-awaited trap warnings, swallowed-cancellation warnings (C11), `threading.local` migration warnings (ST-5), assertions on P1 (Py objects on wrong threads), and slow callback warnings for asyncio facade callbacks (> `slow_step_threshold`).
