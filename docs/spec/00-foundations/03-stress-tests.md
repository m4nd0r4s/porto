# 03 — Stress Tests (Thought Experiments)

Each experiment states a scenario, follows its consequences, and records the **design consequence** that the rest of the spec implements. When a later file says "see ST-n", it means this file.

---

### ST-1 "Isn't a faster loop enough?"
**Scenario:** we ship a Rust asyncio loop and stop there.
**Analysis:** the public rsloop benchmark shows ~2× on raw callbacks, while tasks are slightly slower than uvloop and TCP streams are at parity. A typical request handler does maybe 5–20 awaits. Each await in asyncio means building a Future, adding a done-callback, scheduling a `call_soon` handle, and then a Task `__step` that does a Python-level method dispatch. The loop's own share of that is small.
**Consequence:** Porto has its own task model (ADR-001). Awaits on Porto primitives cost a C-level `PyIter_Send` plus a pointer check, and no per-await Python object is created on the ready-fast-path (§11).

### ST-2 "3.14t asyncio already scales linearly per thread. Why multi-lane Porto?"
**Analysis:** one-loop-per-thread asyncio has three structural problems. (a) There is no load balancing: a hot connection pins one core. (b) Cancellation and task groups can't span loops. (c) Protocol parsing still runs in Python. Porto lanes share one runtime, stealing balances load, scopes span lanes, and parsing happens in Rust.
**Consequence:** multi-lane mode is a first-class feature (§12). The comparison benchmark "N asyncio loops on N threads" is a mandatory gate (§53).

### ST-3 A handler runs 300 ms of CPU-bound Python
**Analysis:** on a single lane, every other task stalls. Stealing helps on multi-lane only if other lanes are idle. Python cannot be preempted.
**Consequence:**
1. The blocking detector warns with the offending task's Python stack when a step exceeds `slow_step_threshold` (default 100 ms) (§23).
2. `porto.to_thread` is one line away.
3. Douro runs sync `def` handlers on the blocking pool by default (§33).
4. When one lane is blocked, stealing drains its queue: other lanes steal the queued tasks, though not the running one (§12).

### ST-4 Work-stealing ping-pong destroys cache locality
**Scenario:** a producer/consumer pair across lanes keeps bouncing.
**Consequence:** a Tokio-style LIFO slot puts the woken task in the waker's lane next (§12 §4). Python tasks additionally get **soft affinity**: a task runs on its last lane unless that lane's queue is above the steal threshold. Stealing is adaptive, never eager by default.

### ST-5 A library uses `threading.local` (e.g., some DB drivers, `decimal` context, legacy ORMs)
**Analysis:** if a task migrates lanes between two awaits, its thread-local state silently changes.
**Consequence:**
- `scope.spawn(..., pin=True)` pins a task, and pinning is inherited by the subtree.
- `RuntimeConfig.python_steal = "off"` disables migration of Python tasks globally.
- Docs warn about this, and the debug mode detects migration of a task that touched `threading.local` objects. Detection is best-effort: we wrap `threading.local.__getattribute__` only in debug mode (§23).

### ST-6 A C extension without `Py_mod_gil` is imported on 3.14t
**Analysis:** CPython re-enables the GIL process-wide at import time. N lanes would then fight over the GIL, which is worse than one lane.
**Consequence:** each lane checks `sys._is_gil_enabled()` (cached; refreshed via an import hook that the runtime installs, §12 §8). If the GIL is on, the runtime enters **GIL-degraded mode**: Python tasks are funneled to lane 0, and the other lanes run only Rust tasks. It logs one warning naming the module (tracked by the import hook).

### ST-7 Docker refuses io_uring
**Consequence:** epoll is the default. `io_backend="io_uring"` probes `io_uring_setup`. On `EPERM`/`ENOSYS`/`EINVAL`, it falls back to epoll and logs a warning, unless `io_backend_strict=True` (§13).

### ST-8 Two native extensions each statically link `porto-core`
**Analysis:** every Rust cdylib carries its own statics. Douro's `.so` and Porto's `.so` would each contain a separate reactor, so registering an FD in the "wrong" one deadlocks.
**Consequence:** **ADR-004 — single runtime, C ABI.** Only `porto._core` owns runtime state. Everything else, including Douro, uses `porto-abi` through a versioned `PyCapsule` vtable (§21). Douro dogfoods the ABI.

### ST-9 Client disconnects while the handler is inside a DB transaction
**Analysis:** cancelling the request scope raises `Cancelled` at the handler's next checkpoint. If cleanup (`ROLLBACK`) awaits, it would immediately raise `Cancelled` again (level-triggered), and the connection would leak.
**Consequence:**
- `finally:` cleanup that awaits MUST use `with porto.shield():` (or `async with porto.scope(shield=True, timeout=5)`). DB integrations shipped by us do this internally.
- Douro's `on_disconnect` policy is configurable per route: `"cancel"` (default), `"ignore"`, or `"cancel_after(seconds)"` (§33).
- Linting: `douro check` warns about `finally` blocks containing `await` without a shield (AST heuristic).

### ST-10 Strict exception groups annoy users
**Analysis:** with strict groups, `except ValueError` around a scope never fires, because the error arrives wrapped in an `ExceptionGroup`. Trio users complained, but non-strict mode creates worse bugs.
**Consequence:** groups are always strict (ADR-009). Two mitigations ease the pain:
1. `except*` is idiomatic (3.11+).
2. In Douro, the exception handling layer unwraps single-exception groups that originate from the handler's own scope before matching exception handlers (§35).

### ST-11 One million idle tasks (WebSocket fan-out)
**Analysis:** the per-task Rust header must be small, and the coroutine frame dominates (~0.5–2 KB in Python).
**Consequence:**
- The task header is ≤ 64 bytes (one cache line) including the intrusive list node, with a hard ceiling of 128 bytes (ADR-034).
- Timers are O(1) wheel inserts, and waiter nodes are intrusive.
- Benchmark gate: 1M sleeping tasks, RSS < 2.5 GB, spawn rate > 1M/s per lane (§53).

### ST-12 Unbounded queues hide overload
**Consequence:**
- `porto.channel()` requires a capacity, and the unbounded variant is a separately named function.
- Douro has `max_concurrent_requests` per lane. When it is exceeded, the acceptor pauses `accept()`, which pushes backpressure to the kernel backlog.
- Load shedding returns `503` with `Retry-After` when the queue wait exceeds `max_queue_wait` (§38, §40).

### ST-13 Slowloris and huge bodies
**Consequence:** header read timeout (10 s), body idle timeout (30 s), max header bytes (16 KiB), max body (1 MiB, per-route override), streaming body API, HTTP/2 rapid-reset mitigation via hyper settings, and a max concurrent streams cap (§40).

### ST-14 `fork()` after the runtime started
**Analysis:** the child inherits locked mutexes, an epoll FD shared with the parent, and missing threads.
**Consequence:**
- `os.register_at_fork` hooks are installed. Forking with a running runtime raises in the child's `after_in_child` hook, and the runtime becomes unusable in the child, with a clear error.
- The Douro supervisor never starts a runtime before forking, and `--preload` asserts that no runtime threads exist (§38).

### ST-15 Mixing asyncpg (asyncio-native) into Porto handlers
**Analysis:** asyncpg uses `loop.create_connection`, protocols, and futures.
**Consequence:** each lane hosts a lazily-created **asyncio facade loop** implemented on Porto's reactor (§22). Porto's trap driver recognizes asyncio futures (`_asyncio_future_blocking`) and bridges them. The overhead per bridged await is ~1 extra callback. That is accepted, and documented as "native drivers are faster".

### ST-16 AnyIO cannot load third-party backends
**Consequence:** see `02` §5. Porto registers a backend into AnyIO's private cache, guarded by a tested AnyIO version range. If the version is outside that range, Porto falls back to letting AnyIO see `"asyncio"` and run on the facade. In parallel, we propose an upstream entry-point registry.

### ST-17 Deadlock: `to_thread` code calls back into the runtime with `from_thread.run` while the lane waits on a sync lock held by that thread
**Consequence:** `from_thread.run` from a blocking-pool thread is allowed. Calling it from a lane thread raises `RuntimeError` immediately (it would block the lane). Porto's sync primitives (`porto.Lock`) are never blocking-acquirable from lane threads (§16, §19).

### ST-18 Timer precision vs throughput
**Consequence:**
- The wheel has 1 ms resolution by default.
- `sleep(0)` is a pure yield.
- Sub-ms deadlines round up, so they never fire early (§14).
- `timer_resolution` can be set down to 100 µs at a CPU cost.

### ST-19 Tracebacks through Rust
**Consequence:**
- Traps never appear as Python frames: they are C-level iterators.
- Errors raised from Rust carry the Rust error chain as `__notes__`.
- `debug=True` records spawn-site stack summaries and appends them as notes on unhandled task exceptions ("task spawned at …").

### ST-20 Graceful shutdown under load (SIGTERM)
**Consequence:** the sequence is:
1. Stop accepting new work.
2. Mark keep-alive connections "close after current request" and send HTTP/2 GOAWAY.
3. Wait up to `graceful_timeout`.
4. Cancel the root scope.
5. Wait for cancel cleanup up to `cancel_timeout`.
6. Hard exit.

All of this is driven by structured scopes (§26, §38).

### ST-21 Python object dropped on a non-attached Rust I/O thread
**Consequence:** Rule P1 (§25): only lane threads (attached) may own `Py<T>`. Cross-thread messages carry `PyDeferred` wrappers whose `Drop` pushes the pointer onto the owning lane's decref queue. An assertion catches violations in debug builds.

### ST-22 Free-threaded data races in user code
**Analysis:** with N lanes, two handlers can mutate a shared dict concurrently. CPython makes builtins memory-safe, but not logically atomic.
**Consequence:**
- The Porto runtime defaults to **1 Python lane**.
- Douro defaults to N lanes on free-threaded builds, because request handlers are conventionally stateless. It is overridable, and `douro run` prints the mode at startup.
- Docs include a "shared state checklist".
- `porto.Lock` and `porto.Mutex[T]` (which owns a value) are recommended.

### ST-23 Cancellation of a Rust future awaited from Python
**Consequence:** dropping the future is the cancellation. The ABI requires futures to be cancel-safe or to document otherwise. Porto's own I/O futures are cancel-safe: partial reads keep their data in the stream buffer (§17).

### ST-24 Huge JSON (50 MB array) into a typed model
**Consequence:**
- The fused parser+validator streams without building an intermediate tree. Memory stays near O(output).
- Body limits apply.
- `douro.JSONStream[T]` gives incremental iteration over top-level arrays (§34).

### ST-25 Unicode and path normalization attacks
**Consequence:** the router matches on the percent-decoded, normalized path (§32 §6). It rejects encoded slashes in segments by default, and dot-segments are resolved before matching (§40).

### ST-26 Users want to keep FastAPI code
**Consequence:**
- `app.mount("/v1", fastapi_app)` runs ASGI apps on the facade (§41).
- `douro migrate fastapi` is a codemod (M9) that rewrites decorators and `Depends` imports.

### ST-27 Rust panics inside the runtime
**Consequence:**
- A panic inside a task fails that task (`porto.RustPanic`) and does not crash the lane.
- A panic in core scheduler code aborts the process, because state is presumed corrupted. `panic=unwind` is required in the profile for the task-level catch.

### ST-28 Windows
**Consequence:** mio's IOCP/AFD gives readiness semantics, so the same driver code works. The TCP/UDP/process/signal subset (Ctrl-C/Ctrl-Break) is supported. Unix sockets are supported on Windows 10+. CI runs on windows-latest (§51).
