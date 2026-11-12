# 19 — Threads & Blocking Work

## 1. Blocking pool
- Elastic: grows on demand up to `blocking_threads` (default 512), idle threads exit after `blocking_keepalive` (10 s; 0 makes a thread leave as soon as it is idle). No thread exists until the first job. A job that finds an idle thread wakes it; a job that finds none starts a thread if the pool is below its limit, and otherwise waits in the queue (FIFO). The queue and the thread accounting are one state under one lock, so a job is never stranded behind a thread that is leaving (loom-tested). `blocking_threads` and `blocking_keepalive` are `RuntimeConfig` fields (`PORTO_BLOCKING_THREADS`, `PORTO_BLOCKING_KEEPALIVE`); the pool's gauges are `blocking_threads` and `blocking_queue_depth` (§23).
- Jobs are `FnOnce() + Send` in Rust, or Python callables.
- A Python job **attaches** the worker thread (`PyGILState_Ensure`-equivalent via a per-thread `PyThreadState` kept for the thread's lifetime) and detaches after the job. The thread creates its state when it starts (`PyThreadState_New`) and deletes it when it exits; a job attaches it through `PyGILState_Ensure`, which finds it instead of making one, so thread-locals set by one job are there for the next job of that thread. An idle worker is detached: it holds no GIL and does not delay a stop-the-world pause on a free-threaded build.
- References: a worker owns Python references only while it runs a Python job (25 P1). The callable, its arguments and the context reach the worker as `PyDeferred`, and the result or the exception (with its traceback) goes back as one: whatever happens to the awaiting task, the lane releases them, and a job dropped unrun is released on the lane, not by the thread that dropped it.
- Thread exhaustion: if the OS refuses a thread, the job goes back to its submitter as an error (`BlockingError::NoThread`, `RuntimeClosed` in Python) when no worker is alive; with one alive it waits in the queue for it. When the last start fails with jobs queued behind it, those jobs are cancelled rather than left waiting for a thread that never comes. A panic in a worker's start hook costs that thread its per-thread state, not its place in the count (a Python job then attaches through an ephemeral state).
- Results reach the awaiting task through the job's join handle, that is the ordinary wake path (an injector push and a notify of the lane); there is no second channel.
- **Shutdown** (§26 step 6): queued jobs that no worker has taken are cancelled (awaiting them yields a cancelled error, `RuntimeClosed` in Python); the pool then waits up to 1 s for the workers that run a job (the lane thread waits *detached*, through its park hook: a Python job takes the interpreter back as it returns, and a worker deletes its own thread state before it counts as gone, so a lane that kept the interpreter attached would run the wait out for nothing), with the runtime's injector and deferred queue still open so that a finishing job's wake and releases are run on the lane; the workers still busy after that are detached and leave when their job ends (a result they deliver has no lane left and is released where it is: attached, by the worker, or leaked once the interpreter finalizes). A worker deletes its `PyThreadState` before it is counted as gone, so a shutdown that reports every worker left has none still tearing one down; a worker detached after the 1 s leaves its state alone (leaked: finalization deletes it) instead of racing `Py_Finalize`. A job cannot be interrupted at any point: aborting a job that has not started only cancels it.

## 2. Python API
```python
result = await porto.to_thread(fn, *args, cancellable: bool = False,
                               limiter: porto.CapacityLimiter | None = None, **kwargs)
```
- Context: runs `fn` inside a **copy** of the caller's contextvars context (a copy made at the call: `fn` sees the values set so far and what it sets does not reach the caller).
- Result: the callable's return value, or its exception raised as it is (the traceback includes the worker's frames, `BaseException`s such as `KeyboardInterrupt` included: they reach the awaiter, they do not stop the run). `TypeError` (that says to pass a callable) for a `fn` that is not callable; `NoRuntimeError` outside `porto.run`. The job is queued when the `await` starts: a call that is never awaited runs nothing.
- Cancellation: the default `cancellable=False` shields the await until the thread finishes (the thread can't be stopped): a cancel or a deadline that arrives meanwhile takes effect at the first checkpoint after the result is back (as for any shielded section: the result is returned first), which is also how Trio's `to_thread.run_sync` and AnyIO's (`abandon_on_cancel=False`) behave. With `cancellable=True`, the await raises `Cancelled` immediately and the result is discarded when the thread finishes (AnyIO's "abandon on cancel"); the job still runs to its end, and so does a job that was queued and not started. Neither mode can interrupt the callable.
- Default limiter: `porto.to_thread.default_limiter()` with 40 tokens, matching AnyIO's default. *Not implemented yet (porto-szb.26): a `limiter` other than `None` raises `NotImplementedError`, and without one the only bound is `blocking_threads`.*

## 3. Calling into the runtime from foreign threads
```python
porto.from_thread.run(async_fn, *args)          # from a to_thread worker: blocks until done, returns result
porto.from_thread.run_sync(fn, *args)           # runs fn on a Python lane, blocks
portal = porto.Portal()        # created inside the runtime; usable from any thread:
fut = portal.submit(coro_fn, *args)   # -> concurrent.futures.Future
```
- On a lane thread, `from_thread.*` raises `RuntimeError` (ST-17).
- The portal's work runs in a portal-owned scope, a child of the scope where the portal was created. Closing the portal cancels outstanding work.

## 4. Rust-side
`porto::task::spawn_blocking(f) -> JoinHandle<R>` (`Handle::spawn_blocking` from any thread; `Handle::try_spawn_blocking` returns the error instead of panicking when the OS refuses the first thread) and `porto::task::block_in_place` (not provided; it is deliberately omitted because it breaks lane attachment invariants). Dropping the `JoinHandle` detaches the job (it still runs, its result is dropped by the worker); `JoinHandle::abort` cancels one that has not started; a panic in the closure is the handle's `JoinError`. A job runs on a thread that is not a lane, so the `blocking_*` methods of the sync primitives are allowed in it (ST-17). `RuntimeBuilder::blocking_threads`, `blocking_keepalive` and `blocking_hooks` configure the pool; a `WorkerHooks` value runs on each new worker thread and the value it returns lives until the thread exits (the Python bindings create the `PyThreadState` there).

## 5. Running sync code in lanes ("inline")
Nothing prevents users from running sync code in a lane. The watchdog (§23) reports steps longer than `slow_step_threshold`.
