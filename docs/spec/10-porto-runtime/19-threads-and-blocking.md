# 19 — Threads & Blocking Work

## 1. Blocking pool
- Elastic: grows on demand up to `blocking_threads` (default 512), idle threads exit after `blocking_keepalive` (10 s).
- Jobs are `FnOnce() + Send` in Rust, or Python callables.
- A Python job **attaches** the worker thread (`PyGILState_Ensure`-equivalent via a per-thread `PyThreadState` kept for the thread's lifetime) and detaches after the job.

## 2. Python API
```python
result = await porto.to_thread(fn, *args, cancellable: bool = False,
                               limiter: porto.CapacityLimiter | None = None, **kwargs)
```
- Context: runs `fn` inside a **copy** of the caller's contextvars context.
- Cancellation: the default `cancellable=False` shields the await until the thread finishes (the thread can't be stopped). With `cancellable=True`, the await raises `Cancelled` immediately and the result is discarded when the thread finishes (AnyIO's "abandon on cancel").
- Default limiter: `porto.to_thread.default_limiter()` with 40 tokens, matching AnyIO's default.

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
`porto::task::spawn_blocking(f) -> JoinHandle<R>` and `porto::task::block_in_place` (not provided; it is deliberately omitted because it breaks lane attachment invariants).

## 5. Running sync code in lanes ("inline")
Nothing prevents users from running sync code in a lane. The watchdog (§23) reports steps longer than `slow_step_threshold`.
