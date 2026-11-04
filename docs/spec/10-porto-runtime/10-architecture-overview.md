# 10 — Porto Architecture Overview

## 1. Layer cake

```
┌──────────────────────────────────────────────────────────────────────┐
│ Python user code: async def …, porto.scope, porto.net, …               │
├──────────────────────────────────────────────────────────────────────┤
│ python/porto/*.py   thin pure-Python layer: typing, helpers, compat,   │
│                     anyio backend, pytest plugin                       │
├──────────────────────────────────────────────────────────────────────┤
│ porto._core  (cdylib, crate porto-py)                                  │
│   • PyTask driver (trap protocol)      • Python API objects            │
│   • asyncio facade loop                • ABI capsule export            │
├───────────────┬─────────────────────────────────┬────────────────────┤
│ porto-core    │ porto-io                         │ porto-net / fs /   │
│ scheduler,    │ drivers: epoll, kqueue, iocp     │ process / signal   │
│ tasks, scopes │ (mio), io_uring (opt), sim       │ tls (rustls), dns  │
│ time, sync    │                                  │                    │
├───────────────┴─────────────────────────────────┴────────────────────┤
│ OS                                                                     │
└──────────────────────────────────────────────────────────────────────┘
        ▲ C ABI (porto-abi, PyCapsule)                                 
        │                                                              
┌───────┴──────────────┐   ┌─────────────────────────────┐            
│ douro._native        │   │ third-party PyO3 extensions │            
│ (uses porto-abi)     │   │ (use porto-pyo3 → porto-abi)│            
└──────────────────────┘   └─────────────────────────────┘            
```

## 2. Crates (Cargo workspace)

| Crate | Kind | Depends on Python? | Purpose |
|-------|------|--------------------|---------|
| `porto-core` | lib | no | Task header, scheduler, lanes, scopes/cancel tree, timers, sync primitives, budget, registry |
| `porto-io` | lib | no | `Driver` trait + backends (mio, io_uring, sim), `Registration`, readiness |
| `porto-net` | lib | no | TCP/UDP/Unix, TLS (rustls), DNS, happy eyeballs |
| `porto-fs` | lib | no | Async FS on the blocking pool / io_uring |
| `porto-process` | lib | no | Child processes, pidfd/SIGCHLD reaping |
| `porto-signal` | lib | no | Signal delivery via self-pipe/signalfd |
| `porto` | lib | no | Facade crate re-exporting the above (the "tokio" equivalent), `#[porto::main]` via `porto-macros` |
| `porto-macros` | proc-macro | no | `#[porto::main]`, `#[porto::test]` |
| `porto-sim` | lib | no | Deterministic scheduler policy, virtual clock, simulated network |
| `porto-abi` | lib | no | `#[repr(C)]` ABI types, the vtable, a safe Rust wrapper for consumers |
| `porto-py` | cdylib (`porto._core`) | **yes** (pyo3) | Python bindings, PyTask driver, asyncio facade, capsule export |
| `porto-pyo3` | lib | yes | Public helper for third-party PyO3 crates (`future_into_py`, `PortoHandle`) built on `porto-abi` |
| `porto-tokio-bridge` | lib | no | Sidecar Tokio runtime for Tokio-only crates |
| `porto-console` | bin | no | TUI console client |

Rule A1: `porto-core`, `porto-io`, and `porto-net` MUST compile and pass tests without Python (`cargo test --workspace --exclude porto-py`).

## 3. Threads in a running process

| Thread | Count | Attached to Python? | Role |
|--------|-------|---------------------|------|
| Lane 0 ("main lane") | 1 | yes (while stepping Python) | Runs the task that calls `porto.run`. Polls the reactor when idle. |
| Lanes 1..N-1 | `lanes-1` | free-threaded: yes when stepping Python. GIL: never | Steal/run tasks |
| Blocking pool | 0..`blocking_threads` | when running Python callables | `to_thread`, FS fallback, DNS |
| Timer/driver thread | 0 | — | **None.** Timers and the I/O driver are polled by lanes (Tokio model). |
| Watchdog | 1 if `slow_step_threshold` is set | briefly (to sample stacks) | Blocking detector |
| Console server | 0/1 | no | Serves introspection snapshots |

`porto.run(main)` turns the **calling thread** into lane 0. This is required so that `main` runs on the thread that called it, which matters for signals: Python delivers signals only to the main thread.

## 4. Life of an await (single lane, fast path)

```
async def handler(stream):
    data = await stream.read(4096)
```
1. `stream.read(4096)` (Rust `#[pymethods]`) tries a nonblocking `recv`.
2. Data is available, so it returns a **ReadyAwaitable** whose `__await__` raises `StopIteration(bytes)` immediately, after a cancel check and a budget decrement. No scheduler involvement.
3. If it gets `EAGAIN`, it returns a **Trap** (kind=`Io`, registration ref, interest=READ, op=Read{max:4096}).
4. The coroutine yields the Trap. The `PyIter_Send` in the driver returns `PYGEN_NEXT` with the trap object.
5. The driver checks `Py_TYPE(obj) == TrapType`, reads `kind`, and parks the task: it stores the op and arms the waker in the registration.
6. The reactor reports readiness, the waker schedules the task, and the lane picks it.
7. The driver re-executes the op (`recv`), gets bytes, and resumes the coroutine with `PyIter_Send(coro, bytes)`.

There is one Python object per I/O completion (the result) and one Trap object per real suspension, and Trap objects are freelisted (§11 §6).

## 5. Process-level invariants

- **I1** Exactly one Porto runtime per process may run at a time (`porto.run` while running → `RuntimeError`). Nested `run` inside a blocking-pool thread is also forbidden. Use `from_thread`.
- **I2** All runtime state lives in `porto._core`. Its ABI capsule is created at module init and is valid for the process lifetime. Runtime *instances* are obtained via the ABI at call time.
- **I3** No lane ever blocks in the kernel while holding an attached thread state, except for bounded critical sections (< 1 µs).
- **I4** Every Python-visible object that references runtime internals holds a strong ref to the runtime instance (`Arc<RuntimeInner>`). Using one after shutdown raises `porto.RuntimeClosed`.
