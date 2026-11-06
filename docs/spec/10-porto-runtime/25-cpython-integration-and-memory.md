# 25 — CPython Integration, Memory & FFI Rules

## 1. Build matrix
CPython 3.11, 3.12, 3.13, 3.14 (GIL) and 3.14t (free-threaded, and later t builds). There is no abi3 (ADR-024). The module declares `Py_mod_gil = Py_MOD_GIL_NOT_USED` (via PyO3's `gil_used = false`).

## 2. Thread-state rules
- **P0** A lane thread creates exactly one `PyThreadState` for its lifetime (Python lanes and blocking workers). It never uses `PyGILState_*` on lanes, because explicit thread states are faster and safe across sub-runtime restarts.
- **P1** `Py<T>`/`PyObject*` may be *owned* only by: (a) Python lanes, (b) blocking-pool workers while running a Python job, (c) data structures whose `Drop` routes through `defer_decref`. Rust-only lanes and the driver never drop Python references directly. Debug builds assert `thread_is_attached()` in `PyObj::drop`.
- **P2** Lanes MUST detach (`PyEval_SaveThread`) before: parking, `epoll_wait`/`turn` with nonzero timeout, futex waits, and joining threads.
- **P3** Lanes MUST NOT hold any Rust mutex while calling into Python, because Python code may re-enter Porto and deadlock. Exception: per-object critical sections managed by CPython.

## 3. Free-threaded specifics
- Use the `*Ref` C APIs (`PyDict_GetItemRef`, `PyList_GetItemRef`, `PyWeakref_GetRef`) everywhere. Lint with a CI grep that bans borrowed-ref APIs in `porto-py`.
- Python objects implemented in Rust (pyclasses) are `#[pyclass(frozen)]` wherever possible. Mutable state uses Rust atomics or `Mutex` internally, never `&mut self` pymethods on shared objects (PyO3 borrow checking under free-threading is a runtime cost and a panic source).
- Iteration over user dicts (e.g., kwargs analysis in Douro at startup) uses `Py_BEGIN_CRITICAL_SECTION` equivalents (`pyo3::sync::with_critical_section`).

## 4. Reference counting & hot paths
- Interned strings for attribute names (`"throw"`, `"_asyncio_future_blocking"`, `"send"`) are created once per interpreter (`GILOnceCell`/`PyOnceLock`).
- Results passed into `PyIter_Send` transfer ownership (steal semantics wrapper). Avoid temporary `Py_INCREF/DECREF` pairs.
- Freelists: `_Trap` (512), `_Ready` (1024), `asyncio Handle` facade objects (256).
- A freelisted object the collector can see through (one with a `__traverse__`) is tracked again only after its new value is written: `tp_alloc` returns it untracked, and the code that writes the value tracks it. Tracking it in `tp_alloc` would expose the old value, whose references were already released, to a traversal (a debug CPython traverses at once to validate).

## 5. Memory budgets
| Object | Target size |
|---|---|
| Task header | ≤ 64 B |
| PyTask body (excl. coroutine) | ≤ 192 B including embedded timer entry and wait node |
| Scope node | ≤ 160 B |
| Registration (ScheduledIo) | ≤ 64 B |
| Trap | ≤ 48 B (Python object header + enum pointer) |

## 6. Fork handling
`os.register_at_fork(before=…, after_in_parent=…, after_in_child=…)`:
- `before`: if a runtime is running, emit RuntimeWarning("fork() while porto runtime is running is unsafe").
- `after_in_child`: mark every runtime in the child as poisoned. Any use raises `RuntimeClosed("runtime does not survive fork()")`. Fresh runtimes may be created in the child.

## 7. Interpreter finalization
At `Py_Finalize`, an `atexit` hook shuts down any runtime still running. Lanes must not touch Python after `Py_IsFinalizing()` is true. They drop deferred decrefs without executing them (leak instead of crash).

## 8. Panics
- `porto-py` is built with `panic = "unwind"`.
- Every FFI entry point (`#[pymethods]` and ABI functions) catches unwinds. Inside a task poll → `RustPanic` failure of that task (ADR-027). Inside scheduler internals → `std::process::abort()` after logging.
