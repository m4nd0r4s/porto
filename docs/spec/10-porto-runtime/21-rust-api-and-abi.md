# 21 — Rust API & the C ABI

## Part A — `porto` crate (pure Rust users)

Tokio-shaped on purpose, to ease porting.

```rust
#[porto::main(lanes = 4)]
async fn main() -> std::io::Result<()> {
    let listener = porto::net::TcpListener::bind("0.0.0.0:8080").await?;
    porto::scope(|s| async move {
        loop {
            let (sock, _) = listener.accept().await?;
            s.spawn(async move { handle(sock).await });
        }
    }).await
}
```

| Module | Items |
|---|---|
| `porto::runtime` | `Builder` (`lanes`, `io_backend`, `steal`, `enable_python(false)`), `Runtime`, `Handle` (`spawn`, `block_on`, `current()`) |
| `porto::task` | `spawn` (into current scope; panics if none), `spawn_blocking`, `JoinHandle<T>`, `yield_now`, `coop::{budget, poll_proceed}`, `TaskId` |
| `porto::scope` | `scope(f) -> Future<Output = Result<R, ScopeError>>`, `Scope::{spawn, cancel, deadline, set_deadline}`, `CancellationToken` (cancel-only, cloneable) |
| `porto::time` | `sleep`, `sleep_until`, `timeout`, `interval`, `Instant` (virtualizable) |
| `porto::net` | `TcpListener`, `TcpStream` (`AsyncRead`/`AsyncWrite` from `porto::io`), `UdpSocket`, `UnixStream`, `UnixListener`, `lookup_host` |
| `porto::io` | `AsyncRead`, `AsyncWrite`, `AsyncReadExt`, `AsyncWriteExt`, `split`, `copy`, `BufReader`, `Interest`, `Ready`, `AsyncFd` |
| `porto::sync` | `Mutex`, `RwLock`, `Semaphore`, `Notify`, `Barrier`, `mpsc`, `oneshot`, `broadcast`, `watch` |
| `porto::tls` | rustls wrappers: `TlsAcceptor`, `TlsConnector` |
| `porto::signal`, `porto::process`, `porto::fs` | |
| `porto::compat::tokio` | `TokioBridge::run(fut)`: run a Tokio-requiring future on a lazily started sidecar Tokio runtime and await the result |
| `porto::compat::hyper` | `PortoExecutor`, `PortoTimer`, `PortoIo<T>` implementing `hyper::rt::{Executor, Timer, Read, Write}` |
| `porto::compat::futures_io` | `AsyncRead`/`AsyncWrite` adapters for the `futures` traits |

Rust-side structured concurrency: `scope` futures own `'static` spawned futures. Dropping the scope future cancels all children (drop = cancel) and waits for them before completing; if it is dropped, it detaches the wait into a runtime-owned reaper so no child outlives without being dropped.

## Part B — porto-abi (C ABI for extensions)

### B.1 Why
ST-8: every cdylib has its own statics. The runtime must have **one** owner (`porto._core`). Extensions access it through a function table obtained at import time.

### B.2 Discovery
```c
PyObject *cap = PyImport_ImportModuleAttrString("porto._core", "_ABI_V1");  // PyCapsule named "porto._core._ABI_V1"
const PortoApiV1 *api = PyCapsule_GetPointer(cap, "porto._core._ABI_V1");
assert(api->abi_major == 1 && api->struct_size >= sizeof(PortoApiV1));
```
Rust consumers use `porto_abi::api() -> &'static PortoApi` (cached in a `OnceLock`). It panics with a clear message if `porto` is not installed or the major version mismatches.

### B.3 Versioning rules
- `abi_major` bumps on any incompatible change. Additive changes append fields and bump `abi_minor`. Consumers check `struct_size` before using a field.
- `porto` wheels export *all* supported majors (`_ABI_V1`, `_ABI_V2`, …) for at least 2 minor releases after a major bump.

### B.4 The table (v1)

```rust
#[repr(C)]
pub struct PortoApiV1 {
    pub abi_major: u32, pub abi_minor: u32, pub struct_size: u32,

    // --- runtime handle ---
    pub current_runtime: extern "C" fn() -> RtHandle,              // null if none on this thread
    pub runtime_clone: extern "C" fn(RtHandle) -> RtHandle,
    pub runtime_drop: extern "C" fn(RtHandle),

    // --- tasks ---
    pub spawn: extern "C" fn(RtHandle, FfiFuture<()>, SpawnOpts) -> TaskHandle,   // into current scope
    pub spawn_in_scope: extern "C" fn(ScopeHandle, FfiFuture<()>, SpawnOpts) -> TaskHandle,
    pub task_abort: extern "C" fn(TaskHandle),
    pub task_join: extern "C" fn(TaskHandle) -> FfiFuture<JoinResult>,
    pub task_drop: extern "C" fn(TaskHandle),
    pub spawn_blocking: extern "C" fn(RtHandle, BlockingFn) -> FfiFuture<()>,

    // --- scopes / cancellation ---
    pub scope_current: extern "C" fn() -> ScopeHandle,
    pub scope_new: extern "C" fn(parent: ScopeHandle, deadline_ns: u64, shield: bool) -> ScopeHandle,
    pub scope_cancel: extern "C" fn(ScopeHandle),
    pub scope_wait_children: extern "C" fn(ScopeHandle) -> FfiFuture<()>,
    pub scope_drop: extern "C" fn(ScopeHandle),
    pub cancelled: extern "C" fn(ScopeHandle) -> FfiFuture<()>,     // completes when scope effectively cancelled

    // --- time ---
    pub now_ns: extern "C" fn(RtHandle) -> u64,                      // runtime clock (virtual in sim)
    pub sleep_until: extern "C" fn(RtHandle, deadline_ns: u64) -> FfiFuture<()>,

    // --- I/O readiness ---
    pub io_register: extern "C" fn(RtHandle, RawFdOrSocket, interest: u8, out: *mut IoHandle) -> i32, // errno
    pub io_poll_ready: extern "C" fn(IoHandle, interest: u8, cx: *mut FfiContext, out_ready: *mut u32) -> u8, // 0=Ready 1=Pending
    pub io_clear_ready: extern "C" fn(IoHandle, ready_tick: u32),
    pub io_deregister: extern "C" fn(IoHandle),

    // --- Python bridge ---
    pub future_into_py: extern "C" fn(FfiFuture<PyResultPtr>) -> *mut PyObject,  // returns awaitable (Trap::AbiFuture)
    pub py_await: extern "C" fn(awaitable: *mut PyObject) -> FfiFuture<PyResultPtr>, // await a Python awaitable from Rust (spawns a PyTask in current scope)
    pub py_spawn: extern "C" fn(ScopeHandle, coro: *mut PyObject, opts: SpawnOpts) -> TaskHandle,
    pub call_soon_py: extern "C" fn(RtHandle, callback: *mut PyObject, args: *mut PyObject) -> i32, // run sync callable on a Python lane
    pub defer_decref: extern "C" fn(RtHandle, *mut PyObject),       // safe from any thread (P1)

    // --- observability ---
    pub task_set_name: extern "C" fn(TaskHandle, *const u8, usize),
    pub record_metric: extern "C" fn(RtHandle, MetricId, f64),
}
```
- `FfiFuture<T>` and `FfiContext` come from the `async-ffi` crate, which defines ABI-stable future and waker wrappers.
- `PyResultPtr`: `{ ok: *mut PyObject, err: *mut PyObject }`, exactly one non-null, with strong references transferred.
- Handles are opaque pointers with manual refcounting (`*_drop`).
- All functions are callable from any runtime-owned thread. The `py_*` functions require the calling thread to be attached unless documented otherwise (`defer_decref` never requires it).

### B.5 Safe Rust wrapper (`porto-abi` crate, and `porto-pyo3`)
```rust
use porto_pyo3::{future_into_py, Handle};

#[pyfunction]
fn fetch_row(py: Python<'_>, id: i64) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        let conn = POOL.get().await?;                  // pure Rust future
        let row = conn.query_one(id).await?;
        Python::attach(|py| Ok(row.into_pyobject(py)?.unbind()))   // pyo3 API naming per version in use
    })
}
```
`porto_pyo3::net::TcpStream` is implemented on `io_register`/`io_poll_ready`, so extension crates get real sockets on Porto's reactor.

### B.6 Rules for extension authors (published in docs)
- Futures passed to `spawn`/`future_into_py` MUST be `Send` (lanes may migrate them).
- Never block inside a future. Use `spawn_blocking`.
- Never call `Python::attach`/`with_gil` inside a future on a Rust-only lane on GIL builds. Use `call_soon_py` or return data and convert on completion. (`future_into_py` converts results on a Python lane automatically.)
