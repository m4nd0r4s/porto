# 22 — asyncio & AnyIO Compatibility

## 1. Goals and limits
- Run asyncio-native libraries (asyncpg, redis.asyncio, aiohttp client, aiokafka, motor) **inside** Porto tasks.
- Run `asyncio.run`-style programs under Porto with `porto.asyncio.install()`, which is useful for gradual adoption.
- Non-goal: 100% asyncio test-suite pass. Target: the libraries listed in S2 (`01`).

## 2. Facade loop
`porto.asyncio.EventLoop(asyncio.AbstractEventLoop)`, implemented **in Rust** as a pyclass. There is one per Python lane, created lazily on first use, and it is registered as the running loop for that thread (`asyncio._set_running_loop`) while the lane is running.

| asyncio API | Implementation |
|---|---|
| `call_soon`, `call_soon_threadsafe`, `call_later`, `call_at` | Handles stored in a per-lane callback queue drained by the lane like a high-priority task. Timers are on the wheel. Returns `asyncio.Handle`/`TimerHandle`-compatible objects (Rust pyclasses with `cancel()`, `cancelled()`, `when()`) |
| `time()` | `porto.current_time()` |
| `create_future()` | stdlib `asyncio.Future(loop=self)` (C-accelerated `_asyncio.Future`) |
| `create_task(coro, name, context)` | **real `asyncio.Task`** (C `_asyncio.Task`) bound to the facade loop. asyncio code keeps its exact semantics (uncancel, cancel messages, current_task) |
| `add_reader/remove_reader/add_writer/remove_writer` | driver registrations |
| `sock_recv/sock_sendall/sock_connect/sock_accept/...` | driver + nonblocking ops |
| `create_connection`, `create_server`, `create_unix_*`, `create_datagram_endpoint` | Rust transports implementing `asyncio.Transport`/`DatagramTransport` with the protocol callback interface (`data_received`, `connection_lost`, flow control `pause_writing`/`resume_writing`), plus `BufferedProtocol` support |
| TLS (`ssl=`) | stdlib `ssl.SSLContext` via asyncio's `sslproto` layered on our transport (correctness over speed). Native rustls only for Porto-native APIs |
| `getaddrinfo`, `getnameinfo` | Porto DNS |
| `run_in_executor` | blocking pool (default executor) or the given executor |
| `subprocess_exec/_shell` | `porto.process` wrapped in asyncio subprocess transports |
| `add_signal_handler` | `porto.signals` |
| `run_forever`, `run_until_complete`, `stop`, `close` | Only valid in `install()` mode, where Porto owns the thread. Inside a running Porto runtime they raise `RuntimeError` |
| `shutdown_asyncgens`, `shutdown_default_executor` | supported |

## 3. Bridging awaits
- **Porto task awaits an asyncio future/task** (`PYGEN_NEXT` yields an object with `_asyncio_future_blocking=True`): the driver sets the flag back to False, calls `fut.add_done_callback(waker_cb)` (a Rust callable that wakes the PyTask), and parks the task in `Pending::AsyncioFuture`. On wake it resumes with `None`; the coroutine's own `await` then reads `fut.result()`. On Porto cancellation it calls `fut.cancel()` and then delivers `Cancelled` (shielded until the future acknowledges, max 1 s, then abandons with a warning).
- **asyncio Task awaits a Porto Trap** (T4): the Trap's `_asyncio_future_blocking` property returns True and it implements `add_done_callback`, `remove_done_callback`, `result`, `cancel`, and `get_loop` as a minimal Future duck type. The facade resolves the trap through a hidden driver path and completes it via callbacks. This is a slow path, but correct.
- **`asyncio.current_task()`** inside a Porto task returns `None`. Some libraries require a current task (e.g., `asyncio.timeout`). Mitigation: **auto-hosting**. In compat mode, when a Porto task calls an asyncio API that needs `current_task()`, a lightweight *proxy* `asyncio.Task` subclass instance is registered as the current task on the facade loop for the duration of the step (`asyncio._enter_task/_leave_task`). Its `cancel()` maps to Porto task cancellation. This makes `asyncio.timeout()` work inside Porto tasks.

## 4. sniffio
Porto sets `sniffio.thread_local.name = "porto"` in Porto task steps. Inside asyncio tasks running on the facade, sniffio reports `"asyncio"`, which is what asyncio's own detection yields.

## 5. AnyIO backend
- Module `porto._anyio_backend` implements `anyio.abc.AsyncBackend`: `run`, `current_token`, `current_time`, `cancelled_exception_class` (→ `porto.Cancelled`), `checkpoint`, `checkpoint_if_cancelled`, `cancel_shielded_checkpoint`, `sleep`, `create_cancel_scope` (→ wraps `porto.scope`), `current_effective_deadline`, `create_task_group`, `create_event`, `create_lock`, `create_semaphore`, `create_capacity_limiter`, `run_sync_in_worker_thread`, `run_async_from_thread`, `run_sync_from_thread`, `create_blocking_portal`, `open_process`, `connect_tcp`, `connect_unix`, `create_tcp_listener`, `create_unix_listener`, `create_udp_socket`, `getaddrinfo`, `wait_readable`/`wait_writable`, `current_default_thread_limiter`, `open_signal_receiver`, `get_current_task`, `get_running_tasks`, `wait_all_tasks_blocked`, `create_test_runner`.
- Installation (`anyio_backend="native"`): at runtime start, if `anyio` is importable and its version is within `ANYIO_TESTED_RANGE`:
  1. `sys.modules["anyio._backends._porto"] = porto._anyio_backend`
  2. If `anyio._core._eventloop.loaded_backends` exists, insert `"porto"` → backend class.
  3. Ensure `BACKENDS` contains `"porto"` (rebind the tuple).
  If the version is outside the range, fall back to `"facade"` (AnyIO sees `"asyncio"` and runs on the facade via auto-hosting) and log at INFO.
- Upstream: propose `anyio.backends` entry-point group. Track it in `55`.

## 6. `porto.asyncio.install()`
Sets the asyncio event loop policy (or, on 3.14+, the default loop factory mechanism; see `asyncio.run(loop_factory=...)`) so that `asyncio.run(main())` creates a `Runtime` with lane 0 = this thread, where the facade loop is the running loop and `main` runs as a real `asyncio.Task`. Porto-native APIs work inside it via the T4 path, and users can opt into native tasks with `porto.asyncio.native(coro)`.
