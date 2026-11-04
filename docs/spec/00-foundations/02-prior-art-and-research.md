# 02 — Prior Art & Research Notes

This file records what exists, what each project teaches us, and what we take or reject. Research was done in October 2026. Re-verify version-specific facts at implementation time.

## 1. CPython asyncio (stdlib)

- **Model:** a single-threaded event loop, `Future` objects, and `Task` objects that step coroutines via `call_soon` callbacks. Each `await` on an I/O op produces a Future and a done-callback round trip through the loop's ready queue.
- **3.14 changes (important competitor baseline):** asyncio was made thread-safe for the free-threaded build. It uses lock-free structures and per-thread state, and it now scales roughly linearly when you run one event loop per thread. Tasks moved from a global WeakSet to a per-thread intrusive linked list stored on the thread state. The current task also lives on the thread state, which lets external tools like `python -m asyncio pstree` inspect tasks.
  - Source: Quansight Labs, *Scaling asyncio on Free-Threaded Python* (2025-09); CPython `InternalDocs/asyncio.md`.
- **Takeaways:**
  - The "N loops on N threads" baseline is free in 3.14t. Porto's multi-lane story must beat it with **work stealing, cross-lane structured cancellation, and Rust-side I/O**.
  - Making introspection visible to external tools is a good idea. Porto exposes a similar task registry (§`23`).
  - asyncio's async-generator finalization hooks (`sys.set_asyncgen_hooks`) are needed for correct `finally` semantics. Porto MUST install equivalent hooks (§`11` §9).

## 2. uvloop
- A libuv-based asyncio loop written in Cython, and the de facto "fast asyncio".
- Takeaway: it speeds up the loop and transports, but asyncio's task model is unchanged. That is the ceiling we described in `01` §2.

## 3. RLoop and rsloop (Rust asyncio loops)
- **RLoop** (Giovanni Barillari, also the author of Granian): an asyncio selector loop in Rust on mio. Unix-only, marked work-in-progress and not production-ready. It lacks SSL, Unix sockets, and debug features.
- **rsloop**: a PyO3 asyncio loop. Each loop owns a dedicated Rust runtime thread for polling and timers, while Python callbacks run on the thread that calls `run_forever`. Its native fast-stream path bypasses asyncio streams. It also exposes a small Rust interop API, so downstream PyO3 extensions can return Rust futures as awaitables.
  - **Published benchmark (CPython 3.14, Linux):** callbacks are ~2.1× faster than uvloop, tasks are slightly *slower* than uvloop (0.92×), and TCP streams are at parity.
- **Takeaways:**
  - This is the strongest evidence for thesis 1 (`01` §2). Fixing the loop alone does not move task or stream throughput.
  - rsloop's dedicated I/O thread adds cross-thread wakeups. Porto's default lane drives its own reactor on the same thread (§`12`), and off-lane I/O threads are opt-in.
  - The "Rust future → Python awaitable" interop is valuable. Porto makes it a stable C ABI (§`21`), so it works across separately compiled extensions.

## 4. Trio / Curio
- **Trio:** nurseries, cancel scopes, level-triggered cancellation, deadlines, checkpoints, `task_status.started()`, strict exception groups, and MockClock with autojump.
- **Curio:** coroutines communicate with the kernel through **traps**, which are yielded tuples. This is the conceptual ancestor of Porto's trap protocol.
- **Takeaways:**
  - We adopt Trio's semantics nearly wholesale (§`15`).
  - We adopt Curio's trap idea, but traps are Rust objects checked by pointer comparison, not Python tuples (§`11`).
  - We reject Trio's insistence on `spawn(fn, *args)`. Porto also accepts coroutine objects, because that is what users expect from asyncio's `TaskGroup.create_task`.

## 5. AnyIO
- A structured-concurrency API over asyncio and Trio, and the base of Starlette, httpx, FastAPI internals, and more.
- **Research finding:** AnyIO has no plugin mechanism for backends. Built-in backend names are a hard-coded tuple (`"asyncio", "trio"`). Backends are loaded by importing `anyio._backends._{name}` and cached in a private module-level dict. Backend detection goes through `sniffio`.
- **Takeaway:** a native `porto` AnyIO backend is the highest-leverage compat move, but it requires either (a) an upstream entry-point-based backend registry, which we should propose, or (b) installing the backend into AnyIO's private cache. Porto MUST implement (b) behind a version check and pursue (a) (§`22`).

## 6. Tokio (Rust)
- Work-stealing multi-thread scheduler. Each worker has a local queue (256 slots) plus a **LIFO slot** for message-passing locality, a global injector, a coop budget (128) per task poll, a shared I/O driver polled by the parked worker, a hierarchical timer wheel (6 levels × 64 slots, 1 ms), and a blocking pool (default max 512 threads).
- **Takeaways:**
  - Porto's scheduler is deliberately Tokio-shaped (§`12`).
  - Porto's Rust API mirrors Tokio's names where sensible (§`21`).
  - We keep the coop budget, the LIFO slot, and the timer wheel.

## 7. Granian
- A Rust HTTP server for Python, built on Hyper and Tokio. It supports ASGI, WSGI, and its own RSGI interface. It offers two threading modes: N single-threaded runtimes ("workers") or one multi-threaded runtime ("runtime"). It also has an optional Rust task implementation for Python coroutines.
- **Published numbers (third-party framework README, single worker):** raw RSGI is roughly 1.4× raw ASGI on Granian. FastAPI on Granian ASGI is roughly 5× slower than raw RSGI on JSON.
- **Takeaways:**
  1. The interface protocol between server and app matters (RSGI vs ASGI ≈ 40%).
  2. **The framework tax dominates.** The 5× gap is framework code written in Python. Douro moves the framework into Rust.

## 8. Robyn, Socketify, BlackSheep, Litestar, msgspec, pydantic-core
- **Robyn:** a Rust server with Python handlers. It shows that "Rust server + Python framework logic" is not enough. Every request still crosses the boundary many times, through middleware, parsing, and responses.
- **Socketify:** uWS bindings. Very fast plaintext, weaker typed-API ergonomics.
- **msgspec / pydantic-core:** prove that compiled validators driven by schemas, written in native code, beat Python validation by 10–50×. Douro builds a comparable engine in Rust that is fused with the JSON parser (§`34`).
- **Litestar:** good DI and DTO ideas, implemented in Python.

## 9. Free-threaded CPython facts we depend on
- 3.14 made the free-threaded build officially supported under PEP 779. It is still opt-in, with roughly 5–10% single-thread overhead in 3.14. Phase III, where it becomes the default, has no date.
- Extensions MUST declare `Py_mod_gil = Py_MOD_GIL_NOT_USED`. If an extension does not, importing it re-enables the GIL at runtime and prints a warning. **Porto must detect this at runtime and adapt (§`12` §8).**
- `PyDict_Next` and borrowed-reference APIs are unsafe under concurrent mutation. Use the `*Ref` variants and `Py_BEGIN_CRITICAL_SECTION`.
- Even without a GIL, extensions should still call `PyEval_SaveThread` around blocking operations, because that is what lets other threads run the cyclic GC (stop-the-world). **Porto lanes MUST detach before parking (§`25`).**

## 10. io_uring in practice
- Docker's default seccomp profile blocks the io_uring syscalls (moby issue #47532, Docker 25+). io_uring has also been a frequent source of kernel CVEs, and several hardened environments disable it.
- **Takeaway:** epoll is the default Linux backend. io_uring is opt-in, probed at startup, and falls back to epoll automatically with a one-line warning (§`13`).

## 11. Rust ecosystem building blocks we will use
| Need | Crate | Notes |
|------|-------|-------|
| Readiness polling (epoll/kqueue/IOCP-AFD) | `mio` 1.x | Mature, cross-platform |
| io_uring | `io-uring` | Low-level, opt-in |
| HTTP/1.1 + HTTP/2 | `hyper` 1.x | Runtime-agnostic via `hyper::rt` traits |
| TLS | `rustls` (+ `rustls-native-certs`/`webpki-roots`) | No OpenSSL |
| WebSocket | `tungstenite` (sans-io core) | Driven by our I/O |
| JSON | `serde_json` + a custom visitor; `simd-json`/`sonic-rs` behind a feature flag | §34 |
| Python bindings | `pyo3` (version with free-threaded support) | §25 |
| ABI-stable futures | `async-ffi` | §21 |
| Lock-free queues | `crossbeam-deque`, `crossbeam-utils` | §12 |
| Concurrency model checking | `loom` | §52 |
| Tracing | `tracing`, `opentelemetry` | §23 |
| TUI | `ratatui` | console |
| Routing | own radix tree (inspired by `matchit`) | §32 |
