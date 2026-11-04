# 01 — Vision, Goals, Non-Goals, Success Criteria

## 1. Vision

Porto aims to be the Tokio of Python. Python developers should get Tokio's performance, correctness, and observability while writing ordinary `async def` code. Douro aims to be the fastest production-grade Python API framework with FastAPI-level DX, and the speed should come from architecture rather than micro-tricks.

## 2. The core thesis

We tested three claims while writing this spec (see `03-stress-tests.md`):

1. **The event loop is not the bottleneck.** Swapping asyncio's loop for a Rust one gives large wins on raw callbacks. Task-heavy and stream-heavy workloads gain little, because overhead lives in `asyncio.Task`/`Future` objects, in callback trampolines, and in protocol parsing written in Python. **Therefore Porto does not implement asyncio's model faster. It replaces the model.**
2. **The Python/Rust boundary is the unit of cost.** Every crossing costs something: attaching a thread state, creating Python objects, and refcount traffic. Fast systems minimize crossings per unit of work. **Therefore Douro is designed around one crossing per request.** The only crossing is the call into the handler. Its arguments are already validated, and the return value is serialized in Rust.
3. **Free-threaded CPython changes the scaling story.** CPython 3.14 officially supports a no-GIL build. Since 3.14, stdlib asyncio scales with one event loop per thread. Porto must offer more than that: work-stealing across lanes, Rust-side parallel I/O, and structured cancellation that crosses lanes safely.

## 3. Goals

### Porto (runtime)
- **G1 Performance.** Task spawn+join, task switch, channel ping-pong, and TCP echo MUST beat asyncio+uvloop on CPython 3.14 by the margins in `50-engineering/53-benchmarks.md` (§Gates).
- **G2 Correctness by construction.** Structured concurrency is the only first-class way to spawn. Cancellation is level-triggered and deadline-based. No task can outlive its scope unless the user explicitly reaches for the root scope.
- **G3 Free-threaded scaling.** On 3.14t, throughput of independent Python tasks SHOULD scale ≥ 0.7× linearly up to 8 lanes.
- **G4 Ecosystem reach.** asyncio-based libraries (asyncpg, redis-py asyncio, aiohttp client, httpx via AnyIO) MUST work inside a Porto runtime through the compat layer (`22-asyncio-anyio-compat.md`).
- **G5 Rust interop.** Any PyO3 extension MUST be able to hand Porto a Rust future as a Python awaitable and drive I/O on Porto's reactor through a stable C ABI (`21-rust-api-and-abi.md`).
- **G6 Observability.** Developers get a live task tree, per-task timings, blocking-step detection with Python stacks, a TUI console, and OpenTelemetry. All of it is built in, not bolted on.
- **G7 Deterministic testing.** A seeded scheduler, a virtual clock, and a simulated network make any concurrency bug reproducible with one seed.
- **G8 Usable from pure Rust.** The `porto` crate is a complete Rust async runtime (tasks, I/O, timers, sync). Douro's native code is its first consumer.

### Douro (framework)
- **G9 Throughput.** Douro's framework tax (Douro vs Porto's raw HTTP handler) MUST be ≤ 20% on the JSON benchmark. Douro MUST exceed FastAPI-on-Uvicorn by ≥ 5× and FastAPI-on-Granian by ≥ 3× per core on the standard suite (`53-benchmarks.md`).
- **G10 DX parity with FastAPI.** Type-hint driven routing, validation, DI, and OpenAPI. A FastAPI user is productive in < 1 hour.
- **G11 Structured requests.** Each request runs in a scope. Client disconnects cancel the scope, timeouts are deadlines, and background work is explicitly owned.
- **G12 Production readiness.** HTTP/1.1, HTTP/2, TLS, WebSockets, SSE, graceful shutdown, a pre-fork supervisor, and limits that are secure by default.
- **G13 Migration path.** Existing ASGI apps can be mounted under a Douro app.

## 4. Non-goals (v1)

- **NG1** Being a drop-in `asyncio` event loop replacement. The compat layer exists, but it is not the product.
- **NG2** PyPy, GraalPy, and subinterpreter-per-lane support. Subinterpreters (PEP 734) are a v2 research item (`55-risks-open-questions.md`).
- **NG3** Python < 3.11 (no native `ExceptionGroup`) and 3.13t (experimental free-threading). Supported targets: CPython 3.11–3.14+ (GIL) and 3.14t+ (free-threaded).
- **NG4** HTTP/3/QUIC in v1. It is designed for (§`31`) and implemented in M9+.
- **NG5** A full ORM or template engine. Douro integrates with existing ones.
- **NG6** WSGI. Sync handlers are supported, but WSGI apps are not mountable.
- **NG7** Windows io_uring-equivalents (IoRing). Windows uses IOCP through the readiness emulation in `13`.

## 5. Success criteria (v1.0 release gates)

| ID | Criterion | Measured by |
|----|-----------|-------------|
| S1 | All `53-benchmarks.md` gates pass on the reference machine | CI perf job |
| S2 | asyncpg, redis.asyncio, httpx, and aiohttp client test suites pass under `porto.run(..., asyncio_compat=True)` with ≥ 95% of tests | compat CI |
| S3 | Zero known data races under TSAN and loom model checks of core structures | CI |
| S4 | Deterministic test mode reproduces 100% of seeded failures across 1000 reruns | CI |
| S5 | Douro passes the h2spec and Autobahn (WebSocket) conformance suites | CI |
| S6 | 72-hour soak at 80% capacity: no RSS growth beyond 5%, no FD leak | soak job |
| S7 | Docs cover 100% of the public API with runnable examples | doc tests |
