# 04 — Architecture Decision Records (index)

Format: **ID — Decision.** *Status.* Rationale (short). Alternatives rejected.

| ID | Decision | Status |
|----|----------|--------|
| ADR-001 | Porto owns its task model. Python coroutines are stepped by Rust via `PyIter_Send` and a **trap protocol**, with no `asyncio.Task`/`Future` per await. | Accepted |
| ADR-002 | Porto is also a pure-Rust runtime (`porto` crate). It is not built on Tokio, because we need control over thread-state attachment, deterministic scheduling, a Python-aware budget, and the task registry. A Tokio *bridge* exists for Tokio-only crates. | Accepted |
| ADR-003 | Execution unit = **lane** (one OS thread, one run queue, one optional reactor share). GIL builds: Python runs on lane 0. Extra lanes run Rust tasks only. Free-threaded builds: N Python-capable lanes. | Accepted |
| ADR-004 | **Single runtime per process**, owned by `porto._core`. Other native code (Douro, third parties) talks to it only through the versioned C ABI `porto-abi` exposed as a PyCapsule. | Accepted |
| ADR-005 | I/O model = **readiness** (epoll/kqueue/IOCP-AFD via mio) as the universal ABI. io_uring is an opt-in Linux backend that implements readiness through multishot poll, plus completion fast paths for owned-buffer ops inside `porto-core` only. | Accepted |
| ADR-006 | Default Linux backend is epoll. io_uring is opt-in, probed, with automatic fallback. | Accepted |
| ADR-007 | Structured concurrency is mandatory. `porto.scope()` is both cancel scope and nursery. There is no global `spawn` in the public Python API, except `Runtime.root_scope` as an explicit escape hatch. | Accepted |
| ADR-008 | Cancellation is **level-triggered** (Trio semantics) with deadlines. `porto.Cancelled` subclasses `asyncio.CancelledError` for ecosystem compatibility. | Accepted |
| ADR-009 | Exception groups are always strict. | Accepted |
| ADR-010 | `scope.spawn` accepts a coroutine object *or* an async callable plus args. | Accepted |
| ADR-011 | The coop budget is 128 "operations" per task step, as in Tokio. Ready-fast-path awaits consume budget and perform a cancel check. | Accepted |
| ADR-012 | Timer = hierarchical wheel, 6 levels × 64 slots, 1 ms default resolution. Timers never fire early. | Accepted |
| ADR-013 | Bounded channels by default. The unbounded variant is a separately named function. | Accepted |
| ADR-014 | asyncio compat via a per-lane **facade loop** on Porto's reactor. It is on by default and created lazily. | Accepted |
| ADR-015 | Native AnyIO backend installed into AnyIO's private cache (version-gated). Upstream registry proposal in parallel. | Accepted |
| ADR-016 | Deterministic test mode is part of the core runtime, not a separate fork: a seeded scheduler, a virtual clock, and a simulated net driver. | Accepted |
| ADR-017 | Douro HTTP engine = hyper 1.x on Porto I/O through the ABI. HTTP/3 deferred. | Accepted |
| ADR-018 | Douro does routing, extraction, validation, and serialization in Rust. Exactly one Python call per request on the common path. | Accepted |
| ADR-019 | Douro uses **oxiform** (sibling project) for validation, serialization, JSON Schema, and config. No own schema engine. Integration via oxiform's embedding C ABI (OX-1). Pydantic accepted through a Douro adapter until oxiform's shim exists. See §34. | Accepted (revised 2026-10-31) |
| ADR-020 | Sync `def` handlers run on the blocking pool by default. `inline=True` opts out. | Accepted |
| ADR-021 | Default request policy: client disconnect cancels the request scope. | Accepted |
| ADR-022 | Douro supervisor: pre-spawn (exec) workers by default; `--preload` forks before any runtime start. The listener FD is owned by the supervisor and inherited (portable). SO_REUSEPORT is optional. | Accepted |
| ADR-023 | Error responses follow RFC 9457 (`application/problem+json`). | Accepted |
| ADR-024 | Python min 3.11 (GIL). Free-threaded min 3.14t. Version-specific wheels, no abi3 (abi3 is unavailable for free-threaded builds at time of writing). | Accepted |
| ADR-025 | Runtime default is 1 Python lane. Douro default on free-threaded builds is `lanes="auto"` (= physical cores, max 16). | Accepted |
| ADR-026 | No OpenSSL dependency: rustls everywhere, `aws-lc-rs` crypto provider by default, `ring` as a feature. | Accepted |
| ADR-027 | Rust-side panics in task context are caught and become `porto.RustPanic`. Panics in scheduler internals abort. | Accepted |
| ADR-028 | Public Python API is fully typed (PEP 561 `py.typed`, `.pyi` stubs for native modules), checked by pyright strict in CI. | Accepted |
| ADR-029 | License: MIT OR Apache-2.0 (dual), matching the Rust ecosystem. | Accepted (2026-11-04) |
| ADR-030 | Telemetry: none. Nothing phones home, ever. | Accepted |
| ADR-031 | Rust **edition 2024**. `rust-toolchain.toml` pins the latest stable (1.99.1 on 2026-11-04) and is bumped deliberately, in its own commit, when a new stable ships. Published crates (`porto`, `porto-abi`, `porto-pyo3`) declare `rust-version` = the pinned toolchain minus 2 minor releases, checked in CI from M3. Internal crates follow the pin. | Accepted (2026-11-04) |
| ADR-032 | Docs are built with **Zensical** (the successor of Material for MkDocs, which entered maintenance mode in 2026) from an `mkdocs.yml`-compatible config, with the API reference generated by **mkdocstrings-python** from the `.pyi` stubs and docstrings. If Zensical blocks us, the same config builds with mkdocs-material 9.x. | Accepted (2026-11-04) |
| ADR-033 | The benchmark reference machine is the maintainer's **Apple M4 Pro Mac** (10 performance + 4 efficiency cores, 48 GB, macOS, kqueue) until a dedicated Linux box exists. See `53-benchmarks.md` §1. | Accepted (2026-11-04) |
| ADR-034 | Speedup gates **tighten by milestone**: M0 kill criterion ≥ 2× asyncio+uvloop (spawn+join, task switch) and §11 absolute targets within 30%; M1 gate = §11 absolute targets and ≥ 2.5×; 1.0 (S1) = the full §53 table (spawn+join ≥ 3×, switch ≥ 2.5×, …). Size budgets: task `Header` ≤ 64 B including the registry's intrusive node (hard ceiling 128 B); `PyTaskBody` ≤ 192 B including the embedded timer entry and wait node, excluding the coroutine. | Accepted (2026-11-04) |

## Rejected alternatives (selected)

- **R1 "Build on Tokio and wrap."** Tokio workers do not know about Python thread states. We would need to attach and detach on every poll, or keep attachment always and block GC stop-the-world. Tokio also has no deterministic mode or Python-aware budget, and no ABI-stable way to share one Tokio runtime across extensions. *Kept only as a bridge.*
- **R2 "Be an asyncio loop (like uvloop)."** See ST-1.
- **R3 "Completion-based ABI (io_uring-first)."** It forces owned-buffer APIs on every extension and is blocked in many containers. Readiness is the portable common denominator.
- **R4 "Implicit global spawn for convenience."** This is the source of leaked tasks and lost exceptions in asyncio. Rejected.
- **R5 "Run Python on a separate thread from I/O in GIL builds."** Every request would pay a cross-thread wake (eventfd + futex) twice. The single lane does I/O and Python, and scaling comes from processes.
