# 42 — Performance Model

## 1. Cost accounting per request (GIL build, lane 0, keep-alive h1, `GET /users/{id}` returning a 5-field Model)
| Stage | Where | Budget |
|---|---|---|
| read + parse request head | Rust | 0.6 µs |
| route + param extract/validate | Rust | 0.2 µs |
| spawn handler PyTask (no Python Task object) | Rust/C | 0.4 µs |
| handler body (user code, trivial) | Python | ~1.0 µs |
| serialize Model (5 fields) | Rust | 0.3 µs |
| build response + write | Rust | 0.6 µs |
| **Total framework overhead** | | **≈ 2.1 µs + user code** |

That is a theoretical ceiling of ≈ 300k req/s per core before kernel/network costs. Realistic target with syscalls: ≥ 150k req/s per core on the reference machine for this endpoint (GIL build).

## 2. Rules for staying fast
- **R1** Never create Python objects the user didn't ask for (lazy `Request`, lazy headers, lazy query dict).
- **R2** One PyTask per request, no extra tasks for deps or middleware (delegated sends).
- **R3** No Python dict for kwargs: use vectorcall with a precomputed kwnames tuple.
- **R4** Response bytes never become a Python `bytes` object unless Python middleware reads them.
- **R5** Lane-local pools for buffers, request contexts, and serializer output vectors.
- **R6** Date header and common header names are cached and preencoded.
- **R7** Batch accepts and syscalls (`writev`). Use `TCP_NODELAY` by default, and `TCP_QUICKACK` off.
- **R8** Logging in Rust, off the request path (queue).

## 3. Scaling model
- GIL build: one Python lane per process → scale by `--workers`. Rust lanes absorb TLS and compression so lane 0's Python time is maximized.
- Free-threaded: N lanes per process with a shared app plan and connection pools. Memory per worker is lower than N processes, and DB pools are shared.
