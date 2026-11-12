# 53 — Benchmarks & Gates

## 1. Reference machine
**Current (ADR-033):** the maintainer's Apple M4 Pro Mac (10 performance + 4 efficiency cores, 48 GB, macOS, kqueue). Rules for a run that counts:
- on AC power, Low Power Mode off, no other heavy process, after a warm-up run;
- Python from `uv` (python-build-standalone, built with PGO + LTO), the same build for Porto and every competitor;
- `pyperf` with its default calibration and ≥ 20 values per benchmark; the gate is the **ratio** to the competitor measured in the same session, not the absolute number;
- multi-lane and HTTP runs use at most 8 lanes/workers so they stay on performance cores, and the load generator gets the rest;
- every result in `bench/results/` records the machine fingerprint (model, cores, OS, Python build, commit).

Linux-only paths (epoll, io_uring) are measured in the Docker Desktop Linux VM on the same Mac, as ratios only. The perf "job" is a local script, `bench/run_reference.sh`, run nightly on the Mac (launchd) or before a milestone gate; regressions > 5% versus the last committed result fail it.

**Later:** a dedicated bare-metal Linux box (16 physical cores x86-64, turbo disabled, `performance` governor, isolated CPUs or two machines over 25 GbE, Python built with `--enable-optimizations --with-lto`). When it exists, it becomes the reference and S1 is re-measured there.

## 2. Micro (pyperf / criterion)
| Bench | Gate vs asyncio+uvloop (3.14, GIL) |
|---|---|
| spawn+join 100k trivial tasks | ≥ 3× faster |
| task switch (yield ping-pong) | ≥ 2.5× |
| channel ping-pong (bounded 1) | ≥ 3× vs asyncio.Queue |
| Lock contention 100 tasks | ≥ 2× |
| Lock uncontended (`async with lock:`) | ≥ 2× |
| Mutex read+set, uncontended (`async with m as g: g.set(g.value + 1)`, vs `asyncio.Lock` + an attribute) | ≥ 1.8× ¹ |
| sleep(0.001) × 100k tasks | timers: ≥ 2× |
| TCP echo 1 KiB, 100 conns | ≥ 1.5× vs uvloop streams |
| 1M sleeping tasks | RSS < 2.5 GB; spawn ≥ 1M/s/lane |
| §11 §11 table | absolute targets |

**TCP echo (M1 exit).** One op is one 1 KiB round trip on one of 100 persistent connections; the server runs in the benchmark's process (porto `serve_tcp` + `read_exact` + `write`; asyncio and uvloop `start_server` + `readexactly` + `write` + `drain`) and the load comes from one external generator for all of them (`bench/micro/echo_client.py`: 4 uvloop client processes, timing itself), so only the server is measured and the ratio of times is the ratio of servers. Status (2026-11-12, `bench/results/2026-11-12-tcp-echo.md`): **not met**; porto is at parity with uvloop streams (0.8-1.06×), because more than 90 % of both servers' time is in `recv`/`send`/`kevent`; the gate's definition is open (porto-vw8.19).

¹ Not 2×: the interpreter's own share of the op is too large. On the reference machine (2026-11-06) asyncio+uvloop takes ~176 ns per op, so 2× would leave 88 ns; CPython's work for `async with` (two special-method lookups and calls, two `GET_AWAITABLE`/`SEND` rounds) plus the body's attribute read, method call and int add is ~60–65 ns of it, measured with samply on porto's 90 ns op. Porto's share is ~24 ns: the lock and the T2 checks at entry and exit, the guard (one object per entry), and the `StopIteration` that 3.12+ `SEND` needs to deliver the guard. The asyncio twin pays for none of these, since it reads and writes a plain attribute. Measured: 1.9× (93 ns vs 176 ns, porto-szb.118).

## 3. Free-threaded scaling (3.14t)
- Independent CPU-light tasks: throughput at 8 lanes ≥ 5.6× of 1 lane (0.7 efficiency).
- **Versus "N asyncio loops on N threads" (3.14t):** Porto 8 lanes ≥ 1.3× on the TCP echo and HTTP JSON benchmarks, and with a skewed load (one hot connection pair + many idle) ≥ 2×. This is the work-stealing gate (ST-2).

## 4. HTTP (oha/wrk2; 256 connections; h1 keep-alive; plus an h2 variant)
Scenarios: plaintext, JSON `{"message":"Hello"}`, path param + 5-field model response, POST 1 KiB JSON body validated into a model, 20-query DB (with mock driver latency 0.2 ms), SSE fan-out 10k clients, WS echo.

Competitors (same machine, 1 process, then N processes): FastAPI+Uvicorn(uvloop,httptools), FastAPI+Granian(ASGI), Litestar+Granian, raw Granian RSGI, Robyn, BlackSheep, Emmett, Go net/http (reference), Axum (ceiling reference).

Gates (per process, GIL build):
- Douro ≥ 5× FastAPI+Uvicorn on JSON, path-param, and POST-validate.
- Douro ≥ 3× FastAPI+Granian on the same.
- Douro ≥ 1.0× raw Granian RSGI on plaintext/JSON (same worker count).
- Framework tax (Douro vs porto raw HTTP handler) ≤ 20%.
- p99 latency at 70% of max RPS: ≤ 2× p50.

## 5. Publishing
Every result in `bench/results/<date>-<commit>.json` comes with machine info. Competitor app sources live in `bench/http/apps/` with exact versions, so others can reproduce. Never publish numbers without the code.
