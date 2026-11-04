# 53 — Benchmarks & Gates

## 1. Reference machine
Dedicated bare-metal: 16 physical cores x86-64 (current gen), turbo disabled, `performance` governor, isolated CPUs for server vs load generator (or two machines over 25 GbE). Linux LTS kernel. Python built with `--enable-optimizations --with-lto`.

## 2. Micro (pyperf / criterion)
| Bench | Gate vs asyncio+uvloop (3.14, GIL) |
|---|---|
| spawn+join 100k trivial tasks | ≥ 3× faster |
| task switch (yield ping-pong) | ≥ 2.5× |
| channel ping-pong (bounded 1) | ≥ 3× vs asyncio.Queue |
| Lock contention 100 tasks | ≥ 2× |
| sleep(0.001) × 100k tasks | timers: ≥ 2× |
| TCP echo 1 KiB, 100 conns | ≥ 1.5× vs uvloop streams |
| 1M sleeping tasks | RSS < 2.5 GB; spawn ≥ 1M/s/lane |
| §11 §11 table | absolute targets |

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
