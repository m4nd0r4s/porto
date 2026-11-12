# 50 — Repository Layout

Monorepo `porto/` (both projects; split later if needed).

```
porto/
├── Cargo.toml                     # workspace, shared lints, profiles
├── rust-toolchain.toml            # pinned latest stable, edition 2024 (ADR-031)
├── pyproject.toml                 # uv workspace root (dev tooling only)
├── crates/
│   ├── porto-core/                # scheduler, task, scope, time, sync, coop, registry
│   │   └── src/{lib.rs,task/{header.rs,harness.rs,state.rs,waker.rs},sched/{lane.rs,shared.rs,idle.rs,inbox.rs},
│   │            scope/{node.rs,cancel.rs},time/{wheel.rs,entry.rs,clock.rs},sync/{mutex.rs,rwlock.rs,semaphore.rs,
│   │            notify.rs,event.rs,barrier.rs,mpsc.rs,oneshot.rs,broadcast.rs,watch.rs},coop.rs,metrics.rs,registry.rs}
│   ├── porto-io/                  # driver trait, mio backend, uring backend, sim backend, scheduled_io.rs
│   ├── porto-net/                 # tcp.rs (done), udp.rs, unix.rs, tls/, dns/, happy_eyeballs.rs, buffer_pool.rs
│   │                              # Python-free sockets and options; the Python types and the IoOps that hold
│   │                              # Python buffers stay in porto-py (io/, api/net/) like the buffer pool
│   ├── porto-fs/ porto-process/ porto-signal/
│   ├── porto/                     # facade + compat::{tokio,hyper,futures_io}
│   ├── porto-macros/
│   ├── porto-sim/                 # seeded scheduler policy, virtual clock, sim network
│   ├── porto-abi/                 # repr(C) table + safe wrappers
│   ├── porto-py/                  # cdylib → python module porto._core
│   │   └── src/{lib.rs,driver/{pytask.rs,step.rs,trap.rs,ready.rs},api/{run.rs,scope.rs,task.rs,time.rs,sync.rs,
│   │            channel.rs,net.rs,tls.rs,fs.rs,process.rs,signals.rs,threads.rs,inspect.rs},asyncio_facade/{loop.rs,
│   │            handles.rs,transports.rs},abi_export.rs,py_compat.rs,gil_watch.rs}
│   ├── porto-pyo3/                # public helper crate for 3rd-party extensions
│   ├── porto-tokio-bridge/
│   ├── porto-console/             # bin
│   ├── douro-router/              # radix tree, normalization
│   ├── douro-http/                # hyper glue over porto-abi, conn mgmt, body, multipart, ws, sse, compression
│   ├── douro-core/                # AppPlan, HandlerPlan, DI runner, layers, responses
│   └── douro-py/                  # cdylib → douro._native (+ supervisor entry)
├── python/
│   ├── porto/                     # __init__.py, _core.pyi, asyncio.py, _anyio_backend.py, testing/, inspect.py,
│   │                              # abc.py, net.py, tls.py, fs.py, process.py, signals.py, lowlevel.py, py.typed, pytest_plugin.py
│   └── douro/                     # __init__.py, _native.pyi, app.py, routing.py, params.py, _plan.py, 
│                                  # responses.py, websockets.py, layers.py, testing.py, openapi.py, cli.py, _worker.py, py.typed
│                                  # (validation/serialization/config: dependency on the `oxiform` package)
├── packaging/
│   ├── porto/pyproject.toml       # maturin, module porto._core, python-source ../../python (porto only)
│   ├── douro/pyproject.toml       # maturin, module douro._native, depends porto==same minor
│   └── porto-console/pyproject.toml
├── tests/
│   ├── rust/ (per crate in crates/*/tests)  ├── python/porto/  ├── python/douro/
│   ├── compat/ (asyncpg, redis, httpx, aiohttp suites runner)
│   ├── typing/ (pyright strict samples)    └── conformance/ (h2spec, autobahn configs)
├── bench/
│   ├── micro/ (criterion + pyperf scripts) ├── http/ (wrk2/oha scenarios, competitor apps) └── results/
├── docs/                          # Zensical + mkdocstrings (ADR-032); guides, API (generated from stubs), cookbook
└── .github/workflows/
```
