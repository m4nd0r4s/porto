# 38 — App Lifecycle, Configuration & Deployment

## 1. App construction
```python
app = douro.App(
    title="My API", version="1.0.0", debug=False,
    lifespan=lifespan,                 # async context manager factory: (app) -> AsyncContextManager
    layers=[...], dependencies=[...], exception_handlers={...},
    openapi_url="/openapi.json", docs_url="/docs",
    limits=douro.Limits(...),           # §40
    server=douro.ServerConfig(...),     # optional defaults for `douro run`
)

@contextlib.asynccontextmanager
async def lifespan(app: douro.App):
    app.state.pool = await create_pool(...)
    try:
        yield
    finally:
        with porto.shield(timeout=10):
            await app.state.pool.close()
```
The lifespan runs in the **app scope** (a child of the worker's root scope). The background scope (§33 §8) is a child of the app scope. App-scope dependencies resolve right after lifespan setup, in parallel where independent.

## 2. CLI (`douro`)
```
douro run app.main:app [--host 0.0.0.0] [--port 8000] [--uds PATH] [--workers N|auto] [--lanes N|auto]
                       [--tls-cert F --tls-key F] [--http auto|1|2] [--reload] [--preload]
                       [--backlog 2048] [--graceful-timeout 30] [--max-requests N --max-requests-jitter J]
                       [--reuse-port] [--log-level info] [--access-log] [--config douro.toml]
douro dev app.main:app        # = run --reload --workers 1 --debug, pretty logs
douro routes app.main:app     # print routing table with handler locations
douro check app.main:app      # freeze + lint (blocking calls in inline handlers, unshielded awaits in finally, missing annotations)
douro openapi app.main:app
douro bench app.main:app /path   # built-in load generator (Rust, h1/h2), prints latency histogram
```
`--workers auto` = physical cores on GIL builds, and 1 on free-threaded builds (where lanes do the scaling); this can be overridden.

## 3. Configuration file and precedence
CLI flags > env `DOURO_*` > `douro.toml` (or `[tool.douro]` in `pyproject.toml`) > `App(server=...)` > defaults.

Implemented with `oxiform.config`: `douro.ServerConfig` is an `@ox.config` (`env_prefix="DOURO_"`, `env_nested="__"`). Sources: `cfg.pyproject("tool.douro")`, `cfg.file("douro.toml", optional=True)`, `cfg.dotenv(".env", optional=True)`, `cfg.env()`, `cfg.mapping(cli_flags)`. `douro config explain` delegates to `cfg.origin` to show where each value came from. TLS key material and other credentials are `ox.Secret[...]`. Applications are encouraged to declare their own settings the same way (`app.settings`). Porto itself does **not** depend on oxiform: `RuntimeConfig` keeps its minimal `PORTO_*` env parsing so the runtime stays dependency-free.

## 4. Supervisor (Rust)
Process model:
```
douro (supervisor, Rust code in douro._native, runs on the CLI's main thread; never starts a Porto runtime)
 ├── binds listeners (TCP/UDS), sets SO_REUSEADDR, optional SO_REUSEPORT
 ├── spawns N workers: `python -m douro._worker --fd 3 --fd 4 --app app.main:app ...` (exec, fresh interpreter)
 │     or, with --preload: imports the app once, then fork()s workers (asserts no non-main threads exist)
 ├── monitors: respawn on crash with backoff (1s → 30s), max-requests recycling, memory limit (--max-rss)
 ├── signals: SIGTERM/SIGINT → graceful stop of all; SIGHUP → rolling reload (spawn new, then stop old one by one);
 │            SIGTTIN/SIGTTOU → +1/−1 worker
 └── health: workers report readiness over a pipe; supervisor exits non-zero if no worker becomes ready in 60 s
```
- With `--reuse-port` each worker binds its own socket (Linux kernel load balancing). Otherwise the inherited listener is shared (accept contention mitigated with `EPOLLEXCLUSIVE` on Linux).
- `--reload` (dev): file watcher (`notify` crate) on the app's package directory plus `--reload-dir`, debounced at 100 ms, restarting workers. It is mutually exclusive with `--preload`.
- Windows: no fork. Spawn mode with socket duplication via `WSADuplicateSocket`.

## 5. Logging
- Python logging integration: the `douro` and `porto` loggers. Default handler: structured JSON in production and pretty output in `dev`.
- Access logs are produced in Rust (no Python crossing) and written to stderr or a file by a dedicated Rust logging task with a bounded queue. When the queue is full, logs are dropped and counted (`douro_access_log_dropped_total`).

## 6. Embedding
`await douro.serve(app, host=..., port=...)` runs the server inside an existing Porto runtime/scope (no supervisor), which is useful in tests and multi-service processes. `douro.serve(..., started=...)` follows the `start()` protocol.
