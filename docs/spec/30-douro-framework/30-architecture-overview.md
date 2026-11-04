# 30 — Douro Architecture Overview

## 1. Principle: one crossing per request
On the common path (route matched, typed params, JSON body, JSON response), Python is entered **once**: the call to the handler coroutine, stepped by Porto until it completes. Everything else is Rust:

```
socket ─▶ [Rust] accept, TLS, HTTP/1.1|2 parse (hyper)
       ─▶ [Rust] Rust middleware layers (limits, CORS, compression decision, request-id, timeouts, auth-verify)
       ─▶ [Rust] router (radix) → RouteId + raw params
       ─▶ [Rust] HandlerPlan.extract(): path/query/header/cookie parse+validate → Python objects;
                 body: fused JSON parse+validate → Python model instance
       ─▶ [Rust] DI plan: resolve cached app-scope deps; request-scope deps that are Rust-native (e.g., Request)
       ─▶ [Py]   handler(**kwargs)  (+ Python deps / Python middleware if declared — extra crossings, opt-in)
       ─▶ [Rust] response: serialize return value via compiled serializer → bytes; headers; compression
       ─▶ [Rust] write, keep-alive / h2 stream end
```

## 2. Components

| Component | Crate / module | Spec |
|---|---|---|
| HTTP engine (hyper on Porto ABI I/O) | `douro-http` | §31 |
| Router | `douro-router` | §32 |
| Handler plans, extractors, DI | `douro-core` + `python/douro/_plan.py` (startup only) | §33 |
| Validation + serialization + config | **oxiform** (external dependency, via its C ABI) | §34 |
| Middleware (Rust layers + Python middleware) | `douro-core`, `douro.middleware` | §35 |
| Responses, streaming, WS, SSE | `douro-core`, `douro.responses` | §36 |
| OpenAPI | oxiform JSON Schema bundle (OX-5) + `douro.openapi` | §37 |
| Lifecycle, config, CLI, supervisor | `douro-cli` (Rust bin) + `douro.cli` | §38 |
| Test client | `douro.testing` | §39 |

Native module: `douro._native` (cdylib). It links `porto-abi`, **not** `porto-core` (ADR-004), and `oxiform-abi`, **not** oxiform's engine. All I/O and tasks go through Porto's ABI, and all validation and serialization through oxiform's (same single-owner rule as ST-8).

## 3. Startup phases
1. **Declare:** decorators register `RouteDecl(method, path, fn, options)` objects in Python. No work is done yet.
2. **Freeze** (on `app.freeze()`, implicitly at first serve or test-client creation):
   1. resolve routers and prefixes;
   2. introspect every handler signature (`typing.get_type_hints(include_extras=True)`, `inspect.signature`);
   3. build `HandlerPlan`s (§33), obtain oxiform plan handles (§34 §4), build the DI graph and detect cycles;
   4. build the router and check conflicts;
   5. generate OpenAPI (lazily, on first request to `/openapi.json`);
   6. pass everything to Rust as an immutable `AppPlan`.
   After freeze, adding routes raises `RuntimeError`.
3. **Serve:** the Rust server owns the plan (`Arc<AppPlan>`) shared by all lanes. There is no per-request Python introspection.

## 4. Concurrency model
- Each connection is a **Rust task** spawned via the ABI into the server scope. Each request is a child scope. The handler is a **PyTask** spawned into the request scope via `py_spawn`.
- GIL build: connection tasks may run on any lane (with `lanes > 1`, Rust lanes do the HTTP/TLS work). Handler PyTasks run on lane 0. Default per worker process: `lanes=2` on GIL builds (one Python lane, one Rust lane). Scale through `--workers` processes.
- Free-threaded build: `lanes="auto"` (ADR-025). Connections and handlers run on any lane, and connection tasks stay lane-affine with their handler (soft affinity).

## 5. What Douro deliberately is not
Not ASGI-based internally (ASGI's message-dict protocol costs ~30–40%, see `02` §7), but it can mount ASGI apps (§41). Not a full-stack web framework: templates and sessions are optional extras.
