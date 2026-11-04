# 35 — Middleware & Hooks

## 1. Two tiers

### 1.1 Rust layers (zero Python crossings)
Configured declaratively. They run in Rust on every request before routing (pre-route) or after the handler (post):

| Layer | Options |
|---|---|
| `douro.layers.RequestId(header="x-request-id", generate="uuid7", trust_incoming=False)` | sets `request.id`, response header, log field |
| `douro.layers.Cors(allow_origins=[...] or regex, allow_methods, allow_headers, expose_headers, allow_credentials, max_age)` | answers preflight entirely in Rust |
| `douro.layers.Compression(algorithms=("zstd","br","gzip"), min_size=1024, level=...)` | §31 §6 |
| `douro.layers.Timeout(seconds)` | default route timeout |
| `douro.layers.TrustedHosts(["api.example.com", "*.example.com"])` | 400 otherwise |
| `douro.layers.ProxyHeaders(trusted=["10.0.0.0/8"], forwarded=True)` | fixes scheme/client IP |
| `douro.layers.RateLimit(key="ip"|"header:x-api-key"|callable_rust_expr, rate="100/minute", burst=20, store="memory"|"redis://…")` | GCRA algorithm; memory store per worker, shared store via Redis (Rust client) |
| `douro.layers.JwtVerify(jwks_url=…, issuer=…, audience=…, algorithms=["RS256","ES256"], required=False)` | verifies and puts claims on `request.auth` (lazy to Python) |
| `douro.layers.SecurityHeaders(hsts=…, csp=…, frame_options="DENY", …)` | |
| `douro.layers.AccessLog(format="json"|"combined", sample=1.0)` | written by a Rust logger, batched, non-blocking |
| `douro.layers.Metrics(path="/metrics")` | Prometheus endpoint served in Rust; per-route histograms |
| `douro.layers.HealthCheck(path="/healthz", ready_path="/readyz")` | readiness flips during shutdown |

`app = App(layers=[...])`, and routers and routes accept `layers=` too (applied post-route).

### 1.2 Python middleware (one extra crossing, opt-in)
```python
@app.middleware
async def timing(request: douro.Request, call_next: douro.Next) -> douro.Response:
    t0 = porto.current_time()
    resp = await call_next(request)
    resp.headers["x-time"] = f"{porto.current_time()-t0:.4f}"
    return resp
```
- Runs in the same PyTask as the handler: `call_next` is a Rust awaitable that runs the rest of the chain and the handler inline (delegated send), so no extra task is created.
- When any Python middleware applies to a route, the response passes through Python as a `douro.Response` object (lazy body). JSON bodies stay as Rust bytes inside the object unless the middleware reads `resp.body`.

### 1.3 Hooks (cheaper than middleware)
`@app.before_request` (may return a Response to short-circuit), `@app.after_response` (runs after the response is sent; must not modify the response; good for logging/metrics), `@app.on_error`. They are per-app or per-router and compiled into the plan.

## 2. Ordering
Pre-route Rust layers (in declaration order) → router → route-level Rust layers → before_request hooks → Python middleware (outermost first) → DI → handler → … reversed for responses → after_response hooks (post-send).

## 3. Custom Rust layers (advanced)
A Rust crate can register layers through `douro-abi` (a `#[repr(C)]` `LayerVTable` exposed via the `douro._native._LAYER_ABI_V1` capsule) and be configured from Python by name. This is optional in v1; the vtable is specified in `douro-core/src/layer_abi.rs` (M8).
