# 41 — ASGI Interop & Migration

## 1. Mounting ASGI apps
`app.mount("/legacy", asgi_app)`: requests under the prefix are converted into the ASGI 3.0 HTTP/WebSocket scope (`scope["root_path"]` set). `receive`/`send` are implemented as Rust awaitables, so they are fast. The app runs as a **real asyncio Task on the lane's facade loop** (§22), so Starlette/FastAPI internals (AnyIO, `asyncio.current_task`) behave exactly as under Uvicorn. Lifespan events for mounted apps are sent during Douro's lifespan.

Performance expectation: comparable to FastAPI on Granian ASGI. Mounting is a migration bridge, not the fast path.

## 2. Exposing Douro as ASGI
Not provided in v1, because it would lose the architecture's benefits. Use `douro run`.

## 3. FastAPI → Douro mapping (docs page + codemod in M9)
| FastAPI | Douro |
|---|---|
| `FastAPI()` | `douro.App()` |
| `APIRouter(prefix=…)` | `douro.Router(prefix=…)` |
| `Depends`, `Query`, `Path`, `Header`, `Body`, `Form`, `File` | same names in `douro` |
| `pydantic.BaseModel` | works via adapter; `ox.Model` / `@ox.model` (oxiform) for max speed |
| `HTTPException` | `douro.HTTPError` (problem+json; `detail` maps) |
| `BackgroundTasks` | `douro.Background` (structured) |
| `@app.middleware("http")` | `@app.middleware` |
| `Request`, `Response`, `JSONResponse`, `StreamingResponse` | `Request`, `Response`, `JSON`, `Stream` |
| `lifespan=` | same |
| `TestClient` | `douro.testing.Client(app).sync()` |
