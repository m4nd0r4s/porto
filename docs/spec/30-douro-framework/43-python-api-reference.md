# 43 — Douro Python API Reference (normative)

```python
# douro/__init__.py public names
App, Router, Request, Response, WebSocket, WebSocketDisconnect, Headers, QueryParams, Cookies,
Path, Query, Header, Cookie, Body, Form, File, UploadFile, Depends, Security, State,
Background, BodyStream, JSONStream, Next,
Model, Email, Url,                 # re-exports of ox.Model, ox.Email, ox.Url (oxiform)
Ge, Gt, Le, Lt, Len, Pattern, MultipleOf,   # re-exports of oxiform constraints
HTTPError, Problem, JSON, Text, HTML, Redirect, NoContent, File as FileResponse, Stream, EventStream, Event,
CancelAfter, Limits, ServerConfig, serve, layers, testing, openapi
```

### App / Router
```python
class App(Router):
    def __init__(self, *, title="Douro", version="0.1.0", description="", debug=False,
                 lifespan=None, layers=(), dependencies=(), exception_handlers=None,
                 openapi_url="/openapi.json", docs_url="/docs", redoc_url=None, swagger_url=None,
                 security_schemes=None, limits: Limits | None = None, server: ServerConfig | None = None,
                 redirect_slashes=True, merge_slashes="redirect") -> None
    state: SimpleNamespace
    dependency_overrides: dict[Callable, Callable]
    def mount(self, prefix: str, app: Any, *, name: str | None = None) -> None
    def exception_handler(self, exc_type: type[BaseException]) -> Callable[[F], F]
    def middleware(self, fn: F) -> F
    def before_request(self, fn: F) -> F;  def after_response(self, fn: F) -> F;  def on_error(self, fn: F) -> F
    def url_for(self, name: str, /, **params: Any) -> str
    def freeze(self) -> None
    def openapi(self) -> dict[str, Any]

class Router:
    def __init__(self, *, prefix="", tags=(), dependencies=(), layers=(), responses=None,
                 timeout=None, on_disconnect="cancel", max_body_size=None, include_in_schema=True) -> None
    def get/post/put/patch/delete/head/options(self, path: str, **route_opts) -> Callable[[F], F]
    def route(self, path: str, *, methods: Sequence[str] | None, **route_opts) -> Callable[[F], F]
    def ws(self, path: str, **opts) -> Callable[[F], F]
    def include(self, router: Router, *, prefix: str = "", **overrides) -> None
```

### Request (lazy)
`method`, `url` (`douro.URL`), `path`, `path_params`, `query` (`QueryParams` multi-dict), `headers` (`Headers`), `cookies`, `client` (`(host, port)`), `scheme`, `http_version`, `id`, `state`, `auth` (from `JwtVerify`), `scope` (`porto.Scope`), `async body() -> bytes`, `async json(type: type[T] = Any) -> T`, `async form() -> FormData`, `stream() -> AsyncIterator[bytes]`, `is_disconnected() -> bool`.

### Errors
`HTTPError(status: int, detail: str | None = None, *, title=None, type_=None, headers=None, **extensions)`.

### Server
`ServerConfig(host="127.0.0.1", port=8000, uds=None, workers=1, lanes="auto", http="auto", tls_cert=None, tls_key=None, backlog=2048, graceful_timeout=30.0, reuse_port=False, access_log=False)`.
`async serve(app, *, config: ServerConfig | None = None, started: porto.Started[int] | None = None, **overrides) -> None`.

All of the above are fully typed. `douro` ships `py.typed`.
