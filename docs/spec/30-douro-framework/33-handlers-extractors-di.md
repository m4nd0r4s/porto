# 33 — Handlers, Extractors & Dependency Injection

## 1. Declaring handlers
```python
@app.get(path, *, name=None, status=200, response_model=..., tags=(), summary=None, description=None,
         timeout=None, on_disconnect="cancel", max_body_size=None, inline=False,
         dependencies=(), responses={}, include_in_schema=True, deprecated=False)
```
The same keywords apply to `post`, `put`, `patch`, `delete`, `head`, `options`, `route(methods=[...])`, and `ws` (§36).

Handlers may be `async def` (run as PyTask on a Python lane) or `def`. Sync handlers run on the blocking pool unless `inline=True` (ADR-020), and `douro check` warns when an `inline=True` handler calls known blocking APIs.

## 2. Parameter source inference (in order)
For each handler parameter `p: T`:
1. `Annotated[T, Marker]` with an explicit marker → that source: `Path()`, `Query()`, `Header()`, `Cookie()`, `Body()`, `Form()`, `File()`, `Depends(fn)`, `State("key")`.
2. `T` is a special type → inject: `douro.Request`, `douro.Response` (mutable header/cookie sink), `douro.WebSocket`, `porto.Scope` (the request scope), `douro.BodyStream`, `douro.App`, `douro.Background` (app-owned spawn handle, §8).
3. The name matches a path parameter → Path.
4. `T` is a "structured" type (`@ox.model`/`ox.Model`, dataclass, attrs, TypedDict, NamedTuple, pydantic model, `list[...]` of those, `dict[...]`) → Body (decoded by oxiform; format from `Content-Type`). At most one inferred body param. With several structured params, the body is an object keyed by parameter names (`embed` mode, as in FastAPI).
5. Otherwise → Query.

Unannotated parameters are an error at freeze: "parameter 'x' of get_user has no type annotation".

## 3. Scalar coercion rules (path/query/header/cookie)
Strings are converted per target type: `int` (decimal, optional sign, no underscores), `float`, `bool` (`true/false/1/0/yes/no/on/off`, case-insensitive), `str`, `UUID`, `datetime`/`date`/`time` (ISO 8601), `Decimal`, `Enum` (by value), `Literal[...]`, `Optional[T]` (missing → None), and `list[T]`/`tuple[T, ...]` (repeated keys `?a=1&a=2`; `Query(explode=False)` for comma-separated). Constraints from `Annotated` (any oxiform constraint: `Ge`, `Gt`, `Le`, `Lt`, `Len`, `Pattern`, `MultipleOf`, …) are applied; the exact coercion table is oxiform's `lax` mode. Headers: the parameter name `x_request_id` maps to the header `x-request-id` (`Header(alias=...)` overrides). Query aliases use `Query(alias="page-size")`.

All of this runs in Rust through oxiform's `lax` mode (one pair-source decode per source, §34 §4–5). Errors accumulate (all params are checked) and produce one 422 with every error, in oxiform's error shape with source-prefixed paths (OX-4).

## 4. HandlerPlan (Rust)
```rust
struct HandlerPlan {
    route_id: RouteId,
    callable: PyObj,                       // the handler function
    kind: HandlerKind,                     // Async | SyncPool | SyncInline
    params: Vec<ParamPlan>,                // in call order → builds a vectorcall argument array
    body: Option<BodyPlan>,                // content-types, validator id, limit, embed keys
    deps: DepGraphSlice,                   // dependency nodes needed (topologically sorted)
    needs_request_object: bool,
    response: ResponsePlan,                // serializer id, default status, headers, model filtering
    policies: RoutePolicies,               // timeout, on_disconnect, limits, middleware chain ids
}
enum ParamPlan {
    Path { index: u8, validator: ScalarValidatorId },
    Query { key: Atom, validator: ValidatorId, required: bool, default: Option<PyObj> },
    Header { name: HeaderName, ... }, Cookie { ... },
    Body, Dep(DepId), Request, Scope, ResponseSink, Background, State(Atom),
}
```
The handler is called with **vectorcall** (`PyObject_Vectorcall`) and keyword names as a precomputed tuple. No dict is built.

## 5. Dependency injection
```python
async def get_db(app: douro.App) -> AsyncIterator[DB]:     # generator dep = setup/teardown
    async with app.state.pool.acquire() as conn:
        yield DB(conn)

def settings() -> Settings: ...                             # sync dep

@app.get("/items")
async def items(db: Annotated[DB, Depends(get_db)],
                s: Annotated[Settings, Depends(settings, scope="app")]) -> list[Item]: ...
```
- Dependencies are functions whose parameters are themselves resolved by the same inference rules (recursive), so they can take Query params, other deps, `Request`, and so on.
- **Scopes:**
  - `"request"` (default): evaluated once per request (deduplicated across the graph).
  - `"app"`: evaluated once, at startup (eagerly, in the lifespan), and cached.
  - `"transient"`: evaluated per use.
- **Generator deps** (sync or async): the code before `yield` runs before the handler. The teardown runs after the response is **fully sent**, in reverse topological order, inside a shielded child scope with a 10 s deadline. Exceptions raised by the handler are thrown into the generator (as in FastAPI ≥ 0.106 semantics: teardown sees the exception).
- **Plan compilation:** the DI graph is topologically sorted at freeze. Cycles produce an error with the cycle path. App-scope deps are resolved at startup and stored as PyObjs in the plan. Request-scope deps that are pure Rust-known values (`Request`, `Scope`, path params) never enter Python.
- **Crossing cost:** each Python dependency adds one Python call within the same PyTask. Calls happen inside the handler task's single step loop (the dep coroutines are awaited by a Rust-implemented "plan runner" coroutine, so there are no extra tasks). Async deps are driven as sub-coroutines of the runner by delegating sends (§11 trap protocol), so they cost no extra scheduler round trips.
- **Overrides** (testing): `app.dependency_overrides[get_db] = fake_db`. This re-freezes the affected plans (only allowed when the app is not serving, or in the test client).

## 6. Return values → responses (summary; detail in §36)
- `None` → 204 if `status` was not set explicitly, else the status with an empty body.
- A model, dataclass, TypedDict, dict, list, or primitive → JSON via the compiled serializer for the declared return annotation (`-> User`). Unannotated returns use the generic encoder.
- `douro.Response` subclasses → as-is.
- The `response_model=` override, plus `response_model_exclude_none` and friends, are compiled into the serializer.
- Return annotation filtering: if the handler returns a subclass instance with extra fields, only the declared fields are serialized (security: prevents leaking fields).

## 7. Request state
`request.state` is a per-request `SimpleNamespace`-like object. `app.state` is app-wide (lifespan). `State("key")` injects `app.state.key`.

## 8. Disconnect policy and background work
- `on_disconnect="cancel"` (default): cancel the request scope.
- `"ignore"`: the handler keeps running, and its response is discarded.
- `douro.CancelAfter(seconds)`: grace period, then cancel.

Background work after the response:
```python
@app.post("/signup")
async def signup(body: NewUser, bg: douro.Background) -> User:
    user = await create(body)
    bg.spawn(send_welcome_email(user))   # owned by the app's background scope, survives the request
    return user
```
`Background.spawn` spawns into the **app background scope**: structured, cancelled at shutdown after `graceful_timeout`, and errors logged and counted, but they never crash the app. This is the only sanctioned way to outlive a request.

## 9. Exceptions → responses
- `raise douro.HTTPError(404, detail="user not found", headers=..., type_="https://…/not-found")` → problem+json.
- `@app.exception_handler(MyError) async def h(req, exc) -> Response`. Matching follows the MRO, and single-exception groups from the handler's own scope are unwrapped first (ST-10).
- Unhandled → 500 problem (with the traceback in the body only when `debug=True`) and a logged error with the request id.
