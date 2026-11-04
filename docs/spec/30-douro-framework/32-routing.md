# 32 — Routing

## 1. Path syntax
- Static: `/users/me`
- Param: `/users/{id}` (one segment). Type from the handler annotation; an inline type is optional: `{id:int}`, `{slug:str}`, `{uid:uuid}`.
- Catch-all: `/files/{path:path}` (rest of path, may contain `/`; must be last).
- Literal braces: `{{` / `}}`.
- Trailing slash: `redirect_slashes=True` (default) answers 308 to the canonical form when only the slash differs. Canonical = as declared.

## 2. Data structure
A radix tree (compressed trie) per HTTP method, plus one "any method" tree for `app.route(methods=None)`. Each node has static children sorted by first byte, an optional param child, and an optional catch-all child. **Priority:** static > param > catch-all at every node, which is deterministic and documented.

Lookup returns `(RouteId, params as byte ranges into the decoded path)`. No allocation on the static path; params use `SmallVec<[_; 4]>`.

## 3. Conflicts (detected at freeze)
- Two routes with identical shape and method (`/a/{x}` and `/a/{y}`) → error naming both handlers and locations.
- A param and a catch-all at the same position with the same method → error.
- An inline type that contradicts the annotation → error.

## 4. 404 / 405
If the path matches in some method tree but not this one → 405 with an `Allow` header (computed at freeze per path shape). `OPTIONS` is answered automatically (204 + Allow) unless a handler is declared, and `HEAD` is auto-derived from `GET` (body discarded, length kept).

## 5. Routers & mounting
```python
users = douro.Router(prefix="/users", tags=["users"], dependencies=[Depends(auth)])
@users.get("/{id}") ...
app.include(users)                              # prefixes/tags/deps compose
app.mount("/static", douro.StaticFiles("public"))   # Rust-native, never enters Python
app.mount("/legacy", asgi_app)                  # §41
```
Router-level options (`timeout`, `on_disconnect`, `max_body_size`, `dependencies`, `middleware`, `responses`, `tags`, `include_in_schema`) are inherited and overridable per route.

## 6. Path normalization (security, §40)
Before matching:
1. Reject a raw path that doesn't start with `/` (except `*` for OPTIONS) with 400.
2. Percent-decode **per segment**. An encoded `/` (`%2F`) inside a segment is rejected with 400 by default (`allow_encoded_slash=False`).
3. Resolve `.` and `..` segments; going above the root → 400.
4. Collapse duplicate slashes → 308 redirect to the normalized path (`merge_slashes="redirect"`, configurable `"reject"|"allow"`).
5. Invalid UTF-8 after decoding → 400.

## 7. Reverse routing
`app.url_for("get_user", id=5) -> "/users/5"`. Route names default to the handler's `__qualname__`, or are set with `name=`.
