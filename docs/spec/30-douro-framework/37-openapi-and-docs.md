# 37 — OpenAPI & Docs

- OpenAPI 3.1.0 document generated lazily at first request to `openapi_url` (default `/openapi.json`; `None` disables it), then cached as bytes in Rust.
- Schemas come from oxiform's bundle API (OX-5) with `ref_template="#/components/schemas/{name}"`, in validation mode for requests and serialization mode for responses.
- Sources: route method/path/name/tags/summary/description (from the docstring's first line and the rest), param schemas from plans, request body schema (content types), response schemas (`-> T`, `status`, `responses={404: Problem, 409: ConflictModel}`), security schemes (`App(security_schemes={...})` + `Security(...)` dependency markers), deprecated flags, and `include_in_schema`.
- Validation error (422) and problem responses are documented automatically.
- Docs UIs: `docs_url="/docs"` (Scalar, default), `redoc_url=None`, `swagger_url=None`. UI assets are served from a pinned CDN URL by default. `douro[docs-offline]` vendors them into the wheel (licenses: Scalar MIT, Swagger UI Apache-2.0, Redoc MIT).
- `douro openapi app:app > openapi.json` CLI export (offline, no server).
- Stable operationIds: `<router_name>_<handler_name>`, overridable with `operation_id=`.
