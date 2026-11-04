# 34 — Validation & Serialization: oxiform

> **Decision (ADR-019, revised):** Douro has **no schema engine of its own**. Validation, serialization, JSON Schema, and configuration are done by **oxiform** (`import oxiform as ox`), a sibling project: a Rust engine with a Python API that validates and (de)serializes JSON, YAML, TOML, and MessagePack in a single pass. The old `douro-schema` crate is removed from the plan.
>
> Normative source for everything about types, constraints, coercion, unions, errors, limits, and encoding: **the oxiform specification (`docs/spec.md` in the oxiform repo)**. This file specifies only **how Douro uses oxiform**, and the oxiform capabilities Douro depends on, labeled `OX-n` (§6).

## 1. Why oxiform
oxiform's design already is what this section used to specify:
- single-pass parse+validate+construct;
- perfect-hash field lookup;
- instances built by filling slots without `__init__`;
- immutable plans shared across free-threaded threads;
- limits on by default;
- RFC 9457 errors with JSON Pointers;
- JSON Schema 2020-12 in validation/serialization modes.

It adds multi-format content negotiation (msgpack/YAML) and layered config for free. Building a second engine would duplicate this work and split the ecosystem.

## 2. What Douro uses

| Douro need | oxiform feature | Notes |
|---|---|---|
| Request body → typed object | `ox.json.decode(bytes, T)` (and `ox.msgpack`, `ox.yaml` by `Content-Type`) via **OX-1** from Rust | `mode="strict"` |
| Path / query / header / cookie / form params | **OX-3** pair-source decoding, `mode="lax"` | one combined 422 via **OX-4** |
| Response serialization | `encode` via **OX-1**/**OX-6** into the response buffer, **against the declared return type** (**OX-2**) | |
| Untyped responses (`dict`, `list`) | `ox.json.encode` (orjson-class target) | |
| PATCH bodies | `ox.Partial[T]` | |
| Validation context | `ox.Context` (`context={"request": request}`) | |
| OpenAPI schemas | **OX-5** bundle API | |
| Large streaming arrays (`douro.JSONStream[T]`) | **OX-7** push decoding | until OX-7 lands, `JSONStream` buffers the body (limits apply) |
| 422 rejection without touching Python, plus GIL relief on GIL builds | **OX-8** tape (validate on a Rust lane, materialize on the Python lane) | optional, perf |
| `douro.toml`, `DOURO_*`, `[tool.douro]`, secrets | `oxiform.config` (`@ox.config`, `cfg.load`, `cfg.origin`, `ox.Secret`) | §38 |
| Content negotiation | `Accept: application/msgpack` / `application/yaml` → matching `ox.<format>.encode` | free feature |

Models: users write `@ox.model`, `ox.Model`, dataclasses, attrs, TypedDict, or NamedTuple. **`douro.Model` is a re-export of `ox.Model`**, and constraints (`Ge`, `Len`, `Pattern`, …) and `Email`/`Url` are re-exported from oxiform. Pydantic models are accepted through Douro's own adapter (`__pydantic_validator__.validate_json`) until oxiform's pydantic shim (oxiform 1.2) exists.

## 3. Conventions Douro adopts from oxiform (overrides earlier text)
- **Error shape:** oxiform's (`code`, `path` as wire names, `pointer`, `message`, `input`, `ctx`). Douro's 422 body = `ValidationError.to_problem()` with sources merged (OX-4). Codes are oxiform's stable codes.
- **Datetimes:** `datetime` requires a timezone; `ox.NaiveDateTime` for naive values.
- **Decimal:** written as a JSON string by default (`ox.Policy(decimal="number")` per app if wanted).
- **Unknown keys:** oxiform default `unknown="ignore"` for models. Douro exposes `App(unknown="forbid")` to tighten request bodies globally (needs OX-9).
- **Limits:** oxiform `ox.Limits` apply to body decoding, and Douro's HTTP limits (§40) apply before decoding. Douro passes `max_bytes = route.max_body_size`.
- **Coercion:** body = `strict`. Path, query, header, cookie, and form sources = `lax`.

## 4. Freeze-time integration
At `app.freeze()`:
1. For each handler, Douro collects types per source (body type, per-param scalar types, return type) and dependency types.
2. Douro requests oxiform plan handles through **OX-1** (`plan_for(type)`) and stores them in `HandlerPlan`. For query/header/cookie params, Douro synthesizes one internal "params model" per handler and source (Python names, wire names = aliases), so each source is **one** decode call (OX-3), not N scalar calls.
3. Declaration errors (`ox.SchemaError`) surface at freeze with the handler location prepended.
4. `ox.warmup(*types)` is called so forward refs resolve before serving.

## 5. Request-time flow (Rust side)
```
body bytes ──(OX-1 decode, plan=body_plan, mode=strict, limits, context)──▶ PyObject | Issues
query pairs ─(OX-3 decode_pairs, plan=query_model)──▶ PyObject | Issues
headers/cookies ─(OX-3 …)──▶ …
all Issues ──(OX-4 combine with source prefixes "body"/"query"/"path"/"header"/"cookie")──▶ one 422 problem
handler return ──(OX-2/OX-6 encode_into(buf, obj, declared_type))──▶ response bytes
```
All calls happen on the Python lane (oxiform needs the interpreter attached; oxiform D15), except the optional OX-8 validate phase.

## 6. Capabilities Douro needs from oxiform

| ID | Capability | Priority | Status in oxiform (2026-10-31) |
|---|---|---|---|
| OX-1 | **Embedding C ABI** (PyCapsule): plan handles, decode from a Rust buffer, encode into a Rust buffer, structured issues, all usable from another cdylib | P0 | missing |
| OX-2 | **Encode against a declared type** (`encode(obj, type=T)`): fields outside `T` are never written | P0 | spec walks the runtime type, so it is missing |
| OX-3 | **Pair-source decoding** (query string, `application/x-www-form-urlencoded`, header/cookie pairs) in `lax` mode, repeated keys → sequences | P0 | `lax` exists; no pair/urlencoded reader |
| OX-4 | **Error path prefixes and combining reports** from several sources into one `ValidationError` / problem | P0 | missing |
| OX-5 | **JSON Schema bundle**: many types → shared `$defs` with a configurable `ref_template`, stable names, input/output variants | P0 (for OpenAPI) | single-type `ox.json_schema` only |
| OX-6 | `encode_into` for a Rust-owned growable buffer, plus a size hint | P1 | Python `encode_into(obj, buf)` planned |
| OX-7 | Push/incremental decoding over chunks (non-contiguous input; streaming top-level arrays) | P1 | reader is pull over a contiguous `&str` |
| OX-8 | Single-document **tape** (validate without the interpreter, materialize later), exposed via OX-1 | P1 | tape planned for `decode_many` only |
| OX-9 | Per-call `unknown=` override | P2 | model option only |
| OX-10 | Key-string cache for dict keys in the Python sink | P2 (perf) | each key allocates a new `str` |

The full rationale and the ready-to-implement request are maintained in the oxiform repository. Douro's M6 depends on OX-1…OX-5 (`54-roadmap.md`).
