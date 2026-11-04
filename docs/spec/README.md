# Porto & Douro — Complete Technical Specification

> **Porto** is an async runtime for Python, written in Rust. It is also a usable Rust runtime in its own right.
> **Douro** is an HTTP API framework built on Porto. *(The Douro river flows through Porto.)*
>
> Both names are **working names**. Check PyPI and crates.io for collisions before the first public release (see `50-engineering/55-risks-open-questions.md`).

Status: Draft v1.0 · Audience: implementers · Language: English

---

## 1. What this document set is

This is a complete specification. A competent Rust + CPython engineer, or a team of agents, should be able to implement both projects end to end from these files alone, without access to the conversations that produced them. When this spec leaves something open, it says so explicitly and gives a default.

The spec has two kinds of statements:

- **Normative statements** use **MUST / MUST NOT / SHOULD / MAY** (RFC 2119 meaning).
- **Informative text** is everything else: rationale, examples, and research notes.

## 2. One-paragraph pitch

Python async today means asyncio. A faster event loop (uvloop, rloop, rsloop) helps, but it hits a ceiling, because the cost lives in the Python-side task machinery and in Python-level protocol parsing, not in the loop. Porto replaces that model with four things:

1. A Rust executor that steps plain `async def` coroutines through a minimal **trap protocol**. There is no `asyncio.Task` and no `Future` object per await.
2. **Structured concurrency** by default: scopes, level-triggered cancellation, and deadlines.
3. **Lanes**: one lane per thread on GIL builds, N work-stealing lanes on free-threaded (3.14t+) builds.
4. Rust-native I/O and protocols, so bytes reach Python only once they are parsed.

Douro uses all of this. The HTTP connection, parsing, routing, extraction, validation, and serialization all happen in Rust. Python runs only the handler body, so there is exactly one boundary crossing per request.

## 3. Reading order

| # | File | Read if you are… |
|---|------|------------------|
| 1 | `00-foundations/01-vision-goals-nongoals.md` | everyone |
| 2 | `00-foundations/02-prior-art-and-research.md` | everyone |
| 3 | `00-foundations/03-stress-tests.md` | everyone; this is why the design looks the way it does |
| 4 | `00-foundations/04-decision-records.md` | everyone; the ADR index |
| 5 | `00-foundations/05-glossary.md` | as needed |
| 6 | `10-porto-runtime/10…26` | runtime implementers |
| 7 | `30-douro-framework/30…43` | framework implementers |
| 8 | `50-engineering/50…55` | everyone before writing code |

## 4. Document map

```
00-foundations/
  01-vision-goals-nongoals.md      Goals, non-goals, success criteria
  02-prior-art-and-research.md     asyncio, uvloop, rloop/rsloop, Trio, AnyIO, Tokio, Granian, ...
  03-stress-tests.md               28 thought experiments and their consequences
  04-decision-records.md           ADR-001 … ADR-030
  05-glossary.md
10-porto-runtime/
  10-architecture-overview.md      Layers, crates, threads, data flow
  11-task-model-and-trap-protocol.md   THE core: how coroutines are driven
  12-scheduler-and-lanes.md        Run queues, stealing, budget, parking, GIL handling
  13-io-driver.md                  epoll/kqueue/IOCP/io_uring, readiness model
  14-time-and-timers.md            Hierarchical timer wheel, clocks
  15-structured-concurrency-and-cancellation.md
  16-sync-primitives-and-channels.md
  17-networking-tls-dns.md
  18-fs-process-signals.md
  19-threads-and-blocking.md
  20-python-api-reference.md       Full public Python API (normative)
  21-rust-api-and-abi.md           Rust crate API + the C ABI for extensions
  22-asyncio-anyio-compat.md
  23-observability.md
  24-deterministic-testing.md
  25-cpython-integration-and-memory.md
  26-errors-config-lifecycle.md
30-douro-framework/
  30-architecture-overview.md
  31-http-server-engine.md
  32-routing.md
  33-handlers-extractors-di.md
  34-schema-validation-serialization.md
  35-middleware-and-hooks.md
  36-responses-streaming-websockets-sse.md
  37-openapi-and-docs.md
  38-app-lifecycle-config-deployment.md
  39-testing-client.md
  40-security-and-limits.md
  41-asgi-interop-and-migration.md
  42-performance-model.md
  43-python-api-reference.md
50-engineering/
  50-repository-layout.md
  51-build-ci-release.md
  52-testing-strategy.md
  53-benchmarks.md
  54-roadmap.md                    Milestones M0…M9 with exit criteria
  55-risks-open-questions.md
```

## 5. The 30-second tour (DX)

```python
import porto

async def fetch(host: str) -> bytes:
    async with await porto.net.connect_tcp(host, 443, tls=True) as s:
        await s.write(f"GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
        return await s.read_to_end(limit=1 << 20)

async def main() -> None:
    async with porto.scope(timeout=5) as s:          # nothing escapes this block
        a = s.spawn(fetch("example.com"))
        b = s.spawn(fetch("example.org"))
    print(len(a.result()), len(b.result()))

porto.run(main)
```

```python
from typing import Annotated
import douro
import oxiform as ox
from douro import App, Depends

app = App()

class NewUser(ox.Model):                 # validation/serialization by oxiform
    name: Annotated[str, ox.Len(1, 80)]
    email: ox.Email

class User(ox.Model):
    id: int
    name: str

@app.post("/users", status=201)
async def create_user(body: NewUser, db: Annotated["DB", Depends(get_db)]) -> User:
    row = await db.insert_user(body.name, body.email)
    return User(id=row.id, name=row.name)

# $ douro run app:app --workers 4
```
