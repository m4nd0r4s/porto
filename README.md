# Porto & Douro

**Porto: an async runtime for Python, written in Rust.** A Rust executor steps
plain `async def` coroutines through a small trap protocol, with structured
concurrency by default, lanes across threads, and I/O done natively in Rust.
It is also a usable Rust runtime in its own right.

**Douro: an HTTP framework built on Porto.** Connection, parsing, routing,
validation and serialization stay in Rust, so each request costs one Python
call: the handler. Validation and serialization come from
[oxiform](https://github.com/m4nd0r4s/oxiform).

The Douro river flows through Porto. Both are working names.

## Why

Python async today means asyncio. A faster event loop helps with raw
callbacks, but task-heavy and stream-heavy code gains little, because the cost
is not in the loop. It is in an `asyncio.Task` and a `Future` per await, in
callback trampolines, and in protocols parsed in Python. So Porto does not run
asyncio's model faster. It replaces it: no `Task`, no `Future` per await, and a
task can never outlive the scope that spawned it.

Every crossing between Python and Rust costs something: attaching a thread
state, creating objects, refcount traffic. That crossing is the unit of cost,
so Douro is built around one per request. The handler gets arguments that are
already validated, and its return value is serialized in Rust.

CPython 3.14 supports a build without the GIL, and asyncio there scales with
one event loop per thread. Porto aims for more: work stealing across lanes,
parallel I/O on the Rust side, and cancellation that crosses lanes safely.

## State

Pre-alpha. There is a specification and nothing you can install or run yet.

The work is spec-driven: [docs/spec/](docs/spec/README.md) is normative, and
code will follow it. The current milestone is **M0**, a spike with a kill
criterion: if spawn and switch are not at least 2× faster than
asyncio+uvloop, the design stops and is redone before anything else is built.

## The tour

This is the **target API** from the spec. None of it runs today.

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

## Roadmap

Each milestone has exit criteria; the full table is in
[54-roadmap.md](docs/spec/50-engineering/54-roadmap.md).

| M | Scope |
|---|---|
| M0 | Spike: one lane, `run`, `scope.spawn`, `sleep`. Kill criterion: ≥ 2× asyncio+uvloop on spawn/switch |
| M1 | Core runtime: scopes, timers, sync primitives, channels, TCP/UDP/Unix I/O |
| M2 | Multi-lane: work stealing, free-threaded attach policy |
| M3 | Stable C ABI for extensions, the `porto` Rust crate, hyper and tokio bridges |
| M4 | asyncio and AnyIO compatibility |
| M5 | Observability and deterministic testing |
| M6 | Douro core: router, oxiform integration, DI, HTTP/1.1 and HTTP/2 server |
| M7 | Douro complete: WebSockets, SSE, OpenAPI, middleware, CLI, ASGI mount |
| M8 | Hardening to 1.0: fuzzing, soak, wheels, API freeze |
| M9 | After 1.0: HTTP/3 and research items |

Tasks are tracked with [beads](https://github.com/gastownhall/beads): `bd ready`
lists what can start now.

## Supported Pythons

CPython 3.11 to 3.14 with the GIL, and free-threaded CPython 3.14t (ADR-024).

## Documentation

- [docs/spec/README.md](docs/spec/README.md): the specification, and the
  reading order.
- [CONTRIBUTING.md](CONTRIBUTING.md): how to work on this repository.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT](LICENSE-MIT), at your option. Unless you state otherwise, any
contribution you submit for inclusion in Porto or Douro is dual-licensed as
above, without any additional terms.
