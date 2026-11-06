Runnable examples, including the sample PyO3 extension that uses the porto ABI (M3, see `docs/spec/50-engineering/54-roadmap.md`).

Each example checks its own results with `assert`, so a broken runtime makes it fail, and runs in
well under a second. `tests/python/test_examples.py` runs them all in CI. Run one with
`uv run python examples/<file>` after `uv sync`.

| File | Feature | Run |
|---|---|---|
| [`run_and_config.py`](run_and_config.py) | `porto.run`, `RuntimeConfig`, `PORTO_*` variables and their precedence, early config errors | `uv run python examples/run_and_config.py` |
| [`scopes.py`](scopes.py) | `porto.scope()`, `spawn`, `Task.result`/`done`, nested scopes, `ExceptionGroup` with `except*` | `uv run python examples/scopes.py` |
| [`cancellation.py`](cancellation.py) | A failing child stops its siblings, `Scope.cancel`, `cancel_called`/`cancelled_caught`, level-triggered `Cancelled`, cancel from another thread | `uv run python examples/cancellation.py` |
| [`errors.py`](errors.py) | `porto.errors`: the hierarchy, and what raises `WouldBlock`, `ClosedResourceError`, `EndOfChannel`, `BrokenResourceError`, `InvalidStateError`, `NoRuntimeError` | `uv run python examples/errors.py` |
| [`sleep_and_clock.py`](sleep_and_clock.py) | `sleep`, `checkpoint`, `current_time`/`monotonic` | `uv run python examples/sleep_and_clock.py` |
| [`event_notify.py`](event_notify.py) | `porto.Event` (one-shot flag) and `porto.Notify` (permits, `notify_one`/`notify_all`) | `uv run python examples/event_notify.py` |
| [`locks.py`](locks.py) | `porto.Lock` (fair, `try_acquire`) and `porto.Mutex[T]` with its guard | `uv run python examples/locks.py` |
| [`channels.py`](channels.py) | `porto.channel[T]`, `porto.unbounded_channel[T]`, backpressure, `async for`, `recv_many`, clones, close semantics | `uv run python examples/channels.py` |
| [`worker_pool.py`](worker_pool.py) | A producer, N workers and a collector joined by channels | `uv run python examples/worker_pool.py [--jobs 200] [--workers 8]` |
| [`threads.py`](threads.py) | OS threads and async code: `blocking_send`, `blocking_recv`, `Event.blocking_wait`, `Notify.blocking_notified` | `uv run python examples/threads.py` |
| [`fan_out.py`](fan_out.py) | Many tasks in one scope, against asyncio and uvloop | `uv run python examples/fan_out.py [--tasks 10000] [--backend porto\|asyncio\|uvloop]` |
| [`ping_pong.py`](ping_pong.py) | Two tasks switching on every hit, against asyncio and uvloop | `uv run python examples/ping_pong.py [--rounds 100000] [--backend porto\|asyncio\|uvloop]` |

Type-checking: `examples/` is in the strict pyright run. A name assigned inside
`async with porto.scope() as s:` and read after the block must be bound before it
(`tasks: list[porto.Task[int]] = []`): `Scope.__aexit__` returns `bool`, so pyright assumes the
block may be skipped. For the same reason, make the scope before a `try` whose handlers read it
(`s = porto.scope()`, then `async with s:`).
