Runnable examples, including the sample PyO3 extension that uses the porto ABI (M3, see `docs/spec/50-engineering/54-roadmap.md`).

Each example checks its own results with `assert`, so a broken runtime makes it fail, and runs in
well under a second. `tests/python/test_examples.py` runs them all in CI. Run one with
`uv run python examples/<file>` after `uv sync`.

| File | Feature | Run |
|---|---|---|
| [`run_and_config.py`](run_and_config.py) | `porto.run` and what it raises, `RuntimeConfig`, `PORTO_*` variables and their precedence (`cancel_timeout`), unknown-variable warnings, early config errors | `uv run python examples/run_and_config.py` |
| [`scopes.py`](scopes.py) | `porto.scope()`, `spawn`, `Task.result`/`done`, nested scopes, `ExceptionGroup` with `except*`, a body exception cancels its scope | `uv run python examples/scopes.py` |
| [`tasks.py`](tasks.py) | `porto.Task` (await it, `result`/`exception`/`done`/`cancelled`, `name`, `id`), `current_task()`, `Task.cancel()`, `Scope.start()` with `Started[T]`, `porto.lowlevel` | `uv run python examples/tasks.py` |
| [`cancellation.py`](cancellation.py) | A failing child stops its siblings, `Scope.cancel`, `cancel_called`/`cancelled_caught`, level-triggered `Cancelled`, `Task.cancel`, a deadline as a cancel, a shielded section, cancel from another thread | `uv run python examples/cancellation.py` |
| [`deadlines_and_shields.py`](deadlines_and_shields.py) | `porto.timeout`, `porto.move_on_after`, `Scope.deadline`, nested deadlines, `porto.shield` (rollback in `finally`), sync `with` cancel scopes | `uv run python examples/deadlines_and_shields.py` |
| [`errors.py`](errors.py) | `porto.errors`: the hierarchy, and what raises `WouldBlock`, `ClosedResourceError`, `EndOfChannel`, `BrokenResourceError`, `Lagged`, `InvalidStateError`, `Cancelled`, `NoRuntimeError` | `uv run python examples/errors.py` |
| [`sleep_and_clock.py`](sleep_and_clock.py) | `sleep`, `sleep_until`, `sleep_forever`, `checkpoint` and `sleep(0)`, `current_time`/`monotonic` | `uv run python examples/sleep_and_clock.py` |
| [`event_notify.py`](event_notify.py) | `porto.Event` (one-shot flag) and `porto.Notify` (permits, `notify_one`/`notify_all`) | `uv run python examples/event_notify.py` |
| [`locks.py`](locks.py) | `porto.Lock` (fair, `try_acquire`) and `porto.Mutex[T]` with its guard | `uv run python examples/locks.py` |
| [`semaphores.py`](semaphores.py) | `porto.Semaphore` and `porto.CapacityLimiter`: limiting a fan-out, several permits, `*_on_behalf_of`, resizing | `uv run python examples/semaphores.py` |
| [`coordination.py`](coordination.py) | `porto.RwLock` (readers, a writer, writer preference), `porto.Condition` (`wait_for`), `porto.Barrier` (rounds, the leader) | `uv run python examples/coordination.py` |
| [`channels.py`](channels.py) | `porto.channel[T]`, `porto.unbounded_channel[T]`, backpressure, `async for`, `recv_many`, clones, close semantics | `uv run python examples/channels.py` |
| [`more_channels.py`](more_channels.py) | `porto.oneshot` (request/reply), `porto.watch` (latest value, `changed()`, a clearable event), `porto.broadcast` (fan-out, `porto.Lagged`, `subscribe()`) | `uv run python examples/more_channels.py` |
| [`worker_pool.py`](worker_pool.py) | A producer, N workers and a collector joined by channels | `uv run python examples/worker_pool.py [--jobs 200] [--workers 8]` |
| [`threads.py`](threads.py) | OS threads and async code: `blocking_send`, `blocking_recv`, `Event.blocking_wait`, `Notify.blocking_notified`, and the `blocking_*` of oneshot, watch and `Semaphore` | `uv run python examples/threads.py` |
| [`blocking.py`](blocking.py) | `porto.to_thread`: blocking code on the elastic thread pool (`blocking_threads`, `blocking_keepalive`), results and exceptions with the worker's traceback, a copied context, a shielded await by default and `cancellable=True` | `uv run python examples/blocking.py` |
| [`shutdown_and_signals.py`](shutdown_and_signals.py) | `Runtime.shutdown`, `porto.lifecycle.shutdown_requested()`, the graceful and cancel waits, abandoned tasks, `porto.signals.receive` (Unix) | `uv run python examples/shutdown_and_signals.py` |
| [`buffers.py`](buffers.py) | `porto.Buffer`: `memoryview`, `bytes(buf)`, `resize`/`extend`, `BufferError` while a view is held | `uv run python examples/buffers.py` |
| [`tcp_echo.py`](tcp_echo.py) | `porto.net.listen_tcp`, `serve_tcp` (`Scope.start`, a scope per connection), `TcpListener` (`port`, `async for`, `aclose`), `TcpStream` (`read_buffer`, `readline`, `read_exact`, `write`, `writev`, `shutdown_write`, `async with`), `IncompleteReadError`, `ClosedResourceError` on a closed listener | `uv run python examples/tcp_echo.py` |
| [`fan_out.py`](fan_out.py) | Many tasks in one scope, against asyncio and uvloop | `uv run python examples/fan_out.py [--tasks 10000] [--backend porto\|asyncio\|uvloop]` |
| [`ping_pong.py`](ping_pong.py) | Two tasks switching on every hit, against asyncio and uvloop | `uv run python examples/ping_pong.py [--rounds 100000] [--backend porto\|asyncio\|uvloop]` |

Type-checking: `examples/` is in the strict pyright run. A name assigned inside
`async with porto.scope() as s:` and read after the block must be bound before it
(`tasks: list[porto.Task[int]] = []`): `Scope.__aexit__` returns `bool`, so pyright assumes the
block may be skipped. For the same reason, make the scope before a `try` whose handlers read it
(`s = porto.scope()`, then `async with s:`).
