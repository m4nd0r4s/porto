# 26 — Errors, Configuration Sources, Runtime Lifecycle

## 1. Configuration precedence
explicit `porto.run(..., **overrides)` > `RuntimeConfig` object > env vars `PORTO_*` > defaults.
Env parsing: ints, floats, bools (`1/0/true/false/yes/no`), and enums by lowercase name. Unknown `PORTO_*` vars produce a warning listing valid names (typo protection).
A field left out of `RuntimeConfig(...)` reads its `PORTO_<FIELD>` variable when the config is created, so `RuntimeConfig()` is the environment over the defaults; an explicit field never reads (or fails on) its variable. `deterministic` has no variable; `PORTO_SEED` overrides its seed (§24). Optional fields (`slow_step_threshold`, `console`) take `none` for `None`. `"auto"` lanes follow the build (`Py_GIL_DISABLED`), not whether the GIL is enabled at run time (that is GIL-degraded mode, `12` §8).

## 2. Runtime lifecycle

```
NEW ──start()──▶ RUNNING ──main returns/raises, or signal──▶ DRAINING ──▶ CANCELLING ──▶ STOPPED
```
1. **start**: detect the build (free-threaded or not), choose the backend (probe), create lanes (lane 0 = caller), install asyncgen hooks, the fork hooks, the signal receiver (if `handle_signals` and main thread), the AnyIO backend, and the import hook (free-threaded). Spawn `main` as the first task in the root scope. Each install is a start step with its own `applies` condition, run in this order and removed in reverse at step 6; a step whose owner has not landed installs nothing (the asyncgen, fork, signal and import steps are such extension points today, the AnyIO backend arrives with `22`).
   A `Runtime` is single-use: `block_on` takes it from `NEW` to `RUNNING`, and it is `STOPPED` for good when the run ends (`block_on` again raises `RuntimeClosed`). `shutdown()` of a runtime that never started or already stopped is a no-op; of one that is running it is not supported yet (steps 3-4 are the shutdown sequence's bead): return from main, or cancel the root scope. One runtime runs per process (I1): a start while another runs raises `RuntimeError`, from a task of that runtime, a blocking-pool worker or any other thread.
2. **RUNNING**: normal operation.
3. **DRAINING** (only entered via `shutdown()` or a signal while main still runs): emit the `porto.shutdown_requested` event, which libraries can await via `porto.lifecycle.shutdown_requested()` (an Event), and give main up to `graceful_timeout` to finish voluntarily.
4. **CANCELLING**: cancel the root scope (reason `Shutdown`). Wait up to `cancel_timeout` for all tasks.
5. Tasks still alive after `cancel_timeout` are **abandoned**. Their coroutines are `close()`d (GeneratorExit) on the lane. If `close()` itself awaits, the task is dropped and a warning lists their names and stacks.
6. **STOPPED**: close the asyncgens, shut down the blocking pool (waits ≤ 1 s, then detaches remaining threads), close the driver, and join the lanes.

`porto.run` returns main's result, or raises main's exception (or `KeyboardInterrupt`/`Terminated` for signals).

## 3. Error reporting
- Unhandled exceptions in tasks always propagate into their scope (C4). Nothing is ever "logged and lost".
- Exceptions crossing from Rust carry `__notes__` with the Rust error chain (`caused by: …`).
- `RustPanic` includes the panic message and location.
