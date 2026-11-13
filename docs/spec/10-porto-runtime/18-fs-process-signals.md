# 18 — Filesystem, Processes, Signals

## 1. Filesystem (`porto.fs`)
Regular files are not pollable. Implementation: blocking pool (default), or io_uring ops when that backend is active.

```python
async with await porto.fs.open(path, "rb") as f:     # modes as builtins.open, text modes supported (encoding, newline)
    data = await f.read()                            # read(n=-1), readline, readinto, write, seek, tell, truncate, flush
await porto.fs.read_bytes(path); await porto.fs.read_text(path, encoding="utf-8")
await porto.fs.write_bytes(path, data, *, atomic=False)   # atomic: write tmp + fsync + rename
await porto.fs.stat(path); exists; mkdir(parents, exist_ok); remove; rename; listdir; walk (async iterator); copy
p = porto.fs.Path("...")   # async mirror of pathlib.Path for I/O methods; pure methods delegate to pathlib
```
Batching: `read_bytes` performs open+fstat+read+close as **one** blocking-pool job (one thread hop).

## 2. Processes (`porto.process`)
```python
proc = await porto.process.spawn(["git","status"], stdin=PIPE|DEVNULL|None, stdout=PIPE, stderr=PIPE,
                                 cwd=None, env=None, start_new_session=False, kill_on_cancel=True)
out = await proc.stdout.read_to_end(limit=...)       # ByteStream
code = await proc.wait()
proc.send_signal(sig); proc.terminate(); proc.kill(); proc.pid; proc.returncode
res = await porto.process.run(args, *, input=None, capture=True, check=False, timeout_via_scope=True) -> CompletedProcess
```
- Linux ≥ 5.3: `pidfd_open` + readiness for exit. Otherwise a SIGCHLD self-pipe and `waitpid(pid, WNOHANG)` reaping, scoped to our children only.
- `kill_on_cancel=True`: if the waiting scope is cancelled, send SIGKILL (POSIX) or TerminateProcess (Windows) and reap in the background (shielded).

## 3. Signals (`porto.signals`)
```python
async with porto.signals.receive(signal.SIGHUP, signal.SIGUSR1) as sigs:
    async for signum in sigs: ...
```
- Implementation: `signal-hook`-style self-pipe registered with the driver (`porto_io::signal`, Unix; no `signalfd`). Python-level handlers are **not** installed. Delivery goes to the receiver regardless of which thread the OS chose.
  - The handler installed for a signal does one async-signal-safe thing: `write` the signal number (one byte) to the pipe, saving and restoring `errno`. A task of the run reads the pipe and calls the receivers on the lane. The pipe byte is only a wakeup: the handler also raises a per-signal pending count that the reader takes, so a full pipe cannot lose a signal, and signals that arrive close together are delivered once (the kernel coalesces a pending signal the same way).
  - The first receiver of a signal replaces its disposition (Python's handler included) and remembers it; the last one leaving puts it back, and the end of the run puts back every one left (a receiver in an abandoned task). Several receivers of a signal all get it.
  - `receive(*signums)` needs at least one catchable signal (`SIGKILL`/`SIGSTOP` raise `ValueError`), and a running runtime on this thread's lane (`NoRuntimeError`).
  - **Windows** has no self-pipe yet: `porto.signals.receive` raises `NotImplementedError` there, and `handle_signals` has no effect (Python's own `KeyboardInterrupt` for Ctrl-C stays). A `SIGINT`/`SIGBREAK` console-control-handler receiver is a later bead.
- **Default runtime behavior** (`handle_signals=True`, on the main thread; the signal start step of 26 §2 installs it with the receiver above and removes it when the run ends): a signal asks the runtime to shut down (26 §2 steps 3-6), and `porto.run` raises the signal's exception after the shutdown, unless main failed with an exception of its own.
  - SIGINT → the shutdown starts with **no graceful period**: `shutdown_requested` is set and the root scope is cancelled (reason `Shutdown`) at once, so Ctrl-C stops a program as it does today; `porto.run` then raises `KeyboardInterrupt`. (The bead's "reason KeyboardInterrupt" is the exception `porto.run` raises, not a cancel reason: cancel reasons are not visible to Python.)
  - SIGTERM → main gets `graceful_timeout` to finish by itself (ST-20: a server stops accepting and drains), then the root scope is cancelled; `porto.run` raises `porto.Terminated(SystemExit)` with exit status 143 (128 + SIGTERM), also when main finished within the graceful period.
  - A **second SIGINT** at any time after the first request forces the shutdown: the root scope is cancelled and the tasks still alive are abandoned (26 §2 step 5) without waiting for `cancel_timeout`, so a shielded task that hangs does not hold the process. (The spec's "within the graceful period" is read as: after the first request, until the run stops.) Any other later signal is ignored.
  - A lane stuck in synchronous code (a busy loop, `time.sleep`) cannot read the pipe, so the first Ctrl-C waits for it. The SIGINT handler therefore **chains**: from the second SIGINT on (counted since the signal was taken over, whether or not the lane acted on the first, so also while main runs synchronous cleanup after the first Ctrl-C), the handler also calls the handler it replaced, Python's C handler, which trips the eval breaker and raises `KeyboardInterrupt` in that code. The handlers are installed without `SA_RESTART`, as Python's are, so a blocking syscall of the lane (`recv`, `read`, a lock or `wait4`) returns `EINTR` and the chained handler's `KeyboardInterrupt` is raised by the code that made it; the driver's poll treats `EINTR` as a wakeup. The second Ctrl-C always works, as with `handle_signals=False` (and as in `asyncio.Runner`, where the first Ctrl-C only cancels main). A Ctrl-C while the root scope drains after main ended is a request too, and a second one forces the exit.
  - Like `asyncio.Runner`, the defaults are installed only for a signal that has Python's default disposition: SIGINT when `signal.getsignal(SIGINT)` is `default_int_handler`, SIGTERM when it is `SIG_DFL`. An ignored SIGINT (`nohup`, `cmd &`) or a handler the program installed is left alone; if neither signal qualifies nothing is installed (no pipe).
  - With `handle_signals=False`, or when the runtime does not start on the main thread, nothing is installed and Python's defaults remain: SIGINT raises `KeyboardInterrupt` at the next checkpoint (the lane polls for it every 50 ms), SIGTERM ends the process.
  - Windows: not implemented yet (Python's own handling stays).
