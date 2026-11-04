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
- Implementation: `signal-hook`-style self-pipe registered with the driver. Python-level handlers are **not** installed. Delivery goes to the receiver regardless of which thread the OS chose.
- **Default runtime behavior** (`handle_signals=True`): SIGINT → cancel the root scope with reason `KeyboardInterrupt`; `porto.run` raises `KeyboardInterrupt` after shutdown. SIGTERM → same, with `porto.Terminated(SystemExit)`. A second SIGINT within the graceful period forces immediate exit. With `handle_signals=False`, Python's defaults remain.
