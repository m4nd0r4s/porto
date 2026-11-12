# 13 — I/O Driver

## 1. Model
Porto exposes **readiness** semantics universally (ADR-005). A resource is registered once. Each operation is attempted nonblocking. On `WouldBlock`, the task waits for readiness of the needed interest, then retries.

## 2. Driver trait

```rust
pub trait Driver: Send + Sync {
    fn register(&self, src: RawSource, interest: Interest) -> io::Result<RegistrationId>;
    fn reregister(&self, id: RegistrationId, interest: Interest) -> io::Result<()>;
    fn deregister(&self, id: RegistrationId) -> io::Result<()>;
    /// Block for at most `timeout`, dispatching readiness to ScheduledIo entries.
    fn turn(&self, timeout: Option<Duration>) -> io::Result<usize>;
    fn waker(&self) -> &DriverWaker;  // unblocks `turn` from another thread
    fn slab(&self) -> &IoSlab;        // the ScheduledIo entries (§2.1)
    /// Whether anything is registered; the lane skips its non-blocking poll otherwise (12 §9).
    fn has_registrations(&self) -> bool { self.slab().live() > 0 }
    /// Release every registration, waking its waiters (runtime shutdown).
    fn shutdown(&self);
    fn kind(&self) -> DriverKind;
}
```

### 2.1 ScheduledIo (per registration)
A slab entry (generation-tagged ID, like Tokio's `ScheduledIo`) holding:
- `readiness: AtomicUsize` — packs the readiness bits (READABLE, WRITABLE, READ_CLOSED, WRITE_CLOSED, ERROR, PRIORITY) plus a 15-bit tick.
- `waiters: Mutex<Waiters>` — `reader: Option<Waker>`, `writer: Option<Waker>`, plus an intrusive list for additional waiters (rare: several tasks waiting on the same socket).

Edge-triggered registration (EPOLLET) is used. Readiness bits are **cleared by the consumer** on `WouldBlock`, using the tick to avoid clearing newer events (Tokio's `clear_readiness(event)` technique).

## 3. Backends

| Backend | OS | Selection |
|---|---|---|
| `epoll` (mio) | Linux | default |
| `kqueue` (mio) | macOS/BSD | default |
| `iocp` (mio AFD emulation) | Windows | default |
| `io_uring` | Linux ≥ 5.19 | `io_backend="io_uring"` |
| `sim` | any | deterministic mode (§24) |

`io_backend="auto"` picks the OS default. A backend that this OS lacks (`epoll` on macOS, `kqueue` on Linux, `iocp` off Windows) or that is not built yet falls back to the OS default with a warning; with `io_backend_strict=True` it is an error that names the alternatives. mio registers every source edge-triggered (`EPOLLET`, `EV_CLEAR`); its tokens are `RegistrationId`s, and `usize::MAX` is reserved for the driver waker.

### 3.1 io_uring backend
- Probe: `io_uring_setup(256, params)` with `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN` when available. On `EPERM`/`ENOSYS`/`EINVAL`, fall back to epoll and warn, unless `io_backend_strict`.
- One ring **per lane** (single issuer). The ring owned by the lane that registered the resource services it.
- Readiness for ABI users: `IORING_OP_POLL_ADD` with `IORING_POLL_ADD_MULTI` (multishot). Results map into the same `ScheduledIo` readiness bits.
- Completion fast paths, internal to `porto-net` only:
  - `accept` multishot
  - `recv` with provided buffer rings (`IORING_REGISTER_PBUF_RING`) for TcpStream reads into pooled buffers
  - `send` with registered buffers when writing `Buffer`s
  - `openat`/`read`/`write`/`statx` for `porto-fs`
- Memory: `RLIMIT_MEMLOCK` issues → fall back with a clear message.

## 4. Registration lifecycle
- `Registration::new(source, interest)` → driver.register. The registration owns the source (an FD on Unix, a socket on Windows).
- Drop → deregister. The slab entry is kept until the generation is bumped, so stale wakers are ignored.
- Closing an FD while another task waits on it: `close()` first deregisters, wakes all waiters with `READ_CLOSED|WRITE_CLOSED|ERROR`, then closes the FD. Waiters observe `porto.ClosedResourceError` (a subclass of `OSError`, errno `EBADF`).
- Close while another task operates on it: each operation holds a reference to the source for the duration of its syscall, and `close()` holds one while it deregisters. `close()` marks the registration closed (no new operation starts), and the FD is closed when the last reference is dropped (Go's `fdMutex` scheme). So the driver never deregisters, and no operation ever uses, an FD number that the close freed and the OS gave to another resource. A loom model covers the race (`close_gate::loom_tests`).
- A registration belongs to the run whose driver it was registered with, and only that run's lane polls the driver. An operation called from a task of another run (another thread's `porto.run`, or a later one) or from no run would wait for readiness nobody reports, so the call raises `RuntimeError` instead, saying to open the socket in the run that uses it (`Registration::is_driven_by`). `close()` needs no run.
- A closed registration never reads its slab entry again: its own closed state is checked first, so neither a reused slot nor the 8-bit generation wrapping lets a later owner's readiness reach it. A registration released by the driver (runtime shutdown) fails the same way, with an error that says the runtime shut down.

## 5. The IoOp enum (used by Trap::Io)

```rust
enum IoOp {
    Read { max: usize },                 // → bytes
    ReadBuffer { max: usize },           // → porto.Buffer (pooled, no bytes copy)
    ReadInto { buf: PyBufferMut },       // → int
    Write { data: PyBufferRef, off: usize },   // loops until all written → None
    Accept,                              // → (TcpStream, addr)
    Connect,                             // → None (check SO_ERROR)
    RecvFrom { max: usize },             // → (bytes, addr)
    SendTo { data: PyBufferRef, addr: SockAddr },
    WaitReadable, WaitWritable,          // raw readiness (porto.lowlevel)
}
```
The driver-side `poll` re-executes the op when readiness fires. This keeps one generic trap kind for all socket ops and avoids per-op Rust futures.

The call that makes the awaitable attempts the op once, nonblocking, whatever readiness the driver has reported (the fast path, 10 §4): on success it returns a ready awaitable, and on `WouldBlock` it clears the readiness it observed (unless newer readiness arrived) and returns one `Io` trap. The call first raises `Cancelled` if the task is cancelled (C1), so a fast path never consumes data it cannot deliver, and a task out of budget gets the trap without an attempt (T2). The trap's wait waits for readiness, attempts again, and resumes the task in the poll whose attempt succeeds. T3: a dropped receive, accept or connect wait consumed nothing. A `Write` sends what it can at each attempt, so one cancelled after a partial send (`off > 0`) has sent part of its data and the stream is in an unknown state, as with trio's `send_all`: the public stream API marks such a stream broken (later writes raise `porto.BrokenResourceError`). `connect()` checks for cancellation before it starts the OS connect. `WaitReadable` and `WaitWritable` always return the trap. OS errors become `OSError` with their errno, so Python picks the subclass (`ConnectionRefusedError`, …).

## 6. Buffers
- `porto.Buffer` is a Rust-owned, growable byte buffer that exposes the buffer protocol (read/write). It is pooled per lane (size classes 4K, 16K, 64K, 256K).
  - `Buffer(size=0)` holds `size` zero bytes; `len(buf)`, `buf.capacity`, `buf.resize(size)`, `buf.extend(data)`. While a view is exported (a `memoryview`, or a pending operation's view) `resize` and `extend` raise `BufferError`, so the exported memory never moves.
  - Storage comes from the pool of the Python lane the buffer is made on, the smallest class that holds the request, and goes back to the pool of the Python lane it is freed on (anywhere else it is just freed). A request above 256K is allocated exactly and never pooled. Each class keeps at most 1 MiB of free storage, and a lane empties its pool when its run ends.
- `PyBufferRef` holds a `Py_buffer` view (from `PyObject_GetBuffer` with `PyBUF_SIMPLE`) and releases it on drop. It MUST be released on a Python lane (P1).
- Zero-copy reads: `stream.read_into(buf)` and `stream.read_buffer(max) -> porto.Buffer` (no `bytes` copy). `bytes(buf)` copies on demand.

## 7. Low-level Python API (`porto.lowlevel`)
`wait_readable(fileobj_or_fd)`, `wait_writable(...)`, `notify_closing(fd)`, `current_driver_kind()`. These are for integrating third-party sockets (e.g., a C library's FD).
