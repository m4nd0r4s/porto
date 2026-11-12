# 17 — Networking, TLS, DNS

## 1. TCP

```python
stream = await porto.net.connect_tcp(host: str, port: int, *,
                tls: bool | porto.tls.ClientConfig = False,
                server_hostname: str | None = None,
                local_addr: tuple[str,int] | None = None,
                happy_eyeballs_delay: float = 0.25,
                nodelay: bool = True) -> porto.net.TcpStream
listener = await porto.net.listen_tcp(host: str | None = None, port: int = 0, *,
                backlog: int = 1024, reuse_port: bool = False,
                tls: porto.tls.ServerConfig | None = None,
                dual_stack: bool = True) -> porto.net.TcpListener
await porto.net.serve_tcp(handler, host=None, port=0, *, tls=None,
                          max_connections: int | None = None,
                          started: porto.Started[int] | None = None)  # runs forever; one task per conn in its own scope
```
`connect_tcp` resolves DNS (§4) and performs RFC 8305 Happy Eyeballs v2 using a scope of racing attempts. Timeouts come from the enclosing scope's deadline.

**`listen_tcp` arguments.** `host` is a numeric IP address, or `None` for every interface; until the resolver (§4) lands, a host name is a `ValueError` that says to resolve it first. `None` with `dual_stack=True` listens on `::` with `IPV6_V6ONLY` off (IPv4 peers show as IPv4-mapped IPv6 addresses), falling back to `0.0.0.0` on a host without IPv6; with `dual_stack=False` it listens on `0.0.0.0` (IPv4 only). For an IPv6 `host`, `dual_stack` sets `IPV6_V6ONLY` to its opposite. `port` is 0..=65535 (`ValueError` otherwise; 0 picks a free port, `listener.port` reports it). `reuse_port` sets `SO_REUSEPORT` (`NotImplementedError` where the OS has none, as on Windows); `SO_REUSEADDR` is set on Unix, not on Windows. `tls` other than `None` raises `NotImplementedError` until §3 lands. The call is cancellable (a cancelled task opens nothing) and raises `RuntimeError` outside a run; OS refusals are `OSError` with their errno (`EADDRINUSE`, `EACCES`). Every socket porto opens or accepts is close-on-exec and nonblocking.

**Runs.** A socket is registered with the driver of the run that opened it (13 §4) and belongs to it: any operation from another run raises `RuntimeError` at the call, saying to open the socket in the run that uses it.

### 1.1 Stream API (`TcpStream`, `UnixStream`, `TlsStream` share the `porto.abc.ByteStream` protocol)
| Method | Semantics |
|---|---|
| `await read(max=65536) -> bytes` | `b""` at EOF; takes bytes the buffered reads left in the receive buffer first |
| `await read_buffer(max=65536) -> porto.Buffer` | zero-copy |
| `await read_into(buf) -> int` | writable buffer protocol |
| `await read_exact(n) -> bytes` | exactly `n` bytes; raises `porto.IncompleteReadError(partial, n)` if the stream ends first |
| `await read_until(sep: bytes, *, limit=65536) -> bytes` | includes sep; raises `porto.LimitOverrunError` |
| `await readline(limit=65536) -> bytes` | `read_until(b"\n", limit=limit)` |
| `await read_to_end(*, limit) -> bytes` | until EOF; `limit` is required |
| `await write(data)` | writes **all** (buffer protocol); backpressure aware |
| `await writev(list_of_buffers)` | vectored |
| `await sendfile(file, offset=0, count=None)` | zero-copy where the OS supports it |
| `await shutdown_write()` | half-close |
| `await aclose()` / `async with` | graceful close; for TLS sends close_notify |
| `set_nodelay(bool)`, `set_keepalive(idle, interval, count)`, `peername`, `sockname`, `fileno()` | `idle` and `interval` are positive seconds, rounded up to whole seconds (what the OS keeps), `count >= 1`; `ValueError` otherwise. `peername`/`sockname` are `(host, port)` or `(host, port, flowinfo, scope_id)`, remembered once read (an accepted stream knows its peer from the accept, so it survives a reset). `fileno()` is the OS descriptor, still owned by the stream; `ClosedResourceError` once closed |

Details of the implemented subset:
- Integer arguments out of range (`read(2**64)`, `set_keepalive(1, 1, -1)`, `listen_tcp(port=2**70)`) are `ValueError`s that name the allowed range, not `OverflowError`.
- One task reads and one task writes at a time: a second concurrent read (or write) raises `porto.BusyResourceError` at the call (13 §5).
- `read(max)`, `read_buffer(max)` need `max >= 1` and `read_into(buf)` a non-empty buffer (`ValueError`): a receive of 0 bytes returns 0, which reads as the end of the stream. Cancelling a read loses no bytes (T3).
- `write` and `writev` raise `TypeError` for anything that is not a bytes-like object, naming the argument (`writev()'s buffers[1]`); `writev` takes any iterable of them and sends with as few vectored system calls as the OS allows.
- A `write`/`writev` cancelled after it sent part of its data leaves the stream broken: later writes raise `porto.BrokenResourceError` (message: an earlier write was cancelled, or failed with an error that did not end the connection, after sending part of its data; close it and open a new connection). Reads and `aclose` still work (13 §5). One that failed after a partial send breaks the stream too, unless its error is a lost connection (reset, broken pipe, not connected, timed out) that later writes repeat on their own.
- `shutdown_write` after which a `write` raises `BrokenPipeError`; a peer reset raises `ConnectionResetError` with `errno` (§5).
- `aclose()` closes at once and is idempotent; it closes even when the calling task is cancelled. Operations waiting on the stream, and every later one, raise `porto.ClosedResourceError` (13 §4). `async with stream` calls it.
- `TcpStream` is a `porto.abc.ByteStream`: a `runtime_checkable` `Protocol` of the methods the stream types share. It holds the methods above now; `sendfile` joins it with its bead.
- `porto.net._connect_tcp_numeric(host, port, *, nodelay=True)` is the private core of `connect_tcp` (a numeric address, no resolution, no Happy Eyeballs), kept until `connect_tcp` replaces it.

**Buffered reads and the internal read buffer.** Each stream owns a receive buffer of the bytes received and not consumed yet, in storage from the lane's pool (13 §6): taken when the first bytes are buffered, handed back as soon as the buffer is empty, so a stream idle between messages holds none. `read_exact`, `read_until`, `readline` and `read_to_end` receive into it and take their result out of it; `read`, `read_buffer` and `read_into` return buffered bytes first (without waiting for the socket; `read_buffer` and `read_into` copy them, the zero-copy receive is for bytes that were not buffered) and only receive when the buffer is empty. Bytes leave the buffer only when an operation completes with them, so cancelling any read loses nothing: what it received is still buffered for the next read of any kind (T3). A read on a closed stream raises `porto.ClosedResourceError`, buffered bytes or not.
- `read_until(sep, *, limit=65536)` scans the buffer without copying, resuming where the last scan of the call stopped (a delimiter that arrives in many small segments costs one pass over the bytes, not one per segment). `sep` is any non-empty bytes-like object (`ValueError` for an empty one, `TypeError` for a `str`); `limit >= len(sep)` (`ValueError` otherwise). The separator must end within the first `limit` bytes: if it does not, `porto.LimitOverrunError` is raised, **nothing is consumed** (the bytes received so far, at most `limit`, stay buffered: read them with `read()`, or call again with a larger limit) and the buffer never grows past `limit` for the call.
- `read_exact(n)` receives no more than the `n` bytes it needs from the socket (anything extra a previous `read_until` buffered counts first). `n >= 0`; `read_exact(0)` returns `b""` at once. `read_to_end(*, limit)` takes `limit` as a required keyword: an unbounded read of a peer's data is a memory bug waiting to happen, so the caller says how much is too much; more than `limit` bytes raise `LimitOverrunError` with the first `limit + 1` bytes left buffered.
- End of stream: `read_exact` and `read_until` raise `porto.IncompleteReadError` whose `.partial` holds the bytes that arrived (consumed, since nothing more can complete them) and `.expected` is `n` (`None` for `read_until`). `readline` is `read_until(b"\n", limit=limit)` and so also raises it at EOF, with `partial == b""` when the stream ended on a line boundary: `while True: try: line = await s.readline() except porto.IncompleteReadError as e: ...` is the loop, with `e.partial` the unterminated last line, if any. `read_to_end` returns what arrived, possibly `b""`. A peer reset raises `ConnectionResetError`, not `IncompleteReadError`.
- A buffered read takes the stream's one reader like `read` does: a second concurrent read of any kind raises `porto.BusyResourceError`.

### 1.2 Listener
`await accept() -> TcpStream`, `async for conn in listener`, `port`, `sockname`, `fileno()`, `aclose()` (also `async with`). On Linux and the BSDs, `accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)` is used; elsewhere `accept` then `fcntl`/`ioctl`, so accepted sockets are nonblocking and close-on-exec everywhere. Cancelling an `accept()` loses no connection (T3). Closing the listener makes a waiting `accept()` raise `porto.ClosedResourceError` and ends an `async for` over it (`StopAsyncIteration`); connections not yet accepted are dropped by the OS.

`EMFILE`/`ENFILE`/`ENOBUFS`/`ENOMEM` handling: `accept` does not fail; it logs a warning on the `porto.net` logger (at most one every 10 s per run), waits 100 ms and tries again, so a server sheds load by not accepting instead of crashing or spinning a core. Closing the listener ends the pause at once. Connections that failed before `accept` could return them (`ECONNABORTED`, `EPROTO`, `EPERM`, and on Linux the network errors accept(2) says to treat like `EAGAIN`; on Windows also a connection reset in the backlog, `WSAECONNRESET`) are skipped silently, as trio and asyncio do: the next queued connection is accepted, so one client's reset never stops a server. At most 64 are skipped in a row; then the accept yields the lane, tries again, and after 4 such rounds over the accept's life backs off and warns like for `EMFILE` (13 §5), so a persistent error (a seccomp policy refusing every accept) or a flood of connect-then-reset clients cannot hold the lane. Several tasks may accept from one listener. On Linux the connection that hit the limit stays queued and is accepted by a later retry; macOS drops it in the kernel.

## 2. UDP and Unix
- `await porto.net.bind_udp(host, port, *, reuse_port=False, broadcast=False) -> UdpSocket` with `send_to`, `recv_from`, `connect`, `send`, `recv`.
- `connect_unix(path)`, `listen_unix(path, *, mode=0o660)`, abstract namespace on Linux (`"\0name"`). `UnixStream.send_fds/recv_fds` (SCM_RIGHTS).

## 3. TLS (rustls)
```python
cfg = porto.tls.ClientConfig.default(alpn=["h2","http/1.1"])        # system roots via rustls-native-certs, fallback webpki-roots
cfg = porto.tls.ClientConfig(roots=porto.tls.Roots.from_pem(...), client_cert=..., verify=True)
srv = porto.tls.ServerConfig.from_pem_files(cert_chain="cert.pem", key="key.pem", alpn=["h2","http/1.1"])
srv = porto.tls.ServerConfig.with_resolver(lambda sni: ...)          # SNI-based cert selection (sync, Rust-cached)
tls_stream = await stream.start_tls(cfg, server_hostname="example.com")
tls_stream.alpn_protocol, tls_stream.peer_certificates(), tls_stream.tls_version
```
- The handshake runs on the stream's lane. With `lanes > 1` on GIL builds, handshakes of streams owned by Rust-only users (Douro) run on Rust lanes.
- Session resumption and tickets are on by default.
- Certificate reload: `ServerConfig.reload()` is atomic and affects new handshakes.
- The crypto provider is `aws-lc-rs` by default (ADR-026). Feature `ring` is the alternative.

## 4. DNS
- Default resolver: `getaddrinfo` on the blocking pool with a per-lane positive cache (TTL 30 s, since getaddrinfo exposes no TTL) and coalescing of concurrent identical lookups.
- Optional: `porto.net.Resolver.hickory(...)` (feature `hickory`) for async DNS with real TTLs.
- `await porto.net.resolve(host, port=0, *, family=0) -> list[SockAddr]`.

## 5. Errors
Socket errors map to builtin `OSError` subclasses (`ConnectionResetError`, `ConnectionRefusedError`, `BrokenPipeError`, `TimeoutError` for OS-level timeouts) with errno set. TLS errors raise `porto.tls.TLSError(OSError)` with a `.reason` code.
