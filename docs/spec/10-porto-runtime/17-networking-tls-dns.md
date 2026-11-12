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

**Runs.** A socket is registered with the driver of the run that opened it (13 §4) and belongs to it: any operation from another run raises `RuntimeError` at the call, saying to open the socket in the run that uses it.

### 1.1 Stream API (`TcpStream`, `UnixStream`, `TlsStream` share the `porto.abc.ByteStream` protocol)
| Method | Semantics |
|---|---|
| `await read(max=65536) -> bytes` | `b""` at EOF |
| `await read_buffer(max=65536) -> porto.Buffer` | zero-copy |
| `await read_into(buf) -> int` | writable buffer protocol |
| `await read_exact(n) -> bytes` | raises `porto.IncompleteReadError(partial)` |
| `await read_until(sep: bytes, *, limit=65536) -> bytes` | includes sep; raises `porto.LimitOverrunError` |
| `await readline(limit=65536)` | `read_until(b"\n")` |
| `await read_to_end(*, limit) -> bytes` | |
| `await write(data)` | writes **all** (buffer protocol); backpressure aware |
| `await writev(list_of_buffers)` | vectored |
| `await sendfile(file, offset=0, count=None)` | zero-copy where the OS supports it |
| `await shutdown_write()` | half-close |
| `await aclose()` / `async with` | graceful close; for TLS sends close_notify |
| `set_nodelay(bool)`, `set_keepalive(idle, interval, count)`, `peername`, `sockname`, `fileno()` | `idle` and `interval` are positive seconds, rounded up to whole seconds (what the OS keeps), `count >= 1`; `ValueError` otherwise. `peername`/`sockname` are `(host, port)` or `(host, port, flowinfo, scope_id)`, remembered once read (an accepted stream knows its peer from the accept, so it survives a reset). `fileno()` is the OS descriptor, still owned by the stream; `ClosedResourceError` once closed |

Details of the implemented subset:
- `read(max)`, `read_buffer(max)` need `max >= 1` and `read_into(buf)` a non-empty buffer (`ValueError`): a receive of 0 bytes returns 0, which reads as the end of the stream. Cancelling a read loses no bytes (T3).
- `write` and `writev` raise `TypeError` for anything that is not a bytes-like object, naming the argument (`writev()'s buffers[1]`); `writev` takes any iterable of them and sends with as few vectored system calls as the OS allows.
- A `write`/`writev` cancelled after it sent part of its data leaves the stream broken: later writes raise `porto.BrokenResourceError` (message: the stream was left mid-write by a cancellation; close it and open a new connection). Reads and `aclose` still work (13 §5).
- `shutdown_write` after which a `write` raises `BrokenPipeError`; a peer reset raises `ConnectionResetError` with `errno` (§5).
- `aclose()` closes at once and is idempotent; it closes even when the calling task is cancelled. Operations waiting on the stream, and every later one, raise `porto.ClosedResourceError` (13 §4). `async with stream` calls it.
- `TcpStream` is a `porto.abc.ByteStream`: a `runtime_checkable` `Protocol` of the methods the stream types share. It holds the methods above now; the buffered reads and `sendfile` join it with their beads.
- `porto.net._connect_tcp_numeric(host, port, *, nodelay=True)` is the private core of `connect_tcp` (a numeric address, no resolution, no Happy Eyeballs), kept until `connect_tcp` replaces it.

**Internal read buffer:** each stream owns a lane-pooled receive buffer. `read_until`/`readline` scan it without copying until the delimiter is found. Cancel safety (T3) is guaranteed because unconsumed bytes stay in the buffer.

### 1.2 Listener
`await accept() -> TcpStream`, `async for conn in listener`, `port`, `aclose()`. On Linux, `accept4(SOCK_NONBLOCK|SOCK_CLOEXEC)` is used. `EMFILE`/`ENFILE` handling: back off 100 ms and log a rate-limited warning; never busy-loop.

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
