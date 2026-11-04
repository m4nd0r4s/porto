# 31 — HTTP Server Engine

## 1. Stack
- `hyper` 1.x `server::conn::{http1, http2}` with `hyper-util`'s auto connection builder for ALPN or h2c-prior-knowledge detection.
- I/O: `PortoAbiStream` implements `hyper::rt::Read + Write` over `porto_abi` readiness (§21 B.4). TLS: `rustls::ServerConnection` driven inside the same adapter (`TlsStream<PortoAbiStream>`).
- Executor: `PortoAbiExecutor` (h2 stream tasks spawned through `api.spawn` into the connection scope). Timer: `PortoAbiTimer` (`sleep_until`).

## 2. Accept loop
- Listener sockets are created by the supervisor (§38) and inherited by FD, or created by the worker when running without a supervisor.
- One accept task per lane per listener (free-threaded) or per worker (GIL). Uses `accept4` loops with batched accepts (up to 64 per readiness event) to amortize wakes.
- Backpressure: if `active_requests_on_lane >= max_concurrent_requests_per_lane` (default 1024), the accept task stops accepting until it drops below 90%. Kernel backlog = `--backlog` (default 2048).

## 3. Connection handling
- Per connection: `ConnState { peer, tls_info, alpn, requests_served, last_activity }`.
- HTTP/1.1: keep-alive (default on), pipelining handled sequentially by hyper, `max_requests_per_connection` (default unlimited), `keep_alive_timeout` (5 s), header read timeout (10 s).
- HTTP/2: `max_concurrent_streams` (100), initial window sizes (stream 1 MiB, conn 2 MiB), `max_header_list_size` (16 KiB), keep-alive ping (20 s interval, 20 s timeout), and the rapid-reset mitigations of the hyper/h2 version in use, plus a per-connection reset rate limit (100 resets / 10 s → GOAWAY).
- h2c: prior-knowledge only (no `Upgrade: h2c`, which is deprecated in RFC 9113).

## 4. Request object model (Rust)
```rust
struct RequestHead { method: Method, uri: Uri, version: Version, headers: HeaderMap, extensions: Extensions }
struct RequestCtx {
    head: RequestHead, route: RouteId, params: SmallVec<[(Atom, Range<usize>); 4]>,  // into path string
    body: BodyState,       // Unread(hyper::body::Incoming) | Buffered(Bytes) | Consumed
    conn: Arc<ConnState>, scope: ScopeHandle, start: u64, request_id: RequestId,
}
```
The Python `douro.Request` object is created **only if** the handler (or a dependency/middleware) asks for it (§33). It is a pyclass wrapping `Arc<RequestCtx>` with lazy accessors (`headers` view without copying, `query` dict built on first access, `await body()`, `await json()`, `stream()`).

## 5. Body handling
- If the plan has a typed body param: the Rust side reads the body (respecting `max_body_size` and the content-length precheck) into a `Bytes` buffer (zero-copy collect for single-chunk bodies), then runs the fused parser+validator (§34).
- Streaming bodies (`body: douro.BodyStream` or `request.stream()`): chunks are delivered as `bytes`/`porto.Buffer` with backpressure (hyper flow control).
- `Expect: 100-continue`: the 100 is sent only when the handler or plan actually reads the body, after limit checks. A request too large gets 413 without reading.
- Content types: `application/json` (+ `*+json`), `application/x-www-form-urlencoded`, `multipart/form-data` (streaming parser `multer`-like, implemented on our body stream, with per-part limits), `application/octet-stream`, `text/plain`. Mismatch → 415.

## 6. Response writing
- Fixed body: `Content-Length` known, single write (headers + body coalesced with `writev`).
- Streaming: chunked (h1) or DATA frames (h2), flushed per chunk, with backpressure from the socket.
- Files: `sendfile` (Linux, plaintext only) or a 256 KiB read-ahead loop. Range requests and ETag/Last-Modified are supported (§36).
- Compression: gzip/br/zstd negotiated by `Accept-Encoding`, only for compressible content types and sizes ≥ 1 KiB, done on the connection's lane (Rust), streaming encoders. Disabled by default; enabled by `CompressionLayer`.
- `Date` header cached per lane, refreshed every second.
- `Server` header: off by default.

## 7. Request scope and cancellation
- Request scope = child of the connection scope, with deadline = `route.timeout` (if any).
- Client disconnect detection: for h1, a read-side EOF/RST on the connection while the request is in flight; hyper reports it when we poll the connection, and we also register read interest during handler execution. For h2, RST_STREAM or connection close. On detection: apply the route's `on_disconnect` policy (§33 §8).
- Timeout → `504 Gateway Timeout` problem response if headers were not sent yet. Otherwise the stream is aborted (h2 RST_STREAM / h1 connection close).

## 8. Graceful shutdown (per worker)
On a shutdown signal from the supervisor:
1. Stop accept tasks.
2. h1: set `Connection: close` on in-flight responses and close idle keep-alive connections immediately. h2: send GOAWAY(last_stream_id).
3. Wait for in-flight requests ≤ `graceful_timeout` (30 s default for the server).
4. Cancel the server scope.
5. Exit.

## 9. HTTP/3 (designed, deferred: M9+)
QUIC via `quinn` with UDP I/O through the porto ABI (`io_register` on a UDP socket). Advertised via `Alt-Svc`. The rest of the pipeline from the router onward is unchanged, because it takes `RequestHead` + body stream.
