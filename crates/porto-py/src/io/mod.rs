//! I/O on registered sockets (13-io-driver.md §5, §6).
//!
//! - `op`: the [`IoOp`](op::IoOp) set an `Io` trap carries, and `submit`, its fast path
//!   (attempt at once, a ready awaitable on success) and slow path (one `Io` trap that waits
//!   for readiness and attempts again);
//! - `buffer` and `pool`: `porto.Buffer` and the per-lane pool of its storage (§6);
//! - `recv_buffer` and `buffered`: a stream's receive buffer, and the buffered reads
//!   (`read_exact`, `read_until`, ...) that fill and drain it;
//! - `buffer_view`: the Python buffers an operation sends from or receives into, released on
//!   a Python lane (P1);
//! - `sockaddr`: socket addresses to and from Python;
//! - `handle`: `_IoHandle`, a registered socket with the operations as methods (a test hook
//!   until the public socket types).

pub(crate) mod buffer;
pub(crate) mod buffer_view;
pub(crate) mod buffered;
pub(crate) mod handle;
pub(crate) mod op;
pub(crate) mod pool;
pub(crate) mod recv_buffer;
pub(crate) mod sockaddr;
