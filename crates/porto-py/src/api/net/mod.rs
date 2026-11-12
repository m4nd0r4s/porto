//! `porto.net`: the TCP stream and listener (17-networking-tls-dns.md §1), over the same
//! registered sockets and [`IoOp`](crate::io::op::IoOp)s as the `_IoHandle` test hook.
//!
//! - `tcp_stream`: `TcpStream`, the `porto.abc.ByteStream` of a TCP connection;
//! - `tcp_listener`: `TcpListener` and `listen_tcp`'s socket.
//!
//! The Python-free parts (the listening socket, accept, the options) are in `porto-net`.

mod tcp_listener;
mod tcp_stream;

use pyo3::prelude::*;
use socket2::Socket;

pub(crate) use tcp_stream::TcpStream;

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TcpStream>()?;
    tcp_listener::register(module)
}

/// The raw descriptor (Unix) or socket (Windows) of `socket`, as the `int` Python's
/// `fileno()` returns.
fn raw_number(socket: &Socket) -> i64 {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        i64::from(socket.as_raw_fd())
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawSocket;
        i64::try_from(socket.as_raw_socket()).unwrap_or(-1)
    }
}
