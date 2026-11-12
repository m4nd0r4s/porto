//! `porto.net`: the TCP stream and listener (17-networking-tls-dns.md §1), over the same
//! registered sockets and [`IoOp`](crate::io::op::IoOp)s as the `_IoHandle` test hook.
//!
//! - `tcp_stream`: `TcpStream`, the `porto.abc.ByteStream` of a TCP connection;
//! - `tcp_listener`: `TcpListener` and `listen_tcp`'s socket.
//!
//! The Python-free parts (the listening socket, accept, the options) are in `porto-net`.

mod tcp_listener;
mod tcp_stream;

use std::ops::RangeInclusive;

use pyo3::exceptions::{PyOverflowError, PyValueError};
use pyo3::prelude::*;
use socket2::Socket;

pub(crate) use tcp_stream::TcpStream;

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TcpStream>()?;
    tcp_listener::register(module)
}

/// The `int` `value` (`TypeError` for anything else), which must be in `range`: a `ValueError`
/// that names the argument and the allowed range otherwise, also for an int too large for any
/// machine integer (which Python's conversion would report as an `OverflowError`).
fn int_in_range(
    value: &Bound<'_, PyAny>,
    call: &str,
    argument: &str,
    range: RangeInclusive<i64>,
    hint: &str,
) -> PyResult<i64> {
    let out_of_range = || {
        PyValueError::new_err(format!(
            "porto: {call}'s {argument} must be an int in {}..={}{hint}, got {}",
            range.start(),
            range.end(),
            value
                .repr()
                .map_or_else(|_| "something else".to_owned(), |repr| repr.to_string())
        ))
    };
    match value.extract::<i64>() {
        Ok(number) if range.contains(&number) => Ok(number),
        Ok(_) => Err(out_of_range()),
        Err(error) if error.is_instance_of::<PyOverflowError>(value.py()) => Err(out_of_range()),
        Err(error) => Err(error),
    }
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
