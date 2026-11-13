//! `porto.net.TcpStream` (17-networking-tls-dns.md §1.1): a connected TCP socket. Its reads,
//! writes and close are the [`SocketStream`] base class's; this adds the TCP socket options and
//! the connect `porto.net.connect_tcp` builds on.

use std::time::Duration;

use porto_net::tcp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pyclass_init::PyClassInitializer;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use super::int_in_range;
use super::stream::SocketStream;
use crate::io::op::{IoOp, os_error, start_connect};
use crate::io::sockaddr;

/// A TCP connection.
#[pyclass(frozen, extends = SocketStream, module = "porto.net", name = "TcpStream")]
pub(crate) struct TcpStream;

impl TcpStream {
    /// A stream over `socket` (nonblocking), registered with the driver of the run on this
    /// thread. `peer` is the peer's address, if the accept gave it.
    pub(crate) fn adopt(
        socket: Socket,
        peer: Option<SockAddr>,
    ) -> PyResult<PyClassInitializer<Self>> {
        Ok(
            PyClassInitializer::from(SocketStream::adopt(socket, "a TcpStream", peer)?)
                .add_subclass(Self),
        )
    }
}

#[pymethods]
impl TcpStream {
    /// Sends small writes at once (`True`) instead of waiting to coalesce them (Nagle).
    fn set_nodelay(slf: &Bound<'_, Self>, nodelay: bool) -> PyResult<()> {
        let socket = slf
            .as_super()
            .get()
            .registration()
            .source()
            .map_err(|error| os_error(&error))?;
        tcp::set_nodelay(&socket, nodelay).map_err(|error| os_error(&error))
    }

    /// Turns on TCP keepalive: the first probe after `idle` seconds without traffic, then one
    /// every `interval` seconds, the connection dropped after `count` unanswered probes. The OS
    /// counts whole seconds, so fractions round up.
    fn set_keepalive(
        slf: &Bound<'_, Self>,
        idle: f64,
        interval: f64,
        count: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let count = int_in_range(
            count,
            "set_keepalive()",
            "count",
            1..=i64::from(u32::MAX),
            " (the probes before the connection is dropped)",
        )?;
        let count = u32::try_from(count).unwrap_or(u32::MAX);
        let invalid = || {
            PyValueError::new_err(format!(
                "porto: set_keepalive(idle, interval, count) needs idle and interval to be \
                 positive finite numbers of seconds and count >= 1 (got idle={idle}, \
                 interval={interval}, count={count})"
            ))
        };
        let seconds = |value: f64| {
            if value > 0.0 {
                Duration::try_from_secs_f64(value).map_err(|_| invalid())
            } else {
                Err(invalid())
            }
        };
        let (idle, interval) = (seconds(idle)?, seconds(interval)?);
        let socket = slf
            .as_super()
            .get()
            .registration()
            .source()
            .map_err(|error| os_error(&error))?;
        tcp::set_keepalive(&socket, idle, interval, count).map_err(|error| os_error(&error))
    }

    /// A TCP socket for `address`'s family, registered with this run's driver and connecting
    /// to `address` (a numeric `(host, port)`), bound to `local_address` first when one is given
    /// (the same shape, of the same family): await `_finish_connect()` for the result. What
    /// `porto.net.connect_tcp` builds on.
    #[staticmethod]
    #[pyo3(signature = (address, local_address=None))]
    fn _connecting(
        py: Python<'_>,
        address: &Bound<'_, PyAny>,
        local_address: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        let address = sockaddr::from_py(address)?;
        let local = local_address.map(sockaddr::from_py).transpose()?;
        let socket = Socket::new(
            Domain::from(i32::from(address.family())),
            Type::STREAM,
            Some(Protocol::TCP),
        )
        .map_err(|error| os_error(&error))?;
        socket
            .set_nonblocking(true)
            .map_err(|error| os_error(&error))?;
        if let Some(local) = local {
            socket.bind(&local).map_err(|error| os_error(&error))?;
        }
        let stream = Py::new(py, Self::adopt(socket, None)?)?;
        start_connect(
            py,
            stream.bind(py).as_super().get().registration(),
            &address,
        )?;
        Ok(stream)
    }

    /// Waits until the connect `_connecting` started is done: `None`, or the connect's error.
    fn _finish_connect(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        slf.as_super().get().submit(py, IoOp::Connect)
    }
}
