//! `porto.net.TcpStream` (17-networking-tls-dns.md §1.1): a connected TCP socket registered
//! with the driver of the run that made it, whose methods return the awaitables of the
//! [`IoOp`]s. It implements `porto.abc.ByteStream`.
//!
//! The stream keeps one fact of its own: whether a write was cancelled after it sent part of its
//! data (13-io-driver.md §5). The bytes on the wire then stop in the middle of what the caller
//! meant to send, so later writes raise `porto.BrokenResourceError` instead of continuing the
//! message from the wrong place.

use std::net::Shutdown;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use porto_net::tcp;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use super::raw_number;
use crate::driver::ready::{ready, ready_none};
use crate::driver::step;
use crate::errors::BrokenResourceError;
use crate::io::buffer_view::{PyBufferMut, PyBufferRef};
use crate::io::handle::register_on_lane;
use crate::io::op::{IoOp, IoRegistration, MidWrite, os_error, start_connect, submit};
use crate::io::sockaddr;

/// The most bytes one `read` asks for by default.
const DEFAULT_MAX: i64 = 65536;

/// A TCP connection.
#[pyclass(frozen, module = "porto.net", name = "TcpStream")]
pub(crate) struct TcpStream {
    registration: Arc<IoRegistration>,
    /// Set when a write was cancelled after sending part of its data.
    mid_write: MidWrite,
    /// The peer's address, known when the stream was accepted, else read on first use.
    peer: OnceLock<SockAddr>,
    /// This end's address, read on first use.
    local: OnceLock<SockAddr>,
}

impl TcpStream {
    /// A stream over `socket` (nonblocking), registered with the driver of the run on this
    /// thread. `peer` is the peer's address, if the accept gave it.
    pub(crate) fn adopt(_py: Python<'_>, socket: Socket, peer: Option<SockAddr>) -> PyResult<Self> {
        Ok(Self {
            registration: register_on_lane(socket, "a TcpStream")?,
            mid_write: Arc::new(AtomicBool::new(false)),
            peer: peer.map_or_else(OnceLock::new, OnceLock::from),
            local: OnceLock::new(),
        })
    }

    fn submit(&self, py: Python<'_>, op: IoOp) -> PyResult<Py<PyAny>> {
        submit(py, &self.registration, op)
    }

    /// Raises `BrokenResourceError` if a cancelled write left the stream mid-message.
    fn ensure_not_mid_write(&self) -> PyResult<()> {
        if self.mid_write.load(Ordering::Acquire) {
            return Err(BrokenResourceError::new_err(
                "porto: this stream was left mid-write by a cancellation: a write was cancelled \
                 after it had sent part of its data, so the bytes on the wire stop in the middle \
                 of a message and anything written now would corrupt it; close the stream \
                 (await stream.aclose()) and open a new connection",
            ));
        }
        Ok(())
    }

    /// The address `read` caches: `known`, or what `lookup` returns the first time.
    fn address(
        py: Python<'_>,
        cell: &OnceLock<SockAddr>,
        lookup: impl FnOnce() -> std::io::Result<SockAddr>,
    ) -> PyResult<Py<PyAny>> {
        if let Some(address) = cell.get() {
            return sockaddr::to_py(py, address);
        }
        let address = lookup().map_err(|error| os_error(&error))?;
        sockaddr::to_py(py, cell.get_or_init(|| address))
    }
}

/// `max` as the length of a receive: at least 1, because a receive of 0 bytes returns 0, which
/// reads as the end of the stream.
fn receive_length(max: i64, call: &str) -> PyResult<usize> {
    usize::try_from(max)
        .ok()
        .filter(|&max| max >= 1)
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "porto: {call} needs max >= 1 (got {max}): a read of 0 bytes would look like the \
                 end of the stream; ask for at least one byte"
            ))
        })
}

#[pymethods]
impl TcpStream {
    /// Receives at most `max` bytes: `bytes`, empty at the end of the stream (the peer closed
    /// or shut down its writing side).
    #[pyo3(signature = (max = DEFAULT_MAX, /))]
    fn read(&self, py: Python<'_>, max: i64) -> PyResult<Py<PyAny>> {
        let max = receive_length(max, "read()")?;
        self.submit(py, IoOp::Read { max })
    }

    /// Receives at most `max` bytes into a `porto.Buffer` from the lane's pool, without the
    /// copy into `bytes`: the buffer, empty at the end of the stream.
    #[pyo3(signature = (max = DEFAULT_MAX, /))]
    fn read_buffer(&self, py: Python<'_>, max: i64) -> PyResult<Py<PyAny>> {
        let max = receive_length(max, "read_buffer()")?;
        self.submit(py, IoOp::ReadBuffer { max })
    }

    /// Receives into `buffer`, a writable bytes-like object: the number of bytes received, 0
    /// at the end of the stream.
    fn read_into(&self, py: Python<'_>, buffer: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let buf = PyBufferMut::new(buffer, "read_into()'s buffer")?;
        if buf.is_empty() {
            return Err(PyValueError::new_err(
                "porto: read_into()'s buffer is empty: a read into no space would look like the \
                 end of the stream; pass a buffer with room for at least one byte",
            ));
        }
        self.submit(py, IoOp::ReadInto { buf })
    }

    /// Sends all of `data`, waiting while the socket's buffers are full: `None` once the last
    /// byte went out. A cancellation after part of it was sent leaves the stream broken.
    fn write(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let data = PyBufferRef::new(data, "write()'s data")?;
        self.ensure_not_mid_write()?;
        self.submit(
            py,
            IoOp::Write {
                data,
                off: 0,
                mid_write: Some(Arc::clone(&self.mid_write)),
            },
        )
    }

    /// Sends all of `buffers` (an iterable of bytes-like objects) in order, with as few system
    /// calls as the OS allows: `None` once the last byte went out.
    fn writev(&self, py: Python<'_>, buffers: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let mut views = Vec::new();
        for (index, buffer) in buffers.try_iter()?.enumerate() {
            views.push(PyBufferRef::new(
                &buffer?,
                &format!("writev()'s buffers[{index}]"),
            )?);
        }
        self.ensure_not_mid_write()?;
        self.submit(
            py,
            IoOp::WriteV {
                buffers: views,
                written: 0,
                mid_write: Some(Arc::clone(&self.mid_write)),
            },
        )
    }

    /// Half-close: tells the peer that nothing more will be sent (it reads the end of the
    /// stream) while this side can still read.
    fn shutdown_write(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        step::raise_if_cancelled(py)?;
        self.registration
            .source()
            .and_then(|socket| socket.shutdown(Shutdown::Write))
            .map_err(|error| os_error(&error))?;
        ready_none(py)
    }

    /// Closes the stream: operations waiting on it fail with `ClosedResourceError`, and what
    /// was sent is delivered before the connection ends. Closing again does nothing. Closes
    /// even when the calling task is cancelled.
    fn aclose(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.close()?;
        ready_none(py)
    }

    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        ready(slf.py(), slf.clone().into_any().unbind())
    }

    #[pyo3(signature = (*_exception))]
    fn __aexit__(&self, py: Python<'_>, _exception: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.aclose(py)
    }

    /// Sends small writes at once (`True`) instead of waiting to coalesce them (Nagle).
    fn set_nodelay(&self, nodelay: bool) -> PyResult<()> {
        let socket = self
            .registration
            .source()
            .map_err(|error| os_error(&error))?;
        tcp::set_nodelay(&socket, nodelay).map_err(|error| os_error(&error))
    }

    /// Turns on TCP keepalive: the first probe after `idle` seconds without traffic, then one
    /// every `interval` seconds, the connection dropped after `count` unanswered probes. The OS
    /// counts whole seconds, so fractions round up.
    fn set_keepalive(&self, idle: f64, interval: f64, count: u32) -> PyResult<()> {
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
        if count == 0 {
            return Err(invalid());
        }
        let socket = self
            .registration
            .source()
            .map_err(|error| os_error(&error))?;
        tcp::set_keepalive(&socket, idle, interval, count).map_err(|error| os_error(&error))
    }

    /// The peer's address: `(host, port)` for IPv4, `(host, port, flowinfo, scope_id)` for IPv6.
    #[getter]
    fn peername(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Self::address(py, &self.peer, || self.registration.source()?.peer_addr())
    }

    /// This end's address, in the same shape.
    #[getter]
    fn sockname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Self::address(py, &self.local, || self.registration.source()?.local_addr())
    }

    /// The OS file descriptor (the socket handle on Windows), for code that integrates with it:
    /// the stream still owns it, and closes it. `ClosedResourceError` once closed.
    fn fileno(&self) -> PyResult<i64> {
        let socket = self
            .registration
            .source()
            .map_err(|error| os_error(&error))?;
        Ok(raw_number(&socket))
    }

    /// A TCP socket for `address`'s family, registered with this run's driver and connecting
    /// to `address` (a numeric `(host, port)`): await `_finish_connect()` for the result.
    /// What `porto.net.connect_tcp` builds on (szb.69), private until then.
    #[staticmethod]
    fn _connecting(py: Python<'_>, address: &Bound<'_, PyAny>) -> PyResult<Self> {
        let address = sockaddr::from_py(address)?;
        let socket = Socket::new(
            Domain::from(i32::from(address.family())),
            Type::STREAM,
            Some(Protocol::TCP),
        )
        .map_err(|error| os_error(&error))?;
        socket
            .set_nonblocking(true)
            .map_err(|error| os_error(&error))?;
        let stream = Self::adopt(py, socket, None)?;
        start_connect(py, &stream.registration, &address)?;
        Ok(stream)
    }

    /// Waits until the connect `_connecting` started is done: `None`, or the connect's error.
    fn _finish_connect(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.submit(py, IoOp::Connect)
    }
}

impl TcpStream {
    /// Closes the registration (see `aclose`).
    fn close(&self) -> PyResult<()> {
        self.registration.close().map_err(|error| os_error(&error))
    }
}
