//! `porto.net.UdpSocket` and the socket `porto.net.bind_udp` opens (17-networking-tls-dns.md §2):
//! a bound datagram socket registered with the driver of the run that made it, whose methods
//! return the awaitables of the [`IoOp`]s `RecvFrom`, `SendTo`, `Send` and `Read`.
//!
//! A datagram is received or sent whole, so unlike a stream there is no partial write to break
//! it, no receive buffer, and no single reader or writer: several tasks may wait to receive on
//! one socket, each getting different datagrams (as several tasks may accept from one listener).
//! A cancelled receive consumed nothing (T3), so no datagram is lost to a cancellation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use porto_net::udp::{self, BindOptions, MAX_DATAGRAM};
use pyo3::exceptions::{PyOSError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use socket2::SockAddr;

use super::{int_in_range, option_error, parse_host, raw_number};
use crate::driver::ready::{ready, ready_none};
use crate::driver::step;
use crate::io::buffer_view::PyBufferRef;
use crate::io::handle::register_on_lane;
use crate::io::op::{IoOp, IoRegistration, ensure_same_run, os_error, submit};
use crate::io::sockaddr;

#[cfg(unix)]
const EAFNOSUPPORT: i32 = libc::EAFNOSUPPORT;
#[cfg(windows)]
const EAFNOSUPPORT: i32 = 10047;
#[cfg(unix)]
const ENOTCONN: i32 = libc::ENOTCONN;
#[cfg(windows)]
const ENOTCONN: i32 = 10057;

/// A bound UDP socket.
#[pyclass(frozen, module = "porto.net", name = "UdpSocket")]
pub(crate) struct UdpSocket {
    registration: Arc<IoRegistration>,
    /// What the socket was bound to (the port, when 0 was asked for).
    bound: SockAddr,
    /// An IPv6 socket that takes IPv4 too: IPv4 destinations are mapped for it.
    dual_stack: bool,
    /// Set by `connect()`: the socket has a default peer, and `send` and `recv` work.
    connected: AtomicBool,
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<UdpSocket>()?;
    module.add_function(wrap_pyfunction!(bind_udp, module)?)
}

/// Opens a datagram socket in this run: `porto.net.bind_udp` (see `python/porto/net.py`).
#[pyfunction(name = "_bind_udp")]
#[pyo3(signature = (host, port, reuse_port, broadcast))]
fn bind_udp(
    py: Python<'_>,
    host: Option<&str>,
    port: &Bound<'_, PyAny>,
    reuse_port: bool,
    broadcast: bool,
) -> PyResult<UdpSocket> {
    let host = parse_host(host, "bind_udp()")?;
    let port = int_in_range(
        port,
        "bind_udp()",
        "port",
        0..=i64::from(u16::MAX),
        " (0 picks a free one)",
    )?;
    let port = u16::try_from(port).unwrap_or(u16::MAX);
    if broadcast && host.is_some_and(|host| host.is_ipv6()) {
        return Err(PyValueError::new_err(
            "porto: bind_udp(broadcast=True) needs an IPv4 host (or None): IPv6 has no \
             broadcast; send to a multicast group instead, or bind an IPv4 address",
        ));
    }
    // A cancelled task must not open a socket it will not use (C1).
    step::raise_if_cancelled(py)?;
    let socket = udp::bind(&BindOptions {
        host,
        port,
        reuse_port,
        broadcast,
    })
    .map_err(|error| option_error(&error, "reuse_port"))?;
    let bound = socket.local_addr().map_err(|error| os_error(&error))?;
    let dual_stack = udp::is_dual_stack(&socket);
    Ok(UdpSocket {
        registration: register_on_lane(socket, "a UdpSocket")?,
        bound,
        dual_stack,
        connected: AtomicBool::new(false),
    })
}

impl UdpSocket {
    /// `address` as a destination this socket can send to: an IP address of its family (an
    /// IPv4 one of a dual-stack socket's is mapped), or `OSError(EAFNOSUPPORT)` saying what to do.
    fn destination(&self, address: &Bound<'_, PyAny>, call: &str) -> PyResult<SockAddr> {
        let destination = sockaddr::from_py(address)?;
        if destination.as_socket().is_none() {
            return Err(PyTypeError::new_err(format!(
                "porto: {call}'s address must be a (host, port) tuple of an IP address, not a \
                 Unix socket path"
            )));
        }
        udp::reachable(&self.bound, self.dual_stack, &destination).ok_or_else(|| {
            let (have, want) = if self.bound.is_ipv6() {
                ("IPv6 only", "IPv4")
            } else {
                ("IPv4", "IPv6")
            };
            PyOSError::new_err((
                EAFNOSUPPORT,
                format!(
                    "porto: {call} cannot send to a {want} address ({address}) from a socket \
                     that is {have}; bind a socket of the destination's family (bind_udp(None) \
                     is IPv6 and IPv4 together)",
                    address = address
                        .repr()
                        .map_or_else(|_| "the address".to_owned(), |repr| repr.to_string()),
                ),
            ))
        })
    }

    /// Raises `OSError(ENOTCONN)` unless `connect()` was called.
    fn ensure_connected(&self, call: &str, instead: &str) -> PyResult<()> {
        if self.connected.load(Ordering::Acquire) {
            return Ok(());
        }
        Err(PyOSError::new_err((
            ENOTCONN,
            format!("porto: {call} needs a connected socket, and this one is not; {instead}"),
        )))
    }

    /// The datagram `data` as a buffer view, refused when no datagram can be that long.
    fn datagram(data: &Bound<'_, PyAny>, call: &str) -> PyResult<PyBufferRef> {
        let data = PyBufferRef::new(data, &format!("{call}'s data"))?;
        if data.bytes().len() > MAX_DATAGRAM {
            return Err(PyValueError::new_err(format!(
                "porto: {call}'s data is {} bytes, more than the {MAX_DATAGRAM} one UDP datagram \
                 can carry; split it into several datagrams, or use a stream (TCP)",
                data.bytes().len()
            )));
        }
        Ok(data)
    }

    /// `max`, the optional size of a receive: 1 to 65535, the default being the most a datagram
    /// carries. A datagram longer than the buffer is cut, and the rest of it is lost.
    fn receive_length(max: Option<&Bound<'_, PyAny>>, call: &str) -> PyResult<usize> {
        let Some(max) = max else {
            return Ok(MAX_DATAGRAM);
        };
        let hint = " (a datagram longer than max is cut to it; none is longer than 65535)";
        let max = int_in_range(
            max,
            call,
            "max",
            1..=i64::try_from(MAX_DATAGRAM).unwrap_or(i64::MAX),
            hint,
        )?;
        Ok(usize::try_from(max).unwrap_or(MAX_DATAGRAM))
    }
}

#[pymethods]
impl UdpSocket {
    /// Sends `data` as one datagram to `address`, a numeric `(host, port)`: `None` once the
    /// kernel took it. Waits while the socket's send buffer is full.
    fn send_to(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        address: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        if self.connected.load(Ordering::Acquire) {
            return Err(PyValueError::new_err(
                "porto: send_to() was called on a connected socket, which sends only to its \
                 peer (and where the OS allows a different address, it differs between OSes); \
                 use send(data), or bind another socket that is not connected for send_to",
            ));
        }
        let data = Self::datagram(data, "send_to()")?;
        let addr = self.destination(address, "send_to()")?;
        submit(py, &self.registration, IoOp::SendTo { data, addr })
    }

    /// Receives one datagram: `(bytes, address)` of its sender. At most `max` bytes (1 to 65535,
    /// default 65535); a longer datagram is cut to `max` and the rest of it is lost. Cancelling
    /// loses no datagram.
    #[pyo3(signature = (max = None, /))]
    fn recv_from(&self, py: Python<'_>, max: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let max = Self::receive_length(max, "recv_from()")?;
        submit(py, &self.registration, IoOp::RecvFrom { max })
    }

    /// Makes `address` (a numeric `(host, port)`) the socket's peer: `send` sends there, `recv`
    /// and `recv_from` take only datagrams from it. UDP has no handshake, so this does not
    /// wait for the peer. May be called again to change the peer.
    fn connect(&self, py: Python<'_>, address: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        ensure_same_run(&self.registration)?;
        let address = self.destination(address, "connect()")?;
        step::raise_if_cancelled(py)?;
        self.registration
            .source()
            .and_then(|socket| socket.connect(&address))
            .map_err(|error| os_error(&error))?;
        self.connected.store(true, Ordering::Release);
        ready_none(py)
    }

    /// Sends `data` as one datagram to the peer `connect()` set: `None` once the kernel took
    /// it.
    fn send(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let data = Self::datagram(data, "send()")?;
        self.ensure_connected(
            "send()",
            "call connect((host, port)) first, or use send_to(data, address)",
        )?;
        submit(py, &self.registration, IoOp::Send { data })
    }

    /// Receives one datagram from the peer `connect()` set: `bytes`, at most `max` (as
    /// `recv_from`). Cancelling loses no datagram.
    #[pyo3(signature = (max = None, /))]
    fn recv(&self, py: Python<'_>, max: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let max = Self::receive_length(max, "recv()")?;
        self.ensure_connected(
            "recv()",
            "call connect((host, port)) first, or use recv_from(), which also says who sent the datagram",
        )?;
        submit(
            py,
            &self.registration,
            IoOp::Read {
                max,
                buffered: None,
            },
        )
    }

    /// The port the socket is bound to (the one the OS picked, for `port=0`).
    #[getter]
    fn port(&self) -> u16 {
        self.bound.as_socket().map_or(0, |address| address.port())
    }

    /// The address the socket is bound to, as `(host, port)` or `(host, port, flowinfo,
    /// scope_id)`. After `connect()` on a socket bound to every interface it names the
    /// interface the OS chose.
    #[getter]
    fn sockname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let address = self
            .registration
            .source()
            .and_then(|socket| socket.local_addr())
            .map_err(|error| os_error(&error))?;
        sockaddr::to_py(py, &address)
    }

    /// The peer `connect()` set, in the same shape as `sockname`; `OSError(ENOTCONN)` before it.
    #[getter]
    fn peername(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_connected("peername", "call connect((host, port)) first")?;
        let address = self
            .registration
            .source()
            .and_then(|socket| socket.peer_addr())
            .map_err(|error| os_error(&error))?;
        sockaddr::to_py(py, &address)
    }

    /// The OS file descriptor (the socket handle on Windows); the socket still owns it.
    /// `ClosedResourceError` once closed.
    fn fileno(&self) -> PyResult<i64> {
        let socket = self
            .registration
            .source()
            .map_err(|error| os_error(&error))?;
        Ok(raw_number(&socket))
    }

    /// Closes the socket: operations waiting on it fail with `ClosedResourceError`. Closing
    /// again does nothing. Closes even when the calling task is cancelled.
    fn aclose(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.registration
            .close()
            .map_err(|error| os_error(&error))?;
        ready_none(py)
    }

    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        ready(slf.py(), slf.clone().into_any().unbind())
    }

    #[pyo3(signature = (*_exception))]
    fn __aexit__(&self, py: Python<'_>, _exception: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.aclose(py)
    }
}
