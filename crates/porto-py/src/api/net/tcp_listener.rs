//! `porto.net.TcpListener` and the socket `porto.net.listen_tcp` opens (17-networking-tls-dns.md
//! §1, §1.2): a listening socket registered with the driver of the run that made it, whose
//! `accept()` returns the awaitable of an [`IoOp::AcceptTcp`].
//!
//! Running out of file descriptors does not fail an accept: it waits ([`IoOp`] backs off) and
//! tries again, so a server sheds load by not accepting rather than by crashing.

use std::sync::Arc;

use porto_net::tcp::{self, ListenOptions};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use socket2::SockAddr;

use super::{int_in_range, option_error, parse_host, raw_number};
use crate::driver::ready::{ready, ready_none};
use crate::driver::step;
use crate::io::handle::register_on_lane;
use crate::io::op::{IoOp, IoRegistration, os_error, submit};
use crate::io::sockaddr;

/// A listening TCP socket.
#[pyclass(frozen, module = "porto.net", name = "TcpListener")]
pub(crate) struct TcpListener {
    registration: Arc<IoRegistration>,
    /// What the socket is bound to (the port, when 0 was asked for).
    local: SockAddr,
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<TcpListener>()?;
    module.add_function(wrap_pyfunction!(listen_tcp, module)?)
}

/// Opens a listening socket in this run: `porto.net.listen_tcp` without its argument checks
/// for `tls` (see `python/porto/net.py`).
#[pyfunction(name = "_listen_tcp")]
#[pyo3(signature = (host, port, backlog, reuse_port, dual_stack))]
fn listen_tcp(
    py: Python<'_>,
    host: Option<&str>,
    port: &Bound<'_, PyAny>,
    backlog: &Bound<'_, PyAny>,
    reuse_port: bool,
    dual_stack: bool,
) -> PyResult<TcpListener> {
    let host = parse_host(host, "listen_tcp()")?;
    let port = int_in_range(
        port,
        "listen_tcp()",
        "port",
        0..=i64::from(u16::MAX),
        " (0 picks a free one)",
    )?;
    let backlog = int_in_range(
        backlog,
        "listen_tcp()",
        "backlog",
        0..=i64::from(i32::MAX),
        " (the OS caps it lower)",
    )?;
    let port = u16::try_from(port).unwrap_or(u16::MAX);
    let backlog = i32::try_from(backlog).unwrap_or(i32::MAX);
    // A cancelled task must not open a socket it will not use (C1).
    step::raise_if_cancelled(py)?;
    let socket = tcp::listen(&ListenOptions {
        host,
        port,
        backlog,
        reuse_port,
        dual_stack,
    })
    .map_err(|error| option_error(&error, "reuse_port"))?;
    let local = socket.local_addr().map_err(|error| os_error(&error))?;
    Ok(TcpListener {
        registration: register_on_lane(socket, "a TcpListener")?,
        local,
    })
}

#[pymethods]
impl TcpListener {
    /// Waits for a connection and accepts it: a `TcpStream`, nonblocking and close-on-exec.
    /// If the process or the system is out of file descriptors it waits 100 ms and tries again,
    /// logging a warning (at most every 10 s) on the `porto.net` logger.
    fn accept(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        submit(
            py,
            &self.registration,
            IoOp::AcceptTcp {
                ending_iteration: false,
            },
        )
    }

    fn __aiter__(slf: &Bound<'_, Self>) -> Py<Self> {
        slf.clone().unbind()
    }

    /// Like `accept()`; an `async for` over the listener ends when the listener is closed.
    fn __anext__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        submit(
            py,
            &self.registration,
            IoOp::AcceptTcp {
                ending_iteration: true,
            },
        )
    }

    /// The port the listener is bound to (the one the OS picked, for `port=0`).
    #[getter]
    fn port(&self) -> u16 {
        self.local.as_socket().map_or(0, |address| address.port())
    }

    /// The address the listener is bound to, as `(host, port)` or `(host, port, flowinfo,
    /// scope_id)`.
    #[getter]
    fn sockname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        sockaddr::to_py(py, &self.local)
    }

    /// The OS file descriptor (the socket handle on Windows); the listener still owns it.
    /// `ClosedResourceError` once closed.
    fn fileno(&self) -> PyResult<i64> {
        let socket = self
            .registration
            .source()
            .map_err(|error| os_error(&error))?;
        Ok(raw_number(&socket))
    }

    /// Stops listening: an `accept()` waiting on the listener fails with `ClosedResourceError`
    /// (an `async for` ends), and connections not accepted yet are refused or reset by the OS.
    /// Closing again does nothing. Closes even when the calling task is cancelled.
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
