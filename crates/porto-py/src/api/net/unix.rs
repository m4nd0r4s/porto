//! `porto.net.UnixStream`, `UnixListener` and the sockets `listen_unix` and `connect_unix` open
//! (17-networking-tls-dns.md §2): Unix domain stream sockets over the same registered sockets
//! and [`IoOp`]s as TCP. A `UnixStream`'s reads, writes and close are the [`SocketStream`] base
//! class's; the listener removes the socket file it created when it closes.
//!
//! Unix domain sockets are Unix-only: on Windows `_listen_unix` and `_connect_unix` raise
//! `NotImplementedError`, saying to use TCP on the loopback.

use std::sync::Arc;

#[cfg(not(unix))]
use pyo3::exceptions::PyNotImplementedError;
use pyo3::prelude::*;
use pyo3::pyclass_init::PyClassInitializer;
use pyo3::wrap_pyfunction;
use socket2::{SockAddr, Socket};

#[cfg(unix)]
use super::int_in_range;
use super::raw_number;
use super::stream::SocketStream;
use crate::driver::ready::{ready, ready_none};
#[cfg(unix)]
use crate::driver::step;
#[cfg(unix)]
use crate::io::handle::register_on_lane;
use crate::io::op::{IoOp, IoRegistration, os_error, submit};
use crate::io::sockaddr;

/// The socket file a listener made, removed when the listener closes.
#[cfg(unix)]
struct SocketFile(parking_lot::Mutex<Option<porto_net::unix::SocketFile>>);
#[cfg(not(unix))]
#[allow(dead_code, reason = "never made on an OS without Unix domain sockets")]
struct SocketFile;

impl SocketFile {
    /// Removes the file, if there is one and it is still the one this listener made.
    fn remove(&self) {
        #[cfg(unix)]
        if let Some(file) = self.0.lock().take() {
            file.remove();
        }
    }
}

/// A Unix domain stream connection.
#[pyclass(frozen, extends = SocketStream, module = "porto.net", name = "UnixStream")]
pub(crate) struct UnixStream;

impl UnixStream {
    /// A stream over `socket` (nonblocking), registered with the driver of the run on this
    /// thread. `peer` is the peer's address, if known.
    pub(crate) fn adopt(
        socket: Socket,
        peer: Option<SockAddr>,
    ) -> PyResult<PyClassInitializer<Self>> {
        Ok(
            PyClassInitializer::from(SocketStream::adopt(socket, "a UnixStream", peer)?)
                .add_subclass(Self),
        )
    }
}

#[pymethods]
impl UnixStream {}

/// A listening Unix domain socket.
#[pyclass(frozen, module = "porto.net", name = "UnixListener")]
pub(crate) struct UnixListener {
    registration: Arc<IoRegistration>,
    /// What the socket is bound to.
    local: SockAddr,
    file: SocketFile,
}

impl Drop for UnixListener {
    fn drop(&mut self) {
        // A listener that was never closed still leaves no socket file behind.
        self.file.remove();
    }
}

pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<UnixStream>()?;
    module.add_class::<UnixListener>()?;
    module.add_function(wrap_pyfunction!(listen_unix, module)?)?;
    module.add_function(wrap_pyfunction!(connect_unix, module)?)
}

#[cfg(not(unix))]
fn unsupported(call: &str) -> PyErr {
    PyNotImplementedError::new_err(format!(
        "porto: {call} needs Unix domain sockets, which porto has on Linux and macOS only (its \
         Windows I/O backend does not exist yet); on Windows use TCP on the loopback \
         (listen_tcp(\"127.0.0.1\") and connect_tcp(\"127.0.0.1\", port))"
    ))
}

/// Opens a Unix listening socket in this run: `porto.net.listen_unix` without the conversion of
/// its `path` to `str` or `bytes` (see `python/porto/net.py`).
#[pyfunction(name = "_listen_unix")]
#[pyo3(signature = (path, backlog, mode))]
fn listen_unix(
    py: Python<'_>,
    path: &Bound<'_, PyAny>,
    backlog: &Bound<'_, PyAny>,
    mode: &Bound<'_, PyAny>,
) -> PyResult<UnixListener> {
    #[cfg(not(unix))]
    {
        let _ = (py, path, backlog, mode);
        Err(unsupported("listen_unix()"))
    }
    #[cfg(unix)]
    {
        let address = sockaddr::unix_address(path, "listen_unix()")?;
        let mode = int_in_range(
            mode,
            "listen_unix()",
            "mode",
            0..=0o777,
            " (0o777 at most: the permission bits of the socket file, such as 0o660; connecting \
             needs write permission on it)",
        )?;
        let backlog = int_in_range(
            backlog,
            "listen_unix()",
            "backlog",
            0..=i64::from(i32::MAX),
            " (the OS caps it lower)",
        )?;
        let mode = u32::try_from(mode).unwrap_or(0o777);
        let backlog = i32::try_from(backlog).unwrap_or(i32::MAX);
        // A cancelled task must not open a socket it will not use (C1).
        step::raise_if_cancelled(py)?;
        let (socket, file) = porto_net::unix::listen(&address, backlog, mode)
            .map_err(|error| sockaddr::path_error(&error, &address))?;
        let local = socket.local_addr().map_err(|error| os_error(&error))?;
        let registration = register_on_lane(socket, "a UnixListener");
        let registration = match registration {
            Ok(registration) => registration,
            Err(error) => {
                if let Some(file) = file {
                    file.remove();
                }
                return Err(error);
            }
        };
        Ok(UnixListener {
            registration,
            local,
            file: SocketFile(parking_lot::Mutex::new(file)),
        })
    }
}

/// Connects a Unix stream socket in this run: `porto.net.connect_unix` without the retry
/// loop (see `python/porto/net.py`). `None` when the listener's backlog is full (`EAGAIN`):
/// the caller waits a little and calls again.
#[pyfunction(name = "_connect_unix")]
#[pyo3(signature = (path,))]
fn connect_unix(py: Python<'_>, path: &Bound<'_, PyAny>) -> PyResult<Option<Py<UnixStream>>> {
    #[cfg(not(unix))]
    {
        let _ = (py, path);
        Err(unsupported("connect_unix()"))
    }
    #[cfg(unix)]
    {
        let address = sockaddr::unix_address(path, "connect_unix()")?;
        // A cancelled task must not open a connection it will not use (C1).
        step::raise_if_cancelled(py)?;
        let connected = porto_net::unix::connect(&address)
            .map_err(|error| sockaddr::path_error(&error, &address))?;
        let Some(socket) = connected else {
            return Ok(None);
        };
        Py::new(py, UnixStream::adopt(socket, Some(address))?).map(Some)
    }
}

#[pymethods]
impl UnixListener {
    /// Waits for a connection and accepts it: a `UnixStream`, nonblocking and close-on-exec.
    /// If the process or the system is out of file descriptors it waits 100 ms and tries again,
    /// logging a warning (at most every 10 s) on the `porto.net` logger.
    fn accept(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        submit(
            py,
            &self.registration,
            IoOp::AcceptUnix {
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
            IoOp::AcceptUnix {
                ending_iteration: true,
            },
        )
    }

    /// The path the listener is bound to (`str`), or its name (`bytes`, starting with a NUL
    /// byte) in Linux's abstract namespace.
    #[getter]
    fn sockname(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        sockaddr::to_py(py, &self.local)
    }

    /// The OS file descriptor; the listener still owns it. `ClosedResourceError` once closed.
    fn fileno(&self) -> PyResult<i64> {
        let socket = self
            .registration
            .source()
            .map_err(|error| os_error(&error))?;
        Ok(raw_number(&socket))
    }

    /// Stops listening and removes the socket file this listener created (when it is still
    /// that file: a path somebody replaced is left alone). An `accept()` waiting on the listener
    /// fails with `ClosedResourceError` (an `async for` ends), and connections not accepted yet
    /// are refused or reset by the OS. Closing again does nothing. Closes even when the calling
    /// task is cancelled.
    fn aclose(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let closed = self.registration.close();
        self.file.remove();
        closed.map_err(|error| os_error(&error))?;
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
