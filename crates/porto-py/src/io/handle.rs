//! `_IoHandle`: a socket registered with the running lane's driver, whose methods return the
//! awaitables of the [`IoOp`]s. A private test hook until the public socket types arrive
//! (`TcpStream`, `TcpListener`, `UdpSocket`, `porto.lowlevel`), which wrap the same
//! registration and [`submit`].

use std::sync::Arc;

use porto_io::Interest;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use socket2::Socket;

use super::buffer_view::{PyBufferMut, PyBufferRef};
use super::op::{IoOp, IoRegistration, os_error, submit};
use super::sockaddr;
use crate::driver::lane;

/// The most bytes one receive asks for by default.
const DEFAULT_MAX: usize = 65536;

/// A nonblocking socket registered with the driver of the run that made it.
#[pyclass(frozen, module = "porto._core", name = "_IoHandle")]
pub(crate) struct IoHandle {
    registration: Arc<IoRegistration>,
}

impl IoHandle {
    /// Registers `socket`, which MUST be nonblocking, with the driver of the run on this
    /// thread.
    pub(crate) fn adopt(py: Python<'_>, socket: Socket) -> PyResult<Py<Self>> {
        let Some(driver) = lane::current().and_then(|handle| handle.driver().cloned()) else {
            return Err(PyRuntimeError::new_err(
                "porto: an I/O handle must be made inside a running porto runtime; create it \
                 from a task (inside porto.run)",
            ));
        };
        let registration =
            IoRegistration::new(driver, socket, Interest::READABLE | Interest::WRITABLE)
                .map_err(|error| os_error(&error))?;
        Py::new(
            py,
            Self {
                registration: Arc::new(registration),
            },
        )
    }

    fn submit(&self, py: Python<'_>, op: IoOp) -> PyResult<Py<PyAny>> {
        submit(py, &self.registration, op)
    }
}

/// A new socket owning a duplicate of `fd`, nonblocking.
fn duplicate(fd: i64) -> PyResult<Socket> {
    let invalid = || {
        PyValueError::new_err(format!(
            "porto: {fd} is not an open socket; pass the fileno() of a socket"
        ))
    };
    #[cfg(unix)]
    let borrowed = {
        use std::os::fd::FromRawFd;
        let fd = std::os::fd::RawFd::try_from(fd).map_err(|_| invalid())?;
        if fd < 0 {
            return Err(invalid());
        }
        // SAFETY: wrapped without ownership (never dropped, see `ManuallyDrop`), only to
        // duplicate it; an fd that is not open makes the duplication fail.
        std::mem::ManuallyDrop::new(unsafe { Socket::from_raw_fd(fd) })
    };
    #[cfg(windows)]
    let borrowed = {
        use std::os::windows::io::FromRawSocket;
        let socket = std::os::windows::io::RawSocket::try_from(fd).map_err(|_| invalid())?;
        // SAFETY: as above.
        std::mem::ManuallyDrop::new(unsafe { Socket::from_raw_socket(socket) })
    };
    let socket = borrowed.try_clone().map_err(|error| os_error(&error))?;
    socket
        .set_nonblocking(true)
        .map_err(|error| os_error(&error))?;
    Ok(socket)
}

#[pymethods]
impl IoHandle {
    /// Registers a duplicate of the socket `fd` (the caller keeps its own), made nonblocking.
    #[new]
    fn new(py: Python<'_>, fd: i64) -> PyResult<Py<Self>> {
        Self::adopt(py, duplicate(fd)?)
    }

    /// Receives at most `max` bytes: `bytes`, empty at end of stream.
    #[pyo3(signature = (max = DEFAULT_MAX, /))]
    fn read(&self, py: Python<'_>, max: usize) -> PyResult<Py<PyAny>> {
        self.submit(py, IoOp::Read { max })
    }

    /// Receives at most `max` bytes into a `porto.Buffer` from the lane's pool, without the
    /// copy into `bytes`: the buffer, empty at end of stream.
    #[pyo3(signature = (max = DEFAULT_MAX, /))]
    fn read_buffer(&self, py: Python<'_>, max: usize) -> PyResult<Py<PyAny>> {
        self.submit(py, IoOp::ReadBuffer { max })
    }

    /// Receives into `buffer`, a writable bytes-like object: the number of bytes received.
    fn read_into(&self, py: Python<'_>, buffer: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let buf = PyBufferMut::new(buffer, "read_into()'s buffer")?;
        self.submit(py, IoOp::ReadInto { buf })
    }

    /// Sends all of `data`: `None` once the last byte went out.
    fn write(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let data = PyBufferRef::new(data, "write()'s data")?;
        self.submit(py, IoOp::Write { data, off: 0 })
    }

    /// Accepts a connection: `(_IoHandle, address)`.
    fn accept(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.submit(py, IoOp::Accept)
    }

    /// Connects to `address` without blocking: `None` once connected, or the error.
    fn connect(&self, py: Python<'_>, address: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let address = sockaddr::from_py(address)?;
        match self
            .registration
            .source()
            .and_then(|socket| socket.connect(&address))
        {
            Ok(()) => {}
            // Started: Unix reports `EINPROGRESS`, Windows `WSAEWOULDBLOCK`.
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.raw_os_error() == EINPROGRESS => {}
            Err(error) => return Err(os_error(&error)),
        }
        self.submit(py, IoOp::Connect)
    }

    /// Receives one datagram of at most `max` bytes: `(bytes, address)`.
    #[pyo3(signature = (max = DEFAULT_MAX, /))]
    fn recv_from(&self, py: Python<'_>, max: usize) -> PyResult<Py<PyAny>> {
        self.submit(py, IoOp::RecvFrom { max })
    }

    /// Sends `data` as one datagram to `address`.
    fn send_to(
        &self,
        py: Python<'_>,
        data: &Bound<'_, PyAny>,
        address: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let data = PyBufferRef::new(data, "send_to()'s data")?;
        let addr = sockaddr::from_py(address)?;
        self.submit(py, IoOp::SendTo { data, addr })
    }

    /// Waits until the socket is readable.
    fn wait_readable(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.submit(py, IoOp::WaitReadable)
    }

    /// Waits until the socket is writable.
    fn wait_writable(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.submit(py, IoOp::WaitWritable)
    }

    /// Closes the socket: pending operations fail with `OSError(EBADF)`, and the handle is
    /// closed once the operations using it are done. Closing again does nothing.
    fn close(&self) -> PyResult<()> {
        self.registration.close().map_err(|error| os_error(&error))
    }

    /// Whether `close()` was called.
    #[getter]
    fn closed(&self) -> bool {
        self.registration.is_closed()
    }
}

/// `EINPROGRESS`, which a nonblocking connect returns once started (`ErrorKind::InProgress` is
/// unstable).
#[cfg(any(target_os = "linux", target_os = "android"))]
const EINPROGRESS: Option<i32> = Some(115);
#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
const EINPROGRESS: Option<i32> = Some(36);
#[cfg(not(unix))]
const EINPROGRESS: Option<i32> = None;
