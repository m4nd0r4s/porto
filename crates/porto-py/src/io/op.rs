//! [`IoOp`]: the operations an `Io` trap carries, and [`submit`], which runs one
//! (13-io-driver.md §5, 10-architecture-overview.md §4).
//!
//! An operation is attempted at once, nonblocking (the fast path): if it succeeds, the await
//! gets a ready awaitable and the scheduler is never involved. If it hits `WouldBlock`, the
//! await gets one `Io` trap, whose wait is the same operation: it waits for the registration's
//! readiness, attempts the operation again, and on `WouldBlock` clears the readiness it consumed
//! and waits again. The trap resumes the task with the result.
//!
//! T3: an operation consumes data only in the attempt that completes it, and that attempt
//! resolves the wait at once, so dropping a pending wait (a cancellation) never loses data; it
//! unlinks the waiter it registered.

use std::io;
use std::sync::Arc;

use porto_io::{ClosedResource, Interest, Registration};
use pyo3::exceptions::{PyMemoryError, PyOSError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use socket2::{SockAddr, Socket};

use super::buffer_view::{PyBufferMut, PyBufferRef};
use super::handle::IoHandle;
use super::sockaddr;
use crate::driver::pending::IoWait;
use crate::driver::pytask::Resume;
use crate::driver::ready::ready;
use crate::driver::step;
use crate::driver::trap::{self, Trap, TrapKind};

/// A registered socket that operations run on.
pub(crate) type IoRegistration = Registration<Socket>;

/// `MSG_NOSIGNAL`: a send to a peer that closed fails with `EPIPE` instead of raising
/// `SIGPIPE` (Linux; elsewhere Python ignores `SIGPIPE` from startup).
#[cfg(any(target_os = "linux", target_os = "android"))]
const SEND_FLAGS: std::ffi::c_int = 0x4000;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const SEND_FLAGS: std::ffi::c_int = 0;

/// An operation on a registered socket, and its state between attempts.
pub(crate) enum IoOp {
    /// Receive at most `max` bytes → `bytes` (empty at end of stream).
    Read { max: usize },
    /// Receive into a writable buffer → the number of bytes received.
    ReadInto { buf: PyBufferMut },
    /// Send all of `data`, from `off` on → `None`. Sends what it can at each attempt.
    Write { data: PyBufferRef, off: usize },
    /// Accept a connection → `(socket, address)`.
    Accept,
    /// Finish a nonblocking connect → `None`, or the connect's error (`SO_ERROR`).
    Connect,
    /// Receive one datagram of at most `max` bytes → `(bytes, address)`.
    RecvFrom { max: usize },
    /// Send one datagram to `addr` → `None`.
    SendTo { data: PyBufferRef, addr: SockAddr },
    /// Wait until the socket is readable → `None` (raw readiness, `porto.lowlevel`).
    WaitReadable,
    /// Wait until the socket is writable → `None`.
    WaitWritable,
}

/// What a completed operation produced, before it becomes a Python value.
pub(crate) enum Completed {
    /// Bytes received.
    Bytes(Vec<u8>),
    /// A byte count.
    Count(usize),
    /// Nothing to return.
    Done,
    /// An accepted connection and its peer's address.
    Accepted(Socket, SockAddr),
    /// A datagram and its sender's address.
    Received(Vec<u8>, SockAddr),
}

impl IoOp {
    /// The readiness the operation waits for.
    pub(crate) fn interest(&self) -> Interest {
        match self {
            Self::Read { .. }
            | Self::ReadInto { .. }
            | Self::Accept
            | Self::RecvFrom { .. }
            | Self::WaitReadable => Interest::READABLE,
            Self::Write { .. } | Self::Connect | Self::SendTo { .. } | Self::WaitWritable => {
                Interest::WRITABLE
            }
        }
    }

    /// Whether the operation is the wait for readiness itself, with no syscall to attempt.
    fn waits_only(&self) -> bool {
        matches!(self, Self::WaitReadable | Self::WaitWritable)
    }

    /// Attempts the operation once, nonblocking (retrying on `EINTR`). `WouldBlock` when the
    /// socket is not ready; a partial send keeps its progress for the next attempt.
    pub(crate) fn attempt(&mut self, socket: &Socket) -> io::Result<Completed> {
        loop {
            match self.attempt_once(socket) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }

    fn attempt_once(&mut self, socket: &Socket) -> io::Result<Completed> {
        match self {
            Self::Read { max } => {
                let mut bytes = receive_buffer(*max)?;
                let received = socket.recv(bytes.spare_capacity_mut())?;
                // SAFETY: `recv` initialized the first `received` bytes.
                unsafe { bytes.set_len(received) };
                Ok(Completed::Bytes(bytes))
            }
            Self::ReadInto { buf } => socket.recv(buf.spare()).map(Completed::Count),
            Self::Write { data, off } => {
                let bytes = data.bytes();
                while *off < bytes.len() {
                    match socket.send_with_flags(&bytes[*off..], SEND_FLAGS)? {
                        0 => return Err(io::ErrorKind::WriteZero.into()),
                        sent => *off += sent,
                    }
                }
                Ok(Completed::Done)
            }
            Self::Accept => {
                let (connection, address) = socket.accept()?;
                connection.set_nonblocking(true)?;
                Ok(Completed::Accepted(connection, address))
            }
            Self::Connect => {
                if let Some(error) = socket.take_error()? {
                    return Err(error);
                }
                match socket.peer_addr() {
                    Ok(_) => Ok(Completed::Done),
                    // Still connecting: writable readiness comes when it is done.
                    Err(error) if error.kind() == io::ErrorKind::NotConnected => {
                        Err(io::ErrorKind::WouldBlock.into())
                    }
                    Err(error) => Err(error),
                }
            }
            Self::RecvFrom { max } => {
                let mut bytes = receive_buffer(*max)?;
                let (received, address) = socket.recv_from(bytes.spare_capacity_mut())?;
                // SAFETY: `recv_from` initialized the first `received` bytes.
                unsafe { bytes.set_len(received) };
                Ok(Completed::Received(bytes, address))
            }
            Self::SendTo { data, addr } => socket
                .send_to_with_flags(data.bytes(), addr, SEND_FLAGS)
                .map(|_| Completed::Done),
            Self::WaitReadable | Self::WaitWritable => Ok(Completed::Done),
        }
    }
}

/// An empty buffer of capacity `max` to receive into; `OutOfMemory` if it cannot be allocated.
fn receive_buffer(max: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(max)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    Ok(bytes)
}

impl Completed {
    /// The Python value of the result.
    fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(match self {
            Self::Bytes(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
            Self::Count(count) => count.into_pyobject(py)?.into_any().unbind(),
            Self::Done => py.None(),
            Self::Accepted(connection, address) => {
                let connection = IoHandle::adopt(py, connection)?;
                (connection, sockaddr::to_py(py, &address)?)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind()
            }
            Self::Received(bytes, address) => {
                (PyBytes::new(py, &bytes), sockaddr::to_py(py, &address)?)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind()
            }
        })
    }
}

/// `error` as a Python exception: an `OSError` with its errno (so Python picks the subclass,
/// `ConnectionResetError` and so on), a `MemoryError` for a receive buffer that could not be
/// allocated.
pub(crate) fn os_error(error: &io::Error) -> PyErr {
    if let Some(closed) = ClosedResource::of(error) {
        return PyOSError::new_err((ClosedResource::ERRNO, format!("porto: {closed}")));
    }
    if error.kind() == io::ErrorKind::OutOfMemory {
        return PyMemoryError::new_err(
            "porto: could not allocate the receive buffer; ask for fewer bytes (a smaller max)",
        );
    }
    match error.raw_os_error() {
        Some(errno) => PyOSError::new_err((errno, error.to_string())),
        None => PyOSError::new_err(error.to_string()),
    }
}

/// Runs `op` on `registration` and returns what the caller's `await` gets: a ready awaitable
/// if it completed at once, else an `Io` trap that completes it.
///
/// Raises `Cancelled` before attempting anything while the running task is cancelled, so a
/// fast path never consumes data it cannot deliver. A task out of budget gets the trap without
/// an attempt: the driver yields before polling its wait (T2).
pub(crate) fn submit(
    py: Python<'_>,
    registration: &Arc<IoRegistration>,
    mut op: IoOp,
) -> PyResult<Py<PyAny>> {
    if !op.waits_only() && step::proceed(py)? {
        match registration.attempt_io(op.interest(), |socket| op.attempt(socket)) {
            Ok(completed) => return ready(py, completed.into_py(py)?),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(os_error(&error)),
        }
    }
    let wait = IoWait::new(wait_for(Arc::clone(registration), op));
    Ok(trap::fast_new::object(py, Trap::new(TrapKind::Io(wait)))?.into_any())
}

/// The slow path of `op`: wait for readiness, attempt, repeat until it does not hit
/// `WouldBlock`. Dropping it while pending unlinks its waiter and consumes nothing (T3).
async fn wait_for(registration: Arc<IoRegistration>, mut op: IoOp) -> Resume {
    let interest = op.interest();
    let result = if op.waits_only() {
        registration
            .readiness(interest)
            .await
            .map(|_| Completed::Done)
    } else {
        registration
            .async_io(interest, |socket| op.attempt(socket))
            .await
    };
    // SAFETY: an `Io` wait is polled by the step loop, on its lane, attached.
    let py = unsafe { Python::assume_attached() };
    let value = result
        .map_err(|error| os_error(&error))
        .and_then(|completed| completed.into_py(py));
    // The views the operation holds are released here, attached (P1).
    drop(op);
    match value {
        Ok(value) => Resume::Value(value),
        Err(error) => Resume::Throw(error.into_value(py)),
    }
}
