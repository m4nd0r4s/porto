//! `SocketStream` (17-networking-tls-dns.md §1.1): a connected stream socket registered with
//! the driver of the run that made it, whose methods return the awaitables of the [`IoOp`]s. It
//! is the Python base class of `porto.net.TcpStream` and `porto.net.UnixStream`, which add what
//! only their kind of socket has (TCP's options, the connect), and so implement
//! `porto.abc.ByteStream` with one body.
//!
//! The stream keeps four things of its own (13-io-driver.md §5, 17-networking-tls-dns.md §1.1):
//!
//! - its receive buffer, the bytes received and not consumed yet: the buffered reads
//!   (`read_exact`, `read_until`, `readline`, `read_to_end`) fill and drain it, and every read
//!   takes buffered bytes before it receives, so what a cancelled read received is never lost;
//! - whether a write was cancelled, or failed with an error that did not end the connection,
//!   after it sent part of its data: the bytes on the wire then stop in the middle of what the
//!   caller meant to send, so later writes raise
//!   `porto.BrokenResourceError` instead of continuing the message from the wrong place;
//! - whether a task is reading it, and whether a task is writing it: a second reader or writer
//!   raises `porto.BusyResourceError` (two writers would interleave their bytes inside one
//!   message, two readers would split what the peer sent).

use std::net::Shutdown;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use socket2::{SockAddr, Socket};

use super::{int_in_range, raw_number};
use crate::driver::ready::{ready, ready_none};
use crate::driver::step;
use crate::errors::{BrokenResourceError, BusyResourceError};
use crate::io::buffer_view::{PyBufferMut, PyBufferRef};
use crate::io::buffered::{BufferedRead, ReadMode, SharedRecvBuffer};
use crate::io::handle::register_on_lane;
use crate::io::op::{Claim, IoOp, IoRegistration, MidWrite, os_error, submit, submit_claimed};
use crate::io::recv_buffer::RecvBuffer;
use crate::io::sockaddr;

/// The most bytes one `read` asks for by default.
const DEFAULT_MAX: usize = 65536;

/// A connected stream socket: what `TcpStream` and `UnixStream` share.
#[pyclass(frozen, subclass, module = "porto._core", name = "_SocketStream")]
pub(crate) struct SocketStream {
    registration: Arc<IoRegistration>,
    /// Set when a write ended after sending part of its data and the connection lives on.
    mid_write: MidWrite,
    /// The bytes received and not consumed yet.
    incoming: SharedRecvBuffer,
    /// Held by the operation of the task reading the stream.
    reading: Arc<AtomicBool>,
    /// Held by the operation of the task writing the stream.
    writing: Arc<AtomicBool>,
    /// The peer's address, known when the stream was accepted, else read on first use.
    peer: OnceLock<SockAddr>,
    /// This end's address, read on first use.
    local: OnceLock<SockAddr>,
}

impl SocketStream {
    /// A buffered read of `mode`, claiming the stream's one reader.
    fn buffered_read(&self, py: Python<'_>, call: &str, mode: ReadMode) -> PyResult<Py<PyAny>> {
        let claim = self.claim_reader(call)?;
        let read = BufferedRead::new(Arc::clone(&self.incoming), mode);
        submit_claimed(
            py,
            &self.registration,
            IoOp::ReadBuffered(read),
            Some(claim),
        )
    }

    /// A stream over `socket` (nonblocking), registered with the driver of the run on this
    /// thread; `what` names it for the error outside a run. `peer` is the peer's address, if
    /// the accept gave it.
    pub(crate) fn adopt(socket: Socket, what: &str, peer: Option<SockAddr>) -> PyResult<Self> {
        Ok(Self {
            registration: register_on_lane(socket, what)?,
            mid_write: Arc::new(AtomicBool::new(false)),
            incoming: Arc::new(Mutex::new(RecvBuffer::default())),
            reading: Arc::new(AtomicBool::new(false)),
            writing: Arc::new(AtomicBool::new(false)),
            peer: peer.map_or_else(OnceLock::new, OnceLock::from),
            local: OnceLock::new(),
        })
    }

    pub(crate) fn submit(&self, py: Python<'_>, op: IoOp) -> PyResult<Py<PyAny>> {
        submit(py, &self.registration, op)
    }

    /// The registration the stream's operations run on.
    pub(crate) fn registration(&self) -> &Arc<IoRegistration> {
        &self.registration
    }

    /// The claim on the stream's one reader, or `BusyResourceError` if another task holds it.
    fn claim_reader(&self, call: &str) -> PyResult<Claim> {
        Claim::acquire(&self.reading).ok_or_else(|| busy(call, "reading from", "read"))
    }

    /// The claim on the stream's one writer, or `BusyResourceError` if another task holds it.
    fn claim_writer(&self, call: &str) -> PyResult<Claim> {
        Claim::acquire(&self.writing).ok_or_else(|| busy(call, "writing to", "write"))
    }

    /// Raises `BrokenResourceError` if an earlier write left the stream mid-message (cancelled, or
    /// failed with an error that did not end the connection).
    fn ensure_not_mid_write(&self) -> PyResult<()> {
        if self.mid_write.load(Ordering::Acquire) {
            return Err(BrokenResourceError::new_err(
                "porto: this stream was left mid-write: an earlier write was cancelled, or failed \
                 with an error that did not end the connection, after it had sent part of its \
                 data, so the bytes on the wire stop in the middle of a message and anything \
                 written now would corrupt it; close the stream (await stream.aclose()) and \
                 open a new connection",
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

fn busy(call: &str, doing: &str, verb: &str) -> PyErr {
    BusyResourceError::new_err(format!(
        "porto: {call} was called while another task is already {doing} this stream; a stream \
         takes one reader and one writer at a time (concurrent {verb}s would interleave or split \
         the data): let one task {verb}, or serialize the tasks with a porto.Lock"
    ))
}

/// The separator of `read_until`: a bytes-like object with at least one byte.
fn separator(sep: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let view = PyBufferRef::new(sep, "read_until()'s sep")?;
    let separator = view.bytes().to_vec();
    if separator.is_empty() {
        return Err(PyValueError::new_err(
            "porto: read_until()'s sep is empty, which matches everywhere; pass the bytes that \
             end a message (b\"\\n\", b\"\\r\\n\")",
        ));
    }
    Ok(separator)
}

/// The `limit` of a search for a separator of `separator_len` bytes: at least that, because the
/// separator has to fit.
fn search_limit(
    limit: Option<&Bound<'_, PyAny>>,
    call: &str,
    separator_len: usize,
) -> PyResult<usize> {
    let Some(limit) = limit else {
        return Ok(DEFAULT_MAX);
    };
    let minimum = i64::try_from(separator_len).unwrap_or(i64::MAX);
    let hint = format!(" (the separator is {separator_len} bytes: it has to fit)");
    let limit = int_in_range(limit, call, "limit", minimum..=i64::MAX, &hint)?;
    Ok(usize::try_from(limit).unwrap_or(usize::MAX))
}

/// `max`, the optional argument of a read, as the length of a receive: at least 1, because a
/// receive of 0 bytes returns 0, which reads as the end of the stream.
fn receive_length(max: Option<&Bound<'_, PyAny>>, call: &str) -> PyResult<usize> {
    let Some(max) = max else {
        return Ok(DEFAULT_MAX);
    };
    let hint = " (a read of 0 bytes would look like the end of the stream; ask for at least one \
                byte)";
    let max = int_in_range(max, call, "max", 1..=i64::MAX, hint)?;
    Ok(usize::try_from(max).unwrap_or(usize::MAX))
}

#[pymethods]
impl SocketStream {
    /// Receives at most `max` bytes: `bytes`, empty at the end of the stream (the peer closed
    /// or shut down its writing side).
    #[pyo3(signature = (max = None, /))]
    fn read(&self, py: Python<'_>, max: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let max = receive_length(max, "read()")?;
        let claim = self.claim_reader("read()")?;
        let buffered = Some(Arc::clone(&self.incoming));
        submit_claimed(
            py,
            &self.registration,
            IoOp::Read { max, buffered },
            Some(claim),
        )
    }

    /// Receives at most `max` bytes into a `porto.Buffer` from the lane's pool, without the
    /// copy into `bytes`: the buffer, empty at the end of the stream.
    #[pyo3(signature = (max = None, /))]
    fn read_buffer(&self, py: Python<'_>, max: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let max = receive_length(max, "read_buffer()")?;
        let claim = self.claim_reader("read_buffer()")?;
        submit_claimed(
            py,
            &self.registration,
            IoOp::ReadBuffer {
                max,
                buffered: Some(Arc::clone(&self.incoming)),
            },
            Some(claim),
        )
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
        let claim = self.claim_reader("read_into()")?;
        let buffered = Some(Arc::clone(&self.incoming));
        submit_claimed(
            py,
            &self.registration,
            IoOp::ReadInto { buf, buffered },
            Some(claim),
        )
    }

    /// Receives exactly `n` bytes: `bytes`. If the stream ends first, `IncompleteReadError` with
    /// the bytes that arrived. Bytes received by a cancelled read stay buffered.
    fn read_exact(&self, py: Python<'_>, n: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let count = int_in_range(
            n,
            "read_exact()",
            "n",
            0..=i64::MAX,
            " (the number of bytes)",
        )?;
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        self.buffered_read(py, "read_exact()", ReadMode::Exact(count))
    }

    /// Receives up to and including the first `sep`: `bytes`. `LimitOverrunError` if `sep` does
    /// not end within the first `limit` bytes (they stay buffered); `IncompleteReadError` if the
    /// stream ends first.
    #[pyo3(signature = (sep, *, limit = None))]
    fn read_until(
        &self,
        py: Python<'_>,
        sep: &Bound<'_, PyAny>,
        limit: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let separator = separator(sep)?;
        let limit = search_limit(limit, "read_until()", separator.len())?;
        self.buffered_read(
            py,
            "read_until()",
            ReadMode::Until {
                call: "read_until()",
                separator,
                limit,
            },
        )
    }

    /// `read_until(b"\n", limit=limit)`: one line, its newline included.
    #[pyo3(signature = (limit = None))]
    fn readline(&self, py: Python<'_>, limit: Option<&Bound<'_, PyAny>>) -> PyResult<Py<PyAny>> {
        let limit = search_limit(limit, "readline()", 1)?;
        self.buffered_read(
            py,
            "readline()",
            ReadMode::Until {
                call: "readline()",
                separator: b"\n".to_vec(),
                limit,
            },
        )
    }

    /// Receives until the end of the stream: `bytes`. `LimitOverrunError` if the stream sends
    /// more than `limit` bytes (they stay buffered).
    #[pyo3(signature = (*, limit))]
    fn read_to_end(&self, py: Python<'_>, limit: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let limit = int_in_range(
            limit,
            "read_to_end()",
            "limit",
            0..=i64::MAX,
            " (the most bytes to return)",
        )?;
        let limit = usize::try_from(limit).unwrap_or(usize::MAX);
        self.buffered_read(py, "read_to_end()", ReadMode::ToEnd { limit })
    }

    /// Sends all of `data`, waiting while the socket's buffers are full: `None` once the last
    /// byte went out. A cancellation, or an error that does not end the connection, after part of
    /// it was sent leaves the stream broken.
    fn write(&self, py: Python<'_>, data: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let data = PyBufferRef::new(data, "write()'s data")?;
        let claim = self.claim_writer("write()")?;
        self.ensure_not_mid_write()?;
        submit_claimed(
            py,
            &self.registration,
            IoOp::Write {
                data,
                off: 0,
                mid_write: Some(Arc::clone(&self.mid_write)),
            },
            Some(claim),
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
        let claim = self.claim_writer("writev()")?;
        self.ensure_not_mid_write()?;
        submit_claimed(
            py,
            &self.registration,
            IoOp::write_v(views, Some(Arc::clone(&self.mid_write))),
            Some(claim),
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

    /// The peer's address, as the `socket` module shows it: `(host, port)` for IPv4, `(host,
    /// port, flowinfo, scope_id)` for IPv6, a path for a Unix socket.
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
}

impl SocketStream {
    /// Closes the registration (see `aclose`).
    pub(crate) fn close(&self) -> PyResult<()> {
        self.registration.close().map_err(|error| os_error(&error))
    }
}
