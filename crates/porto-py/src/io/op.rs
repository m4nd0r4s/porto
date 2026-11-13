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

use std::cell::RefCell;
use std::future::{Future, poll_fn};
use std::io::{self, IoSlice};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Instant;

use porto_core::time::{Deadline, sleep_until};
use porto_io::{ClosedResource, Interest, Registration};
use porto_net::tcp;
use pyo3::exceptions::{PyMemoryError, PyOSError, PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use socket2::{SockAddr, Socket};

use super::buffer::Buffer;
use super::buffer_view::{PyBufferMut, PyBufferRef};
use super::buffered::{BufferedRead, Outcome, SharedRecvBuffer, YieldLane};
use super::handle::IoHandle;
use super::pool::PooledBytes;
use super::sockaddr;
use crate::api::net::TcpStream;
use crate::driver::pending::IoWait;
use crate::driver::pytask::Resume;
use crate::driver::ready::ready;
use crate::driver::trap::{self, Trap, TrapKind};
use crate::driver::{lane, step};
use crate::errors::ClosedResourceError;

/// A registered socket that operations run on.
pub(crate) type IoRegistration = Registration<Socket>;

/// `MSG_NOSIGNAL`: a send to a peer that closed fails with `EPIPE` instead of raising
/// `SIGPIPE` (Linux; elsewhere Python ignores `SIGPIPE` from startup).
#[cfg(any(target_os = "linux", target_os = "android"))]
const SEND_FLAGS: std::ffi::c_int = 0x4000;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const SEND_FLAGS: std::ffi::c_int = 0;

/// Whether a stream was left mid-write: set when a write ends after sending part of its data
/// (13-io-driver.md §5), read by the stream's later writes.
pub(crate) type MidWrite = Arc<AtomicBool>;

/// The most buffers one vectored send hands the kernel (`IOV_MAX` is 1024 on Linux and macOS).
const MAX_IOVECS: usize = 64;

/// The right to be the one task reading (or writing) a stream: set when the operation is made
/// and cleared when it is dropped, whether it completed, failed or was cancelled. A second
/// operation of the same direction while it is held is refused ([`Claim::acquire`]), because
/// two writers would interleave their bytes and two readers would split what the peer sent.
pub(crate) struct Claim(Arc<AtomicBool>);

impl Claim {
    /// Takes the claim on `flag`, or `None` if another operation holds it.
    pub(crate) fn acquire(flag: &Arc<AtomicBool>) -> Option<Self> {
        flag.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .ok()
            .map(|_| Self(Arc::clone(flag)))
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// An operation on a registered socket, and its state between attempts.
pub(crate) enum IoOp {
    /// Receive at most `max` bytes → `bytes` (empty at end of stream). A stream's receive
    /// buffer (`buffered`), if it has bytes, answers without the socket.
    Read {
        max: usize,
        buffered: Option<SharedRecvBuffer>,
    },
    /// Receive at most `max` bytes into a pooled buffer → `porto.Buffer` (no `bytes` copy, unless
    /// the bytes come out of the stream's receive buffer).
    ReadBuffer {
        max: usize,
        buffered: Option<SharedRecvBuffer>,
    },
    /// Receive into a writable buffer → the number of bytes received.
    ReadInto {
        buf: PyBufferMut,
        buffered: Option<SharedRecvBuffer>,
    },
    /// `read_exact`, `read_until`, `readline` or `read_to_end`: receive into the stream's
    /// buffer until the bytes asked for are there → `bytes`.
    ReadBuffered(BufferedRead),
    /// Send all of `data`, from `off` on → `None`. Sends what it can at each attempt. If it
    /// ends having sent only part of `data` (cancelled, or failed with an error that did not end the
    /// connection), it sets `mid_write`.
    Write {
        data: PyBufferRef,
        off: usize,
        mid_write: Option<MidWrite>,
    },
    /// Send all of `buffers`, in as few vectored sends as it can → `None`. Sets `mid_write` like
    /// `Write`. `index` and `offset` are where the next send starts (the buffer, and the byte
    /// in it), `written` how many bytes went out of `total`.
    WriteV {
        buffers: Vec<PyBufferRef>,
        index: usize,
        offset: usize,
        written: usize,
        total: usize,
        mid_write: Option<MidWrite>,
    },
    /// Accept a connection → `(socket, address)`.
    Accept,
    /// Accept a TCP connection → `porto.net.TcpStream`; with `ending_iteration`, a listener
    /// closed under it ends an `async for` (`StopAsyncIteration`) instead of raising.
    AcceptTcp { ending_iteration: bool },
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
    /// How a buffered read ended (`read_exact` and the like).
    Buffered(Outcome),
    /// Bytes received into a pooled buffer.
    Buffer(PooledBytes),
    /// A byte count.
    Count(usize),
    /// Nothing to return.
    Done,
    /// An accepted connection and its peer's address.
    Accepted(Socket, SockAddr),
    /// An accepted TCP connection and its peer's address.
    AcceptedTcp(Socket, SockAddr),
    /// A datagram and its sender's address.
    Received(Vec<u8>, SockAddr),
}

impl IoOp {
    /// A vectored send of `buffers`.
    pub(crate) fn write_v(buffers: Vec<PyBufferRef>, mid_write: Option<MidWrite>) -> Self {
        let total = buffers.iter().map(|buffer| buffer.bytes().len()).sum();
        Self::WriteV {
            buffers,
            index: 0,
            offset: 0,
            written: 0,
            total,
            mid_write,
        }
    }

    /// The operation ended: it completed (`error` is `None`) or failed with `error`.
    ///
    /// A write that ended after sending only part of its data leaves the stream mid-message
    /// (the flag [`Drop`] sets) unless its error is one the connection cannot survive, which
    /// every later write fails with again ([`is_sticky_write_error`]). Any other error (a
    /// transient `ENOBUFS`, `WriteZero`) leaves the connection open at a message boundary the
    /// caller never reached, so the stream stays broken like after a cancellation.
    fn finish(&mut self, error: Option<&io::Error>) {
        let sticky = error.is_none_or(is_sticky_write_error);
        if sticky && let Self::Write { mid_write, .. } | Self::WriteV { mid_write, .. } = self {
            *mid_write = None;
        }
    }

    /// The readiness the operation waits for.
    pub(crate) fn interest(&self) -> Interest {
        match self {
            Self::Read { .. }
            | Self::ReadBuffer { .. }
            | Self::ReadInto { .. }
            | Self::ReadBuffered(_)
            | Self::Accept
            | Self::AcceptTcp { .. }
            | Self::RecvFrom { .. }
            | Self::WaitReadable => Interest::READABLE,
            Self::Write { .. }
            | Self::WriteV { .. }
            | Self::Connect
            | Self::SendTo { .. }
            | Self::WaitWritable => Interest::WRITABLE,
        }
    }

    /// Whether the operation is the wait for readiness itself, with no syscall to attempt.
    fn waits_only(&self) -> bool {
        matches!(self, Self::WaitReadable | Self::WaitWritable)
    }

    /// Completes the operation from bytes the stream already buffered, without the socket: the
    /// result, or `None` when it has to receive. Such a read must not wait for readiness: the
    /// bytes it needs are in the buffer, and the socket may never become readable again.
    pub(crate) fn complete_from_buffer(&mut self) -> Option<io::Result<Completed>> {
        match self {
            Self::Read {
                max,
                buffered: Some(buffered),
            } => {
                let mut buffered = buffered.lock();
                (!buffered.is_empty()).then(|| Ok(Completed::Bytes(buffered.take(*max))))
            }
            Self::ReadBuffer {
                max,
                buffered: Some(buffered),
            } => {
                let mut buffered = buffered.lock();
                (!buffered.is_empty()).then(|| buffered.take_pooled(*max).map(Completed::Buffer))
            }
            Self::ReadInto {
                buf,
                buffered: Some(buffered),
            } => {
                let mut buffered = buffered.lock();
                (!buffered.is_empty())
                    .then(|| Ok(Completed::Count(buffered.take_into(buf.spare()))))
            }
            Self::ReadBuffered(read) => read
                .complete_from_buffer()
                .map(|outcome| Ok(Completed::Buffered(outcome))),
            _ => None,
        }
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
        if let Some(completed) = self.complete_from_buffer() {
            return completed;
        }
        match self {
            Self::ReadBuffered(read) => read.attempt(socket).map(Completed::Buffered),
            Self::Read { max, .. } => {
                let mut bytes = receive_buffer(*max)?;
                let received = socket.recv(bytes.spare_capacity_mut())?;
                // SAFETY: `recv` initialized the first `received` bytes.
                unsafe { bytes.set_len(received) };
                Ok(Completed::Bytes(bytes))
            }
            Self::ReadBuffer { max, .. } => {
                let mut bytes = PooledBytes::with_capacity(*max)?;
                let spare = &mut bytes.vec_mut().spare_capacity_mut()[..*max];
                let received = socket.recv(spare)?;
                // SAFETY: `recv` initialized the first `received` bytes.
                unsafe { bytes.vec_mut().set_len(received) };
                Ok(Completed::Buffer(bytes))
            }
            Self::ReadInto { buf, .. } => socket.recv(buf.spare()).map(Completed::Count),
            Self::Write { data, off, .. } => {
                let bytes = data.bytes();
                while *off < bytes.len() {
                    match socket.send_with_flags(&bytes[*off..], SEND_FLAGS)? {
                        0 => return Err(io::ErrorKind::WriteZero.into()),
                        sent => *off += sent,
                    }
                }
                Ok(Completed::Done)
            }
            Self::WriteV {
                buffers,
                index,
                offset,
                written,
                total,
                ..
            } => {
                while *written < *total {
                    let mut slices = [IoSlice::new(&[]); MAX_IOVECS];
                    let mut count = 0;
                    let mut first = *offset;
                    for buffer in &buffers[*index..] {
                        if count == MAX_IOVECS {
                            break;
                        }
                        let rest = &buffer.bytes()[first..];
                        first = 0;
                        if !rest.is_empty() {
                            slices[count] = IoSlice::new(rest);
                            count += 1;
                        }
                    }
                    let sent = socket.send_vectored_with_flags(&slices[..count], SEND_FLAGS)?;
                    if sent == 0 {
                        return Err(io::ErrorKind::WriteZero.into());
                    }
                    *written += sent;
                    // Move the start past what went out.
                    let mut left = sent;
                    while left > 0 {
                        let room = buffers[*index].bytes().len() - *offset;
                        if left >= room {
                            left -= room;
                            *index += 1;
                            *offset = 0;
                        } else {
                            *offset += left;
                            left = 0;
                        }
                    }
                }
                Ok(Completed::Done)
            }
            Self::Accept => {
                let (connection, address) = tcp::accept(socket)?;
                Ok(Completed::Accepted(connection, address))
            }
            Self::AcceptTcp { .. } => {
                let (connection, address) = tcp::accept(socket)?;
                Ok(Completed::AcceptedTcp(connection, address))
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

/// Whether `error`, from a send, means the connection is gone for good, so that every later
/// send fails again (the peer reset it, it timed out, it is not connected, it was closed or
/// shut down). Anything else may be transient (`ENOBUFS`, `ENOMEM`, `WriteZero`).
fn is_sticky_write_error(error: &io::Error) -> bool {
    ClosedResource::is(error)
        || matches!(
            error.kind(),
            io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::NotConnected
                | io::ErrorKind::TimedOut
        )
}

/// An empty buffer of capacity `max` to receive into; `OutOfMemory` if it cannot be allocated.
fn receive_buffer(max: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(max)
        .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
    Ok(bytes)
}

impl Drop for IoOp {
    fn drop(&mut self) {
        let (mid_write, sent, total) = match self {
            Self::Write {
                data,
                off,
                mid_write: Some(mid_write),
            } => (mid_write, *off, data.bytes().len()),
            Self::WriteV {
                written,
                total,
                mid_write: Some(mid_write),
                ..
            } => (mid_write, *written, *total),
            _ => return,
        };
        // Dropped with part of the data out and the rest never to follow (a cancellation, or
        // an error the connection survives, see `finish`): the stream's bytes now stop in the
        // middle of what the caller meant to send.
        if sent > 0 && sent < total {
            mid_write.store(true, Ordering::Release);
        }
    }
}

impl IoOp {
    /// Whether `error` makes this operation wait and try again instead of failing: running out
    /// of descriptors, buffers or memory in an accept leaves the connection queued, so it is
    /// retried after a pause ([`tcp::ACCEPT_BACKOFF`]).
    fn backs_off_on(&self, error: &io::Error) -> bool {
        matches!(self, Self::Accept | Self::AcceptTcp { .. }) && tcp::is_resource_exhaustion(error)
    }

    /// Whether `error` says the accept skipped [`tcp::MAX_SKIPPED_CONNECTIONS`] connections in
    /// a row: the operation yields its lane and tries again (readiness is not cleared, more
    /// may be waiting), and backs off with a warning if that keeps happening.
    fn skipped_too_many(&self, error: &io::Error) -> bool {
        matches!(self, Self::Accept | Self::AcceptTcp { .. })
            && tcp::AcceptSkipLimit::of(error).is_some()
    }

    /// Whether `error` asks the operation to yield its lane and attempt again (a buffered read
    /// that received a lot without completing): readiness is not cleared, more may be waiting.
    fn yields_on(&self, error: &io::Error) -> bool {
        matches!(self, Self::ReadBuffered(_)) && YieldLane::is(error)
    }

    /// `error` as the exception this operation raises.
    fn raise(&self, error: &io::Error) -> PyErr {
        match self {
            Self::AcceptTcp {
                ending_iteration: true,
            } if ClosedResource::of(error).is_some_and(|closed| !closed.by_driver()) => {
                PyStopAsyncIteration::new_err(())
            }
            _ => os_error(error),
        }
    }
}

thread_local! {
    /// When the lane last warned that accept ran out of descriptors.
    static EXHAUSTION_WARNINGS: RefCell<tcp::RateLimit> =
        const { RefCell::new(tcp::RateLimit::new(tcp::EXHAUSTION_WARNING_INTERVAL)) };
}

/// Forgets what the lane keeps of its run for I/O: the rate limit of the exhaustion warning, so
/// a later run on this thread warns again (the limit is per run, not per thread).
pub(crate) fn clear_run_state() {
    // `try_with`: not worth a panic while the thread's locals are destroyed.
    let _cleared = EXHAUSTION_WARNINGS.try_with(|limit| {
        *limit.borrow_mut() = tcp::RateLimit::new(tcp::EXHAUSTION_WARNING_INTERVAL);
    });
}
/// Logs (rate limited) that an accept backs off: out of file descriptors or memory, or
/// failing for every connection it takes.
fn warn_accept_backs_off(py: Python<'_>, error: &io::Error) {
    if !EXHAUSTION_WARNINGS.with_borrow_mut(|limit| limit.allow(Instant::now())) {
        return;
    }
    let cause = if tcp::AcceptSkipLimit::of(error).is_some() {
        format!(
            "keeps failing for the connections it takes ({error}); a firewall, a seccomp \
             policy or the network is refusing them, or clients are resetting them before they \
             are accepted"
        )
    } else {
        format!(
            "ran out of file descriptors or memory ({error}); close connections you no \
             longer need, or raise the limit (ulimit -n)"
        )
    };
    let message = format!(
        "porto: accept() {cause}; the pending connections wait and accept retries every {} \
         ms. This is logged at most once every {} s",
        tcp::ACCEPT_BACKOFF.as_millis(),
        tcp::EXHAUSTION_WARNING_INTERVAL.as_secs(),
    );
    let logged = py
        .import("logging")
        .and_then(|logging| logging.call_method1("getLogger", ("porto.net",)))
        .and_then(|logger| logger.call_method1("warning", (message,)));
    if let Err(failure) = logged {
        failure.write_unraisable(py, None);
    }
}

impl Completed {
    /// The Python value of the result.
    fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(match self {
            Self::Bytes(bytes) => PyBytes::new(py, &bytes).into_any().unbind(),
            Self::Buffered(outcome) => return outcome.into_py(py),
            Self::Buffer(bytes) => Py::new(py, Buffer::from_pooled(bytes))?.into_any(),
            Self::Count(count) => count.into_pyobject(py)?.into_any().unbind(),
            Self::Done => py.None(),
            Self::Accepted(connection, address) => {
                let connection = IoHandle::adopt(py, connection)?;
                (connection, sockaddr::to_py(py, &address)?)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind()
            }
            Self::AcceptedTcp(connection, address) => {
                // As asyncio and uvloop do for every accepted socket: a server answers small
                // messages, which Nagle would hold back waiting for an ACK (17 §1.2). A socket
                // that refuses (the peer already reset it) is still handed over: its first
                // read or write reports the real error.
                let _ = tcp::set_nodelay(&connection, true);
                Py::new(py, TcpStream::adopt(py, connection, Some(address))?)?.into_any()
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

/// `error` as a Python exception: `porto.ClosedResourceError` (an `OSError` with errno `EBADF`)
/// for a closed or released registration, an `OSError` with its errno otherwise (so Python
/// picks the subclass, `ConnectionResetError` and so on), a `MemoryError` for a receive buffer
/// that could not be allocated.
pub(crate) fn os_error(error: &io::Error) -> PyErr {
    if let Some(closed) = ClosedResource::of(error) {
        return ClosedResourceError::new_err((ClosedResource::ERRNO, format!("porto: {closed}")));
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

/// Raises `RuntimeError` unless the running task belongs to the run whose driver `registration`
/// is registered with (13-io-driver.md §4). Only that run's lane polls the driver: a task of
/// another run (another thread's `porto.run`, a later one) would wait for readiness nobody
/// reports, forever, so the operation is refused at the call.
pub(crate) fn ensure_same_run(registration: &IoRegistration) -> PyResult<()> {
    let driver = lane::current().and_then(|handle| handle.driver().cloned());
    match driver {
        Some(driver) if registration.is_driven_by(&driver) => Ok(()),
        Some(_) => Err(PyRuntimeError::new_err(
            "porto: this socket was opened in another run (another thread's porto.run, or an \
             earlier one) than the one using it, so it can never become ready here; open the \
             socket in the run that uses it",
        )),
        None => Err(PyRuntimeError::new_err(
            "porto: a socket can only be used from a task of the run that opened it; call this \
             from a task inside porto.run, and open the socket in that run",
        )),
    }
}

/// Starts connecting `registration`'s socket to `address` without blocking: the socket is
/// connected once the [`IoOp::Connect`] that follows completes.
///
/// Raises `Cancelled` first while the running task is cancelled: a cancelled task must not
/// start a connection it will never finish (C1).
pub(crate) fn start_connect(
    py: Python<'_>,
    registration: &IoRegistration,
    address: &SockAddr,
) -> PyResult<()> {
    ensure_same_run(registration)?;
    step::raise_if_cancelled(py)?;
    match registration
        .source()
        .and_then(|socket| socket.connect(address))
    {
        Ok(()) => Ok(()),
        // Started: Unix reports `EINPROGRESS`, Windows `WSAEWOULDBLOCK`. A signal that
        // interrupts a nonblocking connect (`EINTR`) leaves it going on asynchronously too:
        // its outcome comes with writability (POSIX `connect`).
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
            ) || error.raw_os_error() == EINPROGRESS =>
        {
            Ok(())
        }
        Err(error) => Err(os_error(&error)),
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

/// Runs `op` on `registration` and returns what the caller's `await` gets: a ready awaitable
/// if it completed at once, else an `Io` trap that completes it.
///
/// Raises `Cancelled` before attempting anything while the running task is cancelled, so a
/// fast path never consumes data it cannot deliver. A task out of budget gets the trap without
/// an attempt: the driver yields before polling its wait (T2).
pub(crate) fn submit(
    py: Python<'_>,
    registration: &Arc<IoRegistration>,
    op: IoOp,
) -> PyResult<Py<PyAny>> {
    submit_claimed(py, registration, op, None)
}

/// [`submit`] for an operation holding `claim` (the stream's one reader or writer): the claim
/// is released when the operation is dropped, which is when it ends however it ends.
pub(crate) fn submit_claimed(
    py: Python<'_>,
    registration: &Arc<IoRegistration>,
    mut op: IoOp,
    claim: Option<Claim>,
) -> PyResult<Py<PyAny>> {
    ensure_same_run(registration)?;
    if !op.waits_only() && step::proceed(py)? {
        match registration.attempt_io(op.interest(), |socket| op.attempt(socket)) {
            Ok(completed) => {
                op.finish(None);
                return ready(py, completed.into_py(py)?);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            // The trap tries again after a pause (below), so it fails to the caller never.
            Err(error) if op.backs_off_on(&error) => warn_accept_backs_off(py, &error),
            // The trap yields the lane before it accepts again.
            Err(error) if op.skipped_too_many(&error) => {}
            // The trap yields the lane before it receives more.
            Err(error) if op.yields_on(&error) => {}
            Err(error) => {
                op.finish(Some(&error));
                return Err(op.raise(&error));
            }
        }
    }
    let wait = IoWait::new(wait_for(Arc::clone(registration), op, claim));
    Ok(trap::fast_new::object(py, Trap::new(TrapKind::Io(wait)))?.into_any())
}

/// How many rounds (over the operation's life) an accept may skip its [`tcp::MAX_SKIPPED_CONNECTIONS`] before it
/// stops yielding the lane and backs off (with the warning) instead.
const CAPPED_ROUNDS_BEFORE_BACKOFF: usize = 4;

/// Lets the other tasks of the lane run, then continues: wakes itself and reports pending
/// once, like the cooperative budget does.
async fn yield_lane() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// Waits [`tcp::ACCEPT_BACKOFF`], or until `registration` is closed, whichever is first: closing
/// a listener must end the accept waiting on it at once, not up to a pause later.
async fn back_off(registration: &IoRegistration) {
    let mut pause = pin!(sleep_until(Deadline::after(tcp::ACCEPT_BACKOFF)));
    let mut closed = pin!(registration.closed());
    poll_fn(|cx| {
        if pause.as_mut().poll(cx).is_ready() || closed.as_mut().poll(cx).is_ready() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

/// The slow path of `op`: wait for readiness, attempt, repeat until it does not hit
/// `WouldBlock`. Dropping it while pending unlinks its waiter and consumes nothing (T3).
async fn wait_for(registration: Arc<IoRegistration>, mut op: IoOp, claim: Option<Claim>) -> Resume {
    let interest = op.interest();
    // A closed stream answers nothing, buffered bytes or not: like the fast path, which fails
    // on the closed registration before it looks at the buffer.
    let from_buffer = if registration.is_closed() {
        None
    } else {
        op.complete_from_buffer()
    };
    let result = if let Some(completed) = from_buffer {
        // The bytes were buffered already (the fast path was skipped, out of budget): there is
        // nothing to wait for.
        completed
    } else if op.waits_only() {
        registration
            .readiness(interest)
            .await
            .map(|_| Completed::Done)
    } else {
        // Rounds (over the operation's life) that skipped the most connections an accept may: the first ones
        // yield the lane (more connections may be waiting), then the accept backs off.
        let mut capped_rounds = 0;
        loop {
            let result = registration
                .async_io(interest, |socket| op.attempt(socket))
                .await;
            match result {
                Err(error) if op.backs_off_on(&error) => {
                    // SAFETY: an `Io` wait is polled by the step loop, on its lane, attached.
                    warn_accept_backs_off(unsafe { Python::assume_attached() }, &error);
                    back_off(&registration).await;
                }
                Err(error) if op.yields_on(&error) => yield_lane().await,
                Err(error) if op.skipped_too_many(&error) => {
                    capped_rounds += 1;
                    if capped_rounds < CAPPED_ROUNDS_BEFORE_BACKOFF {
                        yield_lane().await;
                    } else {
                        // SAFETY: as above.
                        warn_accept_backs_off(unsafe { Python::assume_attached() }, &error);
                        back_off(&registration).await;
                    }
                }
                result => break result,
            }
        }
    };
    // It ended by completing or failing, not by a cancellation.
    op.finish(result.as_ref().err());
    // SAFETY: an `Io` wait is polled by the step loop, on its lane, attached.
    let py = unsafe { Python::assume_attached() };
    let value = result
        .map_err(|error| op.raise(&error))
        .and_then(|completed| completed.into_py(py));
    // The views the operation holds are released here, attached (P1), and then the claim.
    drop(op);
    drop(claim);
    match value {
        Ok(value) => Resume::Value(value),
        Err(error) => Resume::Throw(error.into_value(py)),
    }
}

#[cfg(test)]
mod tests {
    use pyo3::types::PyBytes;

    use super::*;

    fn flag() -> MidWrite {
        Arc::new(AtomicBool::new(false))
    }

    /// A write of 10 bytes of which `sent` went out.
    fn write(py: Python<'_>, sent: usize, mid_write: &MidWrite) -> IoOp {
        let data = PyBufferRef::new(PyBytes::new(py, b"0123456789").as_any(), "data").unwrap();
        IoOp::Write {
            data,
            off: sent,
            mid_write: Some(Arc::clone(mid_write)),
        }
    }

    /// A vectored write of 10 bytes in two buffers of which `sent` went out.
    fn write_v(py: Python<'_>, sent: usize, mid_write: &MidWrite) -> IoOp {
        let buffers = [b"01234".as_slice(), b"56789".as_slice()]
            .map(|bytes| PyBufferRef::new(PyBytes::new(py, bytes).as_any(), "data").unwrap())
            .into();
        let mut op = IoOp::write_v(buffers, Some(Arc::clone(mid_write)));
        if let IoOp::WriteV { written, .. } = &mut op {
            *written = sent;
        }
        op
    }

    fn ended_with(error: Option<io::Error>, sent: usize) -> [bool; 2] {
        Python::initialize();
        Python::attach(|py| {
            [write, write_v].map(|make| {
                let mid_write = flag();
                let mut op = make(py, sent, &mid_write);
                op.finish(error.as_ref());
                drop(op);
                mid_write.load(Ordering::Acquire)
            })
        })
    }

    #[test]
    fn a_write_dropped_unfinished_after_a_partial_send_marks_the_stream() {
        Python::initialize();
        Python::attach(|py| {
            for make in [write, write_v] {
                for (sent, broken) in [(0, false), (4, true), (10, false)] {
                    let mid_write = flag();
                    drop(make(py, sent, &mid_write)); // a cancellation: no `finish`
                    assert_eq!(mid_write.load(Ordering::Acquire), broken, "sent {sent}");
                }
            }
        });
    }

    #[test]
    fn a_partial_write_that_failed_with_an_error_the_connection_survives_marks_the_stream() {
        // WriteZero, a transient ENOBUFS or ENOMEM (BSD, macOS), an interrupted syscall...
        let transient = [
            io::Error::from(io::ErrorKind::WriteZero),
            io::Error::from(io::ErrorKind::OutOfMemory),
            io::Error::from(io::ErrorKind::Other),
            #[cfg(unix)]
            io::Error::from_raw_os_error(55), // ENOBUFS on macOS and the BSDs (105 on Linux)
        ];
        for error in transient {
            assert_eq!(
                ended_with(Some(error), 4),
                [true, true],
                "a later write would start mid-message"
            );
        }
    }

    #[test]
    fn a_partial_write_that_failed_with_a_lost_connection_leaves_it_to_the_sockets_own_errors() {
        let sticky = [
            io::Error::from(io::ErrorKind::ConnectionReset),
            io::Error::from(io::ErrorKind::ConnectionAborted),
            io::Error::from(io::ErrorKind::BrokenPipe),
            io::Error::from(io::ErrorKind::NotConnected),
            io::Error::from(io::ErrorKind::TimedOut),
        ];
        for error in sticky {
            assert_eq!(ended_with(Some(error), 4), [false, false]);
        }
    }

    /// Closed under the write: the error a closed registration reports.
    #[cfg(unix)]
    #[test]
    fn a_partial_write_that_failed_because_the_stream_was_closed_leaves_it_to_the_close() {
        let (left, _right) = std::os::unix::net::UnixStream::pair().unwrap();
        left.set_nonblocking(true).unwrap();
        let driver: Arc<dyn porto_io::Driver> = Arc::new(porto_io::MioDriver::new().unwrap());
        let registration =
            Registration::new(driver, std::os::fd::OwnedFd::from(left), Interest::READABLE)
                .unwrap();
        registration.close().unwrap();
        let closed = registration.source().err().expect("closed");
        assert_eq!(ended_with(Some(closed), 4), [false, false]);
    }

    #[test]
    fn a_completed_write_never_marks_the_stream() {
        assert_eq!(ended_with(None, 10), [false, false]);
    }

    #[test]
    fn accept_classifies_what_to_yield_and_what_to_back_off_on() {
        let accept = IoOp::AcceptTcp {
            ending_iteration: false,
        };
        let skipped = tcp::AcceptSkipLimit::error(io::ErrorKind::ConnectionAborted.into());
        assert!(accept.skipped_too_many(&skipped));
        assert!(
            !accept.backs_off_on(&skipped),
            "a pause is for resource errors"
        );
        assert!(!accept.skipped_too_many(&io::Error::from(io::ErrorKind::WouldBlock)));
        #[cfg(unix)]
        assert!(accept.backs_off_on(&io::Error::from_raw_os_error(24))); // EMFILE
        assert!(!IoOp::Connect.skipped_too_many(&skipped));
    }
}
